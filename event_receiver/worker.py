import logging
import multiprocessing
import time

from .handlers import handle_event
from .queue import claim, finish

logger = logging.getLogger(__name__)


def _child(send, handler, task):
    try:
        outcome = handler(task)
        if outcome not in {"success", "skipped"}:
            raise ValueError("Handler must return success or skipped")
        send.send((outcome, None))
    except BaseException as exc:
        send.send(("success", f"{type(exc).__name__}: {exc}"[:1000]))
    finally:
        send.close()


def run_handler(task, timeout, handler=handle_event):
    # A separate process can be stopped even if a handler hangs. No SQLite
    # connection crosses the spawn boundary (also works on Windows).
    context = multiprocessing.get_context("spawn")
    receive, send = context.Pipe(duplex=False)
    process = context.Process(target=_child, args=(send, handler, task), daemon=True)
    try:
        process.start()
        send.close()
        if receive.poll(timeout):
            try:
                return receive.recv()
            except EOFError:
                return "success", "Handler process exited without a result"
        return "success", "Handler timeout exceeded"
    finally:
        if process.pid is not None:
            process.join(timeout=0.1)
            if process.is_alive():
                process.terminate()
                process.join(timeout=1)
            if process.is_alive():
                process.kill()
                process.join()
            process.close()
        receive.close()
        send.close()


def run_once(path, timeout=5, clock=time.time, handler=handle_event):
    task = claim(path, clock(), lease_seconds=timeout + 30)
    if task is None:
        return False
    try:
        outcome, error = run_handler(task, timeout, handler)
    except Exception as exc:
        outcome, error = "success", f"Handler startup failure: {exc}"
    finish(path, task, clock(), outcome=outcome, error=error)
    logger.info("Task %s attempt %s: %s", task["id"], task["attempts"], error or outcome)
    return True


def run_forever(path, timeout=5, poll_interval=0.5):
    try:
        while True:
            try:
                worked = run_once(path, timeout)
            except Exception:
                # Temporary database faults must not permanently stop the worker.
                logger.exception("Worker iteration failed; will retry")
                worked = False
            if not worked:
                time.sleep(poll_interval)
    except KeyboardInterrupt:
        logger.info("Worker stopped; unfinished leases will be recovered on restart")
