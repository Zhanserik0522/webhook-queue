import logging

logger = logging.getLogger(__name__)


def handle_event(task):
    """Deterministic failure scenarios make retries reproducible."""
    event_type = task["type"]
    if event_type == "simulated_failure":
        raise RuntimeError("Simulated permanent failure")
    if event_type == "simulated_flaky" and task["attempts"] < 3:
        raise RuntimeError("Simulated temporary failure")
    if event_type not in {"payment_succeeded", "payment_failed", "user_registered",
                          "simulated_flaky"}:
        return "skipped"
    logger.info("Handled event %s (%s), attempt %s",
                task["event_id"], event_type, task["attempts"])
    return "success"
