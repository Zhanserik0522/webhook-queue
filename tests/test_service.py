import os
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from event_receiver import create_app
from event_receiver.db import connect, transaction
from event_receiver.queue import claim, enqueue, finish
from event_receiver.worker import run_once


@pytest.fixture
def app(tmp_path):
    return create_app({"TESTING": True, "DATABASE": tmp_path / "test.sqlite3"})


@pytest.fixture
def client(app):
    return app.test_client()


def event(event_id="abc-123", event_type="payment_succeeded"):
    return {"event_id": event_id, "type": event_type, "payload": {"amount": 100}}


def task_row(app):
    connection = connect(app.config["DATABASE"])
    try:
        return dict(connection.execute("SELECT * FROM tasks ORDER BY id LIMIT 1").fetchone())
    finally:
        connection.close()


def test_duplicate_webhook_creates_one_event_and_one_task(app, client):
    assert client.post("/webhook", json=event()).status_code == 202
    changed = event(event_type="payment_failed")
    changed["payload"] = {"amount": 900}
    response = client.post("/webhook", json=changed)
    assert response.status_code == 200
    assert response.json == {"event_id": "abc-123", "duplicate": True}
    stats = client.get("/stats").json
    assert stats["events_by_type"] == {"payment_succeeded": 1}
    assert stats["tasks_by_status"] == {
        "pending": 1, "success": 0, "failed": 0, "skipped": 0,
    }
    assert task_row(app)["attempts"] == 0


@pytest.mark.parametrize("body", [
    None, [], {}, {"event_id": "", "type": "test", "payload": {}},
    {"event_id": 12, "type": "test", "payload": {}},
    {"event_id": "a", "type": "  ", "payload": {}},
    {"event_id": "a", "type": "test", "payload": []},
    {"event_id": "a", "type": "test"},
])
def test_invalid_body_is_rejected(client, body):
    import json

    response = client.post("/webhook", data=json.dumps(body), content_type="application/json")
    assert response.status_code == 400
    assert response.json["error"]
    assert client.get("/stats").json["events_by_type"] == {}


def test_bad_json_content_type_and_size(client):
    assert client.post("/webhook", data="{}").status_code == 415
    malformed = client.post("/webhook", data="{broken", content_type="application/json")
    assert malformed.status_code == 400
    assert malformed.json["error"] == "Body is not valid JSON"
    assert client.post("/webhook", json={**event(), "payload": {"x": float("nan")}}).status_code == 400
    assert client.post("/webhook", data="x" * (1024 * 1024 + 1),
                       content_type="application/json").status_code == 413


def test_concurrent_duplicates(app):
    path = app.config["DATABASE"]
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: enqueue(path, event(), 100, 1000), range(16)))
    assert results.count(True) == 1
    assert app.test_client().get("/stats").json["tasks_by_status"]["pending"] == 1


def test_event_and_task_insert_are_atomic(app):
    path = app.config["DATABASE"]
    with transaction(path) as connection:
        connection.execute("""CREATE TRIGGER reject_task BEFORE INSERT ON tasks
                              BEGIN SELECT RAISE(ABORT, 'test rollback'); END""")
    with pytest.raises(sqlite3.IntegrityError, match="test rollback"):
        enqueue(path, event(), 100, 1000)
    assert app.test_client().get("/stats").json["events_by_type"] == {}


@pytest.mark.parametrize("event_type, expected", [
    ("payment_succeeded", "success"), ("unknown_event", "skipped"),
])
def test_real_worker_processes_or_skips(app, event_type, expected):
    path = app.config["DATABASE"]
    enqueue(path, event(event_type=event_type), 100, 1000)
    assert run_once(path, clock=lambda: 100)
    assert task_row(app)["status"] == expected
    assert task_row(app)["attempts"] == 1
    assert not run_once(path, clock=lambda: 100)


@pytest.mark.parametrize("event_type, expected", [
    ("simulated_flaky", "success"), ("simulated_failure", "failed"),
])
def test_three_attempts_with_increasing_backoff(app, event_type, expected):
    path = app.config["DATABASE"]
    enqueue(path, event(event_type=event_type), 100, 1000)
    assert run_once(path, clock=lambda: 100)
    assert task_row(app)["available_at"] == 102
    assert not run_once(path, clock=lambda: 101)
    assert run_once(path, clock=lambda: 102)
    assert task_row(app)["available_at"] == 106
    assert not run_once(path, clock=lambda: 105)
    assert run_once(path, clock=lambda: 106)
    row = task_row(app)
    assert row["status"] == expected
    assert row["attempts"] == 3
    assert row["finished_at"] == 106
    assert not run_once(path, clock=lambda: 10000)


def test_concurrent_workers_do_not_claim_same_task(app):
    path = app.config["DATABASE"]
    enqueue(path, event(), 100, 1000)
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: claim(path, 100, 10), range(8)))
    assert sum(task is not None for task in results) == 1


def test_crashed_worker_recovery_and_stale_result(app):
    path = app.config["DATABASE"]
    enqueue(path, event(), 100, 1000)
    first = claim(path, 100, 10)
    assert claim(path, 109, 10) is None
    assert claim(path, 110, 10) is None  # lease expiry + 2 seconds backoff
    second = claim(path, 112, 10)
    assert second["attempts"] == 2
    assert not finish(path, first, 113)
    assert finish(path, second, 114)
    assert task_row(app)["status"] == "success"


def test_three_worker_crashes_eventually_fail(app):
    path = app.config["DATABASE"]
    enqueue(path, event(), 100, 1000)
    assert claim(path, 100, 10)["attempts"] == 1
    assert claim(path, 112, 10)["attempts"] == 2
    assert claim(path, 126, 10)["attempts"] == 3
    assert claim(path, 136, 10) is None
    assert task_row(app)["status"] == "failed"


def test_expired_unprocessed_task_is_skipped(app):
    path = app.config["DATABASE"]
    enqueue(path, event(), 100, 20)
    assert claim(path, 120, 10) is None
    assert task_row(app)["status"] == "skipped"


def hanging_handler(_task):
    time.sleep(60)
    return "success"


def crashing_handler(_task):
    os._exit(7)


@pytest.mark.parametrize("handler, message", [
    (hanging_handler, "timeout"), (crashing_handler, "without a result"),
])
def test_hanging_or_crashed_child_does_not_block_queue(app, handler, message):
    path = app.config["DATABASE"]
    enqueue(path, event(), 100, 1000)
    timeout = 0.3 if handler is hanging_handler else 5
    assert run_once(path, timeout=timeout, clock=lambda: 100, handler=handler)
    row = task_row(app)
    assert row["status"] == "pending"
    assert row["attempts"] == 1
    assert row["lease_token"] is None
    assert message in row["last_error"]
    assert run_once(path, clock=lambda: 102)
    assert task_row(app)["status"] == "success"


def test_stats_and_worker_cli(app, client):
    assert client.get("/stats").json == {
        "events_by_type": {},
        "tasks_by_status": {"pending": 0, "success": 0, "failed": 0, "skipped": 0},
    }
    client.post("/webhook", json=event("one"))
    client.post("/webhook", json=event("two", "unknown_event"))
    runner = app.test_cli_runner()
    assert runner.invoke(args=["worker", "--once"]).exit_code == 0
    assert runner.invoke(args=["worker", "--once"]).exit_code == 0
    stats = client.get("/stats").json
    assert stats["events_by_type"] == {"payment_succeeded": 1, "unknown_event": 1}
    assert stats["tasks_by_status"] == {"pending": 0, "success": 1, "failed": 0, "skipped": 1}
