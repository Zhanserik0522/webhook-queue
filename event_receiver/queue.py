import json
import uuid

from .db import transaction

MAX_ATTEMPTS = 3
RETRY_BASE_SECONDS = 2


def enqueue(path, event, now, ttl):
    # Both rows commit together. The unique key is the concurrency guard.
    with transaction(path) as connection:
        cursor = connection.execute(
            "INSERT INTO events VALUES (?, ?, ?, ?) ON CONFLICT(event_id) DO NOTHING",
            (event["event_id"], event["type"], json.dumps(event["payload"]), now),
        )
        if cursor.rowcount == 0:
            return False
        connection.execute(
            "INSERT INTO tasks(event_id, available_at, expires_at) VALUES (?, ?, ?)",
            (event["event_id"], now, now + ttl),
        )
        return True


def claim(path, now, lease_seconds):
    with transaction(path) as connection:
        # A crashed worker already consumed its attempt when acquiring the lease.
        connection.execute("""
            UPDATE tasks SET status='failed', finished_at=?, lease_token=NULL,
                lease_until=NULL, last_error='Worker lease expired after final attempt'
            WHERE status='pending' AND attempts >= ? AND lease_until <= ?
        """, (now, MAX_ATTEMPTS, now))
        # Bound the lifetime of abandoned/unhandled work, including clock-independent
        # retries that otherwise might keep failing before a useful handler runs.
        connection.execute("""
            UPDATE tasks SET status='skipped', finished_at=?, lease_token=NULL,
                lease_until=NULL, last_error='Task lifetime exceeded'
            WHERE status='pending' AND expires_at <= ?
                AND (lease_until IS NULL OR lease_until <= ?)
        """, (now, now, now))
        # A lost attempt is retried with the same backoff as an ordinary error.
        connection.execute("""
            UPDATE tasks SET available_at=lease_until + ? * (1 << (attempts - 1)),
                lease_token=NULL, lease_until=NULL, last_error='Worker lease expired'
            WHERE status='pending' AND lease_until <= ? AND attempts < ?
        """, (RETRY_BASE_SECONDS, now, MAX_ATTEMPTS))
        row = connection.execute("""
            SELECT t.*, e.type, e.payload FROM tasks t
            JOIN events e USING(event_id)
            WHERE status='pending' AND lease_token IS NULL AND available_at <= ?
                AND attempts < ?
            ORDER BY available_at, t.id LIMIT 1
        """, (now, MAX_ATTEMPTS)).fetchone()
        if row is None:
            return None
        task = dict(row)
        task["attempts"] += 1
        task["lease_token"] = uuid.uuid4().hex
        connection.execute("""
            UPDATE tasks SET attempts=?, lease_token=?, lease_until=? WHERE id=?
        """, (task["attempts"], task["lease_token"], now + lease_seconds, task["id"]))
        task["payload"] = json.loads(task["payload"])
        return task


def finish(path, task, now, outcome="success", error=None):
    if outcome not in {"success", "skipped"}:
        raise ValueError("Invalid handler outcome")
    if error is not None:
        status = "failed" if task["attempts"] >= MAX_ATTEMPTS else "pending"
    else:
        status = outcome
    delay = RETRY_BASE_SECONDS * 2 ** (task["attempts"] - 1)
    with transaction(path) as connection:
        cursor = connection.execute("""
            UPDATE tasks SET status=?, available_at=?, lease_until=NULL,
                lease_token=NULL, last_error=?, finished_at=?
            WHERE id=? AND status='pending' AND lease_token=?
        """, (
            status, now + delay, str(error)[:1000] if error is not None else None,
            None if status == "pending" else now, task["id"], task["lease_token"],
        ))
        # A stale worker cannot overwrite a reclaimed task's result.
        return cursor.rowcount == 1
