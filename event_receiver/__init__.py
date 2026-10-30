import json
import logging
import math
import os
import sqlite3
import time
from pathlib import Path

import click
from flask import Flask, jsonify, request
from werkzeug.exceptions import BadRequest, RequestEntityTooLarge

from .db import connect, init_db
from .queue import enqueue


def create_app(config=None):
    app = Flask(__name__)
    app.config.from_mapping(
        DATABASE=os.environ.get("DATABASE", str(Path(app.instance_path) / "events.sqlite3")),
        MAX_CONTENT_LENGTH=1024 * 1024,
        TASK_TTL_SECONDS=86400,
    )
    if config:
        app.config.update(config)
    ttl = app.config["TASK_TTL_SECONDS"]
    if not isinstance(ttl, (int, float)) or not math.isfinite(ttl) or ttl <= 0:
        raise ValueError("TASK_TTL_SECONDS must be a positive finite number")
    init_db(app.config["DATABASE"])

    @app.errorhandler(RequestEntityTooLarge)
    def too_large(_error):
        return jsonify(error="JSON body must not exceed 1 MiB"), 413

    @app.errorhandler(sqlite3.OperationalError)
    def database_unavailable(error):
        app.logger.exception("Database unavailable", exc_info=error)
        return jsonify(error="Database temporarily unavailable; retry the request"), 503

    @app.post("/webhook")
    def webhook():
        if not request.is_json:
            return jsonify(error="Content-Type must be application/json"), 415
        try:
            event = request.get_json()
        except (BadRequest, RecursionError):
            return jsonify(error="Body is not valid JSON"), 400
        if not isinstance(event, dict):
            return jsonify(error="JSON body must be an object"), 400
        for field in ("event_id", "type"):
            value = event.get(field)
            if not isinstance(value, str) or not value.strip() or len(value) > 255:
                return jsonify(error=f"{field} must be a non-empty string up to 255 characters"), 400
        if not isinstance(event.get("payload"), dict):
            return jsonify(error="payload must be a JSON object"), 400
        try:
            # Python's decoder accepts NaN/Infinity, which aren't valid JSON.
            json.dumps(event, allow_nan=False)
        except (ValueError, OverflowError, RecursionError):
            return jsonify(error="JSON numbers must be finite"), 400
        created = enqueue(app.config["DATABASE"], event, time.time(), ttl)
        return jsonify(event_id=event["event_id"], duplicate=not created), 202 if created else 200

    @app.get("/stats")
    def stats():
        connection = connect(app.config["DATABASE"])
        try:
            # Both counters reflect the same snapshot even during concurrent writes.
            connection.execute("BEGIN")
            events = {row["type"]: row["count"] for row in connection.execute(
                "SELECT type, COUNT(*) AS count FROM events GROUP BY type"
            )}
            tasks = dict.fromkeys(("pending", "success", "failed", "skipped"), 0)
            tasks.update({row["status"]: row["count"] for row in connection.execute(
                "SELECT status, COUNT(*) AS count FROM tasks GROUP BY status"
            )})
            return jsonify(events_by_type=events, tasks_by_status=tasks)
        finally:
            connection.close()

    @app.cli.command("worker")
    @click.option("--once", is_flag=True, help="Process at most one available task and exit.")
    @click.option("--timeout", default=5.0, type=click.FloatRange(min=0.01), show_default=True)
    @click.option("--poll-interval", default=0.5, type=click.FloatRange(min=0.01), show_default=True)
    def worker(once, timeout, poll_interval):
        """Process the SQLite queue (run separately from the web server)."""
        from .worker import run_forever, run_once

        logging.basicConfig(level=logging.INFO)
        if once:
            run_once(app.config["DATABASE"], timeout)
        else:
            run_forever(app.config["DATABASE"], timeout, poll_interval)

    return app
