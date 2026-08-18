from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
from typing import Any

from aconex.workflow import normalize_workflow_status_fields

from .connection import connect, resolve_db_path
from .schema import (
    WORKFLOW_COLUMNS,
    WORKFLOW_COMMENT_COLUMNS,
    apply_migrations,
    migrate_docflow_sync_to_number_pk,
    migrate_workflows_to_number_pk,
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _rows_to_dicts(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows]


def _json_dump(value: Any) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def dedupe_workflows(db_path: str | Path | None = None) -> list[str]:
    """Collapse duplicate workflow_number rows. Returns numbers that were merged."""
    path = resolve_db_path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        collapsed = apply_migrations(conn)
        extra = migrate_workflows_to_number_pk(conn)
        migrate_docflow_sync_to_number_pk(conn)
        conn.commit()
        return sorted(set(collapsed) | set(extra))
    finally:
        conn.close()


def upsert_workflow(row: Mapping[str, Any], db_path: str | Path | None = None) -> bool:
    data = {column: row.get(column) for column in WORKFLOW_COLUMNS}
    workflow_number = str(data.get("workflow_number") or "").strip()
    if not workflow_number:
        raise ValueError("workflow_number is required")
    data["workflow_number"] = workflow_number
    if data.get("workflow_id") is not None:
        data["workflow_id"] = str(data["workflow_id"]).strip() or None
    data = normalize_workflow_status_fields(data)

    now = _utc_now()
    data["last_checked_at"] = data["last_checked_at"] or now

    with connect(db_path) as conn:
        existing = conn.execute(
            "SELECT * FROM workflows WHERE workflow_number = ?",
            (workflow_number,),
        ).fetchone()
        changed = existing is None or any(
            existing[column] != data[column]
            for column in WORKFLOW_COLUMNS
            if column not in {"last_checked_at", "last_changed_at", "source"}
        )
        if changed:
            data["last_changed_at"] = data["last_changed_at"] or now
        elif existing is not None:
            data["last_changed_at"] = existing["last_changed_at"]

        placeholders = ", ".join("?" for _ in WORKFLOW_COLUMNS)
        columns = ", ".join(WORKFLOW_COLUMNS)
        update_clause = ", ".join(
            f"{column} = excluded.{column}"
            for column in WORKFLOW_COLUMNS
            if column != "workflow_number"
        )
        conn.execute(
            f"""
            INSERT INTO workflows ({columns})
            VALUES ({placeholders})
            ON CONFLICT(workflow_number) DO UPDATE SET {update_clause}
            """,
            tuple(data[column] for column in WORKFLOW_COLUMNS),
        )
    return changed


def get_pending_workflows(db_path: str | Path | None = None) -> list[dict[str, Any]]:
    """Return locally stored Workflows that still need Aconex status checks."""
    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM workflows
            WHERE (is_completed IS NULL OR is_completed = 0)
              AND lower(trim(coalesce(review_status, ''))) NOT IN ('terminate', 'terminated')
            ORDER BY workflow_number_int, workflow_number
            """
        ).fetchall()
    return _rows_to_dicts(rows)


def get_open_workflows(db_path: str | Path | None = None) -> list[dict[str, Any]]:
    """Backward-compatible name for pending Workflows."""
    return get_pending_workflows(db_path)


def add_workflow_history(
    workflow_id: str | None = None,
    workflow_number: str | None = None,
    checked_at: str | None = None,
    change_summary: str | None = None,
    old_data_json: Any | None = None,
    new_data_json: Any | None = None,
    db_path: str | Path | None = None,
) -> int:
    with connect(db_path) as conn:
        cursor = conn.execute(
            """
            INSERT INTO workflow_history (
                workflow_id,
                workflow_number,
                checked_at,
                change_summary,
                old_data_json,
                new_data_json
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                workflow_id,
                workflow_number,
                checked_at or _utc_now(),
                change_summary,
                _json_dump(old_data_json) if old_data_json is not None else None,
                _json_dump(new_data_json) if new_data_json is not None else None,
            ),
        )
    return int(cursor.lastrowid)


def upsert_workflow_comment(row: Mapping[str, Any], db_path: str | Path | None = None) -> bool:
    data = {column: row.get(column) for column in WORKFLOW_COMMENT_COLUMNS}
    if not data["workflow_number"]:
        raise ValueError("workflow_number is required")
    if not data["mail_id"]:
        raise ValueError("mail_id is required")
    data["created_at"] = data["created_at"] or _utc_now()

    with connect(db_path) as conn:
        existing = conn.execute(
            """
            SELECT *
            FROM workflow_comments
            WHERE workflow_number = ? AND mail_id = ?
            """,
            (data["workflow_number"], data["mail_id"]),
        ).fetchone()
        changed = existing is None or any(
            existing[column] != data[column]
            for column in WORKFLOW_COMMENT_COLUMNS
            if column != "created_at"
        )

        placeholders = ", ".join("?" for _ in WORKFLOW_COMMENT_COLUMNS)
        columns = ", ".join(WORKFLOW_COMMENT_COLUMNS)
        update_clause = ", ".join(
            f"{column} = excluded.{column}"
            for column in WORKFLOW_COMMENT_COLUMNS
            if column not in {"workflow_number", "mail_id", "created_at"}
        )
        conn.execute(
            f"""
            INSERT INTO workflow_comments ({columns})
            VALUES ({placeholders})
            ON CONFLICT(workflow_number, mail_id) DO UPDATE SET {update_clause}
            """,
            tuple(data[column] for column in WORKFLOW_COMMENT_COLUMNS),
        )
    return changed


def add_update_run(
    command: str,
    checked_count: int = 0,
    changed_count: int = 0,
    failed_count: int = 0,
    notes: str | None = None,
    run_time: str | None = None,
    db_path: str | Path | None = None,
) -> int:
    with connect(db_path) as conn:
        cursor = conn.execute(
            """
            INSERT INTO update_runs (
                run_time,
                command,
                checked_count,
                changed_count,
                failed_count,
                notes
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                run_time or _utc_now(),
                command,
                checked_count,
                changed_count,
                failed_count,
                notes,
            ),
        )
    return int(cursor.lastrowid)


def load_workflows(db_path: str | Path | None = None) -> list[dict[str, Any]]:
    with connect(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM workflows ORDER BY workflow_number_int, workflow_number"
        ).fetchall()
    return _rows_to_dicts(rows)


def load_docflow_sync_state(db_path: str | Path | None = None) -> dict[str, str]:
    """Return the last successfully handled DocFlow payload hash by workflow number."""
    with connect(db_path) as conn:
        rows = conn.execute(
            "SELECT workflow_number, payload_hash FROM docflow_workflow_sync"
        ).fetchall()
    return {str(row["workflow_number"]): str(row["payload_hash"]) for row in rows}


def upsert_docflow_sync_state(
    workflow_number: str,
    payload_hash: str,
    *,
    synced_at: str | None = None,
    db_path: str | Path | None = None,
) -> None:
    number = str(workflow_number or "").strip()
    if not number:
        raise ValueError("workflow_number is required")
    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO docflow_workflow_sync (workflow_number, payload_hash, last_synced_at)
            VALUES (?, ?, ?)
            ON CONFLICT(workflow_number) DO UPDATE SET
                payload_hash = excluded.payload_hash,
                last_synced_at = excluded.last_synced_at
            """,
            (number, payload_hash, synced_at or _utc_now()),
        )


def delete_docflow_sync_state(
    workflow_numbers: list[str] | set[str] | tuple[str, ...] | None = None,
    *,
    db_path: str | Path | None = None,
) -> int:
    """Drop stored DocFlow payload hashes so the next push re-applies status.

    When ``workflow_numbers`` is None/empty, clears the entire sync table.
    Returns the number of deleted rows.
    """
    with connect(db_path) as conn:
        if not workflow_numbers:
            cur = conn.execute("DELETE FROM docflow_workflow_sync")
            return int(cur.rowcount or 0)
        numbers = sorted({str(value).strip() for value in workflow_numbers if str(value).strip()})
        if not numbers:
            return 0
        placeholders = ", ".join("?" for _ in numbers)
        cur = conn.execute(
            f"DELETE FROM docflow_workflow_sync WHERE workflow_number IN ({placeholders})",
            numbers,
        )
        return int(cur.rowcount or 0)


def load_workflow_comments(db_path: str | Path | None = None) -> list[dict[str, Any]]:
    with connect(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM workflow_comments ORDER BY workflow_number_int, sent_date, mail_number"
        ).fetchall()
    return _rows_to_dicts(rows)


def load_update_runs(db_path: str | Path | None = None) -> list[dict[str, Any]]:
    with connect(db_path) as conn:
        rows = conn.execute("SELECT * FROM update_runs ORDER BY id").fetchall()
    return _rows_to_dicts(rows)
