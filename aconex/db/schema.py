from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from datetime import datetime, timezone
import sqlite3
from typing import Any

from aconex.workflow import normalize_workflow_status_fields


SCHEMA_VERSION = 3
SCHEMA_VERSION_KEY = "version"

WORKFLOW_COLUMNS = (
    "workflow_number",
    "workflow_number_int",
    "workflow_id",
    "workflow_title",
    "review_outcome",
    "review_status",
    "step_1_completed_time",
    "step_1_due_time",
    "step_1_review_status",
    "step_1_overdue_duration_or_status",
    "step_2_completed_time",
    "step_2_due_time",
    "step_2_review_status",
    "step_2_overdue_duration_or_status",
    "is_completed",
    "last_checked_at",
    "last_changed_at",
    "source",
)

WORKFLOW_COMMENT_COLUMNS = (
    "workflow_number",
    "workflow_number_int",
    "mail_id",
    "mail_number",
    "mail_subject",
    "sent_date",
    "from_user",
    "comment_text",
    "doc_no",
    "review_step",
    "participant",
    "review_outcome",
    "review_comment",
    "source",
    "created_at",
)

DOCFLOW_SYNC_COLUMNS = (
    "workflow_number",
    "payload_hash",
    "last_synced_at",
)

STATUS_MERGE_COLUMNS = (
    "workflow_title",
    "review_outcome",
    "review_status",
    "step_1_completed_time",
    "step_1_due_time",
    "step_1_review_status",
    "step_1_overdue_duration_or_status",
    "step_2_completed_time",
    "step_2_due_time",
    "step_2_review_status",
    "step_2_overdue_duration_or_status",
    "is_completed",
    "last_checked_at",
    "last_changed_at",
    "source",
)

_CURRENT_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS workflows (
    workflow_number TEXT PRIMARY KEY,
    workflow_number_int INTEGER,
    workflow_id TEXT,
    workflow_title TEXT,
    review_outcome TEXT,
    review_status TEXT,
    step_1_completed_time TEXT,
    step_1_due_time TEXT,
    step_1_review_status TEXT,
    step_1_overdue_duration_or_status TEXT,
    step_2_completed_time TEXT,
    step_2_due_time TEXT,
    step_2_review_status TEXT,
    step_2_overdue_duration_or_status TEXT,
    is_completed INTEGER,
    last_checked_at TEXT,
    last_changed_at TEXT,
    source TEXT
);

CREATE TABLE IF NOT EXISTS workflow_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_id TEXT,
    workflow_number TEXT,
    checked_at TEXT,
    change_summary TEXT,
    old_data_json TEXT,
    new_data_json TEXT
);

CREATE TABLE IF NOT EXISTS workflow_comments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_number TEXT,
    workflow_number_int INTEGER,
    mail_id TEXT,
    mail_number TEXT,
    mail_subject TEXT,
    sent_date TEXT,
    from_user TEXT,
    comment_text TEXT,
    doc_no TEXT,
    review_step TEXT,
    participant TEXT,
    review_outcome TEXT,
    review_comment TEXT,
    source TEXT,
    created_at TEXT,
    UNIQUE(workflow_number, mail_id)
);

CREATE TABLE IF NOT EXISTS update_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_time TEXT,
    command TEXT,
    checked_count INTEGER,
    changed_count INTEGER,
    failed_count INTEGER,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS docflow_workflow_sync (
    workflow_number TEXT PRIMARY KEY,
    payload_hash TEXT NOT NULL,
    last_synced_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_workflows_open
    ON workflows(is_completed, workflow_number_int);
CREATE INDEX IF NOT EXISTS idx_workflow_history_workflow_number
    ON workflow_history(workflow_number);
CREATE INDEX IF NOT EXISTS idx_workflow_history_workflow_id
    ON workflow_history(workflow_id);
CREATE INDEX IF NOT EXISTS idx_workflow_comments_workflow_number
    ON workflow_comments(workflow_number);
"""


def apply_migrations(conn: sqlite3.Connection) -> list[str]:
    """Bring the database to SCHEMA_VERSION.

    Version 0: empty file
    Version 1: legacy workflow_id primary key
    Version 2: workflow_number primary key (no schema_meta)
    Version 3: schema_meta plus one-shot status cleanup

    Returns workflow numbers collapsed while moving off the legacy primary key.
    """
    version = current_schema_version(conn)
    if version >= SCHEMA_VERSION:
        return []

    collapsed: list[str] = []
    if version == 0:
        _create_current_schema(conn)
        _set_schema_version(conn, SCHEMA_VERSION)
        return []

    if version < 2:
        collapsed = migrate_workflows_to_number_pk(conn)
        migrate_docflow_sync_to_number_pk(conn)
        if collapsed:
            print(
                "Collapsed duplicate workflow_number rows during schema migration: "
                + ", ".join(collapsed)
            )

    _create_current_schema(conn)
    if version < 3:
        _normalize_legacy_status_values(conn)
    _set_schema_version(conn, SCHEMA_VERSION)
    return collapsed


def current_schema_version(conn: sqlite3.Connection) -> int:
    if table_exists(conn, "schema_meta"):
        row = conn.execute(
            "SELECT value FROM schema_meta WHERE key = ?",
            (SCHEMA_VERSION_KEY,),
        ).fetchone()
        if row and str(row["value"]).strip().isdigit():
            return int(row["value"])

    if not table_exists(conn, "workflows"):
        return 0
    primary_key = table_primary_key(conn, "workflows")
    if primary_key == "workflow_number":
        return 2
    return 1


def _create_current_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(_CURRENT_SCHEMA_SQL)
    _ensure_columns(
        conn,
        "workflows",
        {
            "review_outcome": "TEXT",
            "review_status": "TEXT",
            "step_1_review_status": "TEXT",
            "step_2_review_status": "TEXT",
        },
    )
    _ensure_columns(
        conn,
        "workflow_comments",
        {
            "doc_no": "TEXT",
            "review_step": "TEXT",
            "participant": "TEXT",
            "review_outcome": "TEXT",
            "review_comment": "TEXT",
        },
    )


def _set_schema_version(conn: sqlite3.Connection, version: int) -> None:
    conn.execute(
        """
        INSERT INTO schema_meta (key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (SCHEMA_VERSION_KEY, str(version)),
    )


def _ensure_columns(conn: sqlite3.Connection, table_name: str, columns: Mapping[str, str]) -> None:
    existing = {
        row["name"]
        for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    }
    for column, column_type in columns.items():
        if column not in existing:
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column} {column_type}")


def table_primary_key(conn: sqlite3.Connection, table_name: str) -> str | None:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    for row in rows:
        if int(row["pk"] or 0) == 1:
            return str(row["name"])
    return None


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


def _normalize_legacy_status_values(conn: sqlite3.Connection) -> None:
    """One-shot cleanup of values previously rewritten on every init_db call."""
    if not table_exists(conn, "workflows"):
        return
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    conn.execute(
        """
        UPDATE workflows
        SET review_status = 'Terminate',
            last_changed_at = ?
        WHERE lower(trim(coalesce(review_outcome, ''))) = 'none'
          AND coalesce(review_status, '') != 'Terminate'
        """,
        (now,),
    )
    conn.execute(
        """
        UPDATE workflows
        SET step_1_overdue_duration_or_status = CASE
                WHEN step_1_overdue_duration_or_status = '審批中' THEN 'pending'
                ELSE step_1_overdue_duration_or_status
            END,
            step_2_overdue_duration_or_status = CASE
                WHEN step_2_overdue_duration_or_status = '審批中' THEN 'pending'
                ELSE step_2_overdue_duration_or_status
            END
        WHERE step_1_overdue_duration_or_status = '審批中'
           OR step_2_overdue_duration_or_status = '審批中'
        """
    )


def select_winner_workflow(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    """Pick one canonical row for a workflow_number.

    Prefer completed / richer status, then newest check time, then stable min workflow_id.
    Merge non-empty status fields from siblings when the winner is missing them.
    """
    if not rows:
        raise ValueError("rows is required")
    ordered = sorted(rows, key=_workflow_row_rank, reverse=True)
    winner = dict(ordered[0])
    for sibling in ordered[1:]:
        for column in STATUS_MERGE_COLUMNS:
            current = winner.get(column)
            candidate = sibling.get(column)
            if _is_empty_status(current) and not _is_empty_status(candidate):
                winner[column] = candidate
        winner_id = str(winner.get("workflow_id") or "")
        sibling_id = str(sibling.get("workflow_id") or "")
        if sibling_id and (not winner_id or sibling_id < winner_id):
            winner["workflow_id"] = sibling_id
    if winner.get("is_completed") in (None, ""):
        winner["is_completed"] = 0
    return normalize_workflow_status_fields(winner)


def _is_empty_status(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    return False


def _workflow_row_rank(row: Mapping[str, Any]) -> tuple[Any, ...]:
    """Higher rank wins. Last key is inverted workflow_id so min id wins ties."""
    workflow_id = str(row.get("workflow_id") or "")
    return (
        int(row.get("is_completed") or 0),
        1 if str(row.get("step_2_completed_time") or "").strip() else 0,
        1 if str(row.get("step_1_completed_time") or "").strip() else 0,
        0 if str(row.get("review_outcome") or "").strip().casefold() in {"", "pending"} else 1,
        str(row.get("last_checked_at") or ""),
        str(row.get("last_changed_at") or ""),
        1 if workflow_id else 0,
        "".join(chr(255 - ord(ch)) for ch in workflow_id) if workflow_id else "",
    )


def migrate_workflows_to_number_pk(conn: sqlite3.Connection) -> list[str]:
    """Rebuild workflows with workflow_number PK and collapse duplicate numbers."""
    if not table_exists(conn, "workflows"):
        return []

    primary_key = table_primary_key(conn, "workflows")
    raw_rows = [dict(row) for row in conn.execute("SELECT * FROM workflows").fetchall()]
    by_number: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in raw_rows:
        number = str(row.get("workflow_number") or "").strip()
        if not number:
            number = str(row.get("workflow_id") or "").strip()
            if not number:
                continue
            row["workflow_number"] = number
        by_number[number].append(row)

    duplicated_numbers = sorted(number for number, rows in by_number.items() if len(rows) > 1)
    needs_rebuild = primary_key != "workflow_number" or bool(duplicated_numbers)
    if not needs_rebuild:
        return []

    winners = [select_winner_workflow(rows) for rows in by_number.values()]
    conn.execute("ALTER TABLE workflows RENAME TO workflows_legacy_pk_migrate")
    conn.execute(
        """
        CREATE TABLE workflows (
            workflow_number TEXT PRIMARY KEY,
            workflow_number_int INTEGER,
            workflow_id TEXT,
            workflow_title TEXT,
            review_outcome TEXT,
            review_status TEXT,
            step_1_completed_time TEXT,
            step_1_due_time TEXT,
            step_1_review_status TEXT,
            step_1_overdue_duration_or_status TEXT,
            step_2_completed_time TEXT,
            step_2_due_time TEXT,
            step_2_review_status TEXT,
            step_2_overdue_duration_or_status TEXT,
            is_completed INTEGER,
            last_checked_at TEXT,
            last_changed_at TEXT,
            source TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_workflows_open
            ON workflows(is_completed, workflow_number_int)
        """
    )
    placeholders = ", ".join("?" for _ in WORKFLOW_COLUMNS)
    columns = ", ".join(WORKFLOW_COLUMNS)
    for winner in winners:
        data = {column: winner.get(column) for column in WORKFLOW_COLUMNS}
        conn.execute(
            f"INSERT INTO workflows ({columns}) VALUES ({placeholders})",
            tuple(data[column] for column in WORKFLOW_COLUMNS),
        )
    conn.execute("DROP TABLE workflows_legacy_pk_migrate")
    return duplicated_numbers


def migrate_docflow_sync_to_number_pk(conn: sqlite3.Connection) -> None:
    if not table_exists(conn, "docflow_workflow_sync"):
        return
    primary_key = table_primary_key(conn, "docflow_workflow_sync")
    columns = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(docflow_workflow_sync)").fetchall()
    }
    if primary_key == "workflow_number" and "workflow_id" not in columns:
        return

    raw_rows = [dict(row) for row in conn.execute("SELECT * FROM docflow_workflow_sync").fetchall()]
    id_to_number = {
        str(row["workflow_id"]): str(row["workflow_number"])
        for row in conn.execute(
            "SELECT workflow_id, workflow_number FROM workflows WHERE workflow_id IS NOT NULL"
        ).fetchall()
        if row["workflow_id"] and row["workflow_number"]
    }
    by_number: dict[str, dict[str, Any]] = {}
    for row in raw_rows:
        number = str(row.get("workflow_number") or "").strip()
        if not number:
            workflow_id = str(row.get("workflow_id") or "").strip()
            number = id_to_number.get(workflow_id, workflow_id)
        if not number:
            continue
        existing = by_number.get(number)
        if existing is None or str(row.get("last_synced_at") or "") >= str(
            existing.get("last_synced_at") or ""
        ):
            by_number[number] = {
                "workflow_number": number,
                "payload_hash": row.get("payload_hash"),
                "last_synced_at": row.get("last_synced_at"),
            }

    conn.execute("ALTER TABLE docflow_workflow_sync RENAME TO docflow_workflow_sync_legacy")
    conn.execute(
        """
        CREATE TABLE docflow_workflow_sync (
            workflow_number TEXT PRIMARY KEY,
            payload_hash TEXT NOT NULL,
            last_synced_at TEXT NOT NULL
        )
        """
    )
    for row in by_number.values():
        if not row.get("payload_hash") or not row.get("last_synced_at"):
            continue
        conn.execute(
            """
            INSERT INTO docflow_workflow_sync (workflow_number, payload_hash, last_synced_at)
            VALUES (?, ?, ?)
            """,
            (row["workflow_number"], row["payload_hash"], row["last_synced_at"]),
        )
    conn.execute("DROP TABLE docflow_workflow_sync_legacy")
