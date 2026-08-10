from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
from typing import Any

from aconex.config import ROOT_DIR


DEFAULT_DB_PATH = ROOT_DIR / "data" / "state" / "aconex.sqlite"

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


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _db_path(db_path: str | Path | None = None) -> Path:
    return Path(db_path) if db_path is not None else DEFAULT_DB_PATH


def _connect(db_path: str | Path | None = None) -> sqlite3.Connection:
    path = _db_path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def _rows_to_dicts(rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows]


def _json_dump(value: Any) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def init_db(db_path: str | Path | None = None) -> Path:
    path = _db_path(db_path)
    with _connect(path) as conn:
        _ensure_schema(conn)
        _migrate_workflows_to_number_pk(conn)
        _migrate_docflow_sync_to_number_pk(conn)
        conn.execute(
            """
            UPDATE workflows
            SET review_status = 'Terminate',
                last_changed_at = ?
            WHERE lower(trim(coalesce(review_outcome, ''))) = 'none'
              AND coalesce(review_status, '') != 'Terminate'
            """,
            (_utc_now(),),
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
    return path


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
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
    )
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


def _ensure_columns(conn: sqlite3.Connection, table_name: str, columns: Mapping[str, str]) -> None:
    existing = {
        row["name"]
        for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    }
    for column, column_type in columns.items():
        if column not in existing:
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column} {column_type}")


def _table_primary_key(conn: sqlite3.Connection, table_name: str) -> str | None:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    for row in rows:
        if int(row["pk"] or 0) == 1:
            return str(row["name"])
    return None


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


def _select_winner_workflow(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
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
        # Keep the smallest Aconex step WorkflowId as the stable auxiliary id.
        winner_id = str(winner.get("workflow_id") or "")
        sibling_id = str(sibling.get("workflow_id") or "")
        if sibling_id and (not winner_id or sibling_id < winner_id):
            winner["workflow_id"] = sibling_id
    if winner.get("is_completed") in (None, ""):
        winner["is_completed"] = 0
    return winner


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
        # Negate lexical order via inverse sort of empty-padded id: use second pass.
        # We reverse=True overall, so put inverted id as negative by using empty last.
        # Use a pair: prefer non-empty ids, then smaller id via reverse of id string with max-fill.
        1 if workflow_id else 0,
        "".join(chr(255 - ord(ch)) for ch in workflow_id) if workflow_id else "",
    )


def _migrate_workflows_to_number_pk(conn: sqlite3.Connection) -> list[str]:
    """Rebuild workflows with workflow_number PK and collapse duplicate numbers.

    Returns workflow numbers that were collapsed from more than one row.
    """
    if not _table_exists(conn, "workflows"):
        return []

    primary_key = _table_primary_key(conn, "workflows")
    raw_rows = [dict(row) for row in conn.execute("SELECT * FROM workflows").fetchall()]
    by_number: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in raw_rows:
        number = str(row.get("workflow_number") or "").strip()
        if not number:
            # Fallback for corrupt rows: keep under workflow_id so data is not dropped.
            number = str(row.get("workflow_id") or "").strip()
            if not number:
                continue
            row["workflow_number"] = number
        by_number[number].append(row)

    duplicated_numbers = sorted(number for number, rows in by_number.items() if len(rows) > 1)
    needs_rebuild = primary_key != "workflow_number" or bool(duplicated_numbers)
    if not needs_rebuild:
        return []

    winners = [_select_winner_workflow(rows) for rows in by_number.values()]
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


def _migrate_docflow_sync_to_number_pk(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "docflow_workflow_sync"):
        return
    primary_key = _table_primary_key(conn, "docflow_workflow_sync")
    columns = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(docflow_workflow_sync)").fetchall()
    }
    if primary_key == "workflow_number" and "workflow_id" not in columns:
        return

    raw_rows = [dict(row) for row in conn.execute("SELECT * FROM docflow_workflow_sync").fetchall()]
    # Map old workflow_id-based hashes onto workflow_number via current workflows table.
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


def dedupe_workflows(db_path: str | Path | None = None) -> list[str]:
    """Collapse duplicate workflow_number rows. Returns numbers that were merged."""
    path = _db_path(db_path)
    with _connect(path) as conn:
        _ensure_schema(conn)
        collapsed = _migrate_workflows_to_number_pk(conn)
        _migrate_docflow_sync_to_number_pk(conn)
        return collapsed


def upsert_workflow(row: Mapping[str, Any], db_path: str | Path | None = None) -> bool:
    init_db(db_path)
    data = {column: row.get(column) for column in WORKFLOW_COLUMNS}
    workflow_number = str(data.get("workflow_number") or "").strip()
    if not workflow_number:
        raise ValueError("workflow_number is required")
    data["workflow_number"] = workflow_number
    # workflow_id is auxiliary (one of Aconex's per-step ids); optional.
    if data.get("workflow_id") is not None:
        data["workflow_id"] = str(data["workflow_id"]).strip() or None

    now = _utc_now()
    data["last_checked_at"] = data["last_checked_at"] or now

    with _connect(db_path) as conn:
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
    init_db(db_path)
    with _connect(db_path) as conn:
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
    init_db(db_path)
    with _connect(db_path) as conn:
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
    init_db(db_path)
    data = {column: row.get(column) for column in WORKFLOW_COMMENT_COLUMNS}
    if not data["workflow_number"]:
        raise ValueError("workflow_number is required")
    if not data["mail_id"]:
        raise ValueError("mail_id is required")
    data["created_at"] = data["created_at"] or _utc_now()

    with _connect(db_path) as conn:
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
    init_db(db_path)
    with _connect(db_path) as conn:
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
    init_db(db_path)
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM workflows ORDER BY workflow_number_int, workflow_number"
        ).fetchall()
    return _rows_to_dicts(rows)


def load_docflow_sync_state(db_path: str | Path | None = None) -> dict[str, str]:
    """Return the last successfully handled DocFlow payload hash by workflow number."""
    init_db(db_path)
    with _connect(db_path) as conn:
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
    init_db(db_path)
    number = str(workflow_number or "").strip()
    if not number:
        raise ValueError("workflow_number is required")
    with _connect(db_path) as conn:
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


def load_workflow_comments(db_path: str | Path | None = None) -> list[dict[str, Any]]:
    init_db(db_path)
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM workflow_comments ORDER BY workflow_number_int, sent_date, mail_number"
        ).fetchall()
    return _rows_to_dicts(rows)


def load_update_runs(db_path: str | Path | None = None) -> list[dict[str, Any]]:
    init_db(db_path)
    with _connect(db_path) as conn:
        rows = conn.execute("SELECT * FROM update_runs ORDER BY id").fetchall()
    return _rows_to_dicts(rows)
