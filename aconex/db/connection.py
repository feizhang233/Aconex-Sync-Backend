from __future__ import annotations

from pathlib import Path
import sqlite3

from aconex.config import ROOT_DIR

from .schema import apply_migrations


DEFAULT_DB_PATH = ROOT_DIR / "data" / "state" / "aconex.sqlite"


def resolve_db_path(db_path: str | Path | None = None) -> Path:
    return Path(db_path) if db_path is not None else DEFAULT_DB_PATH


def connect(db_path: str | Path | None = None) -> sqlite3.Connection:
    """Open SQLite and apply pending schema migrations once.

    After the database reaches the current schema version this is a single
    metadata lookup, not a table rebuild or data rewrite.
    """
    path = resolve_db_path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    apply_migrations(conn)
    conn.commit()
    return conn


def init_db(db_path: str | Path | None = None) -> Path:
    path = resolve_db_path(db_path)
    with connect(path):
        pass
    return path
