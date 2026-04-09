"""
Путь к БД и схема FTS5 для поиска по судебным актам.
"""

from __future__ import annotations

import os
import sqlite3

DB_PATH = os.environ.get("DB_PATH", "court_acts.db")

FTS_CREATE_SQL = """
CREATE VIRTUAL TABLE IF NOT EXISTS court_acts_fts
USING fts5(
    decision_id UNINDEXED,
    case_number,
    court_name,
    full_text,
    tokenize='unicode61'
);
"""


def connect(*, row_factory: type | None = None) -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    if row_factory is not None:
        conn.row_factory = row_factory
    return conn


def init_fts_schema(conn: sqlite3.Connection) -> None:
    """Создаёт виртуальную таблицу court_acts_fts (без заполнения)."""
    conn.executescript(FTS_CREATE_SQL)
    conn.commit()
