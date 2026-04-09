#!/usr/bin/env python3
"""
Один раз создаёт и заполняет FTS5-индекс из таблицы court_acts.
Запуск: python build_fts.py
"""

from __future__ import annotations

import os
import sqlite3

from database import DB_PATH, init_fts_schema

BATCH = 1000


def main() -> None:
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("DROP TABLE IF EXISTS court_acts_fts")
        conn.commit()
        init_fts_schema(conn)

        total = conn.execute("SELECT COUNT(*) FROM court_acts").fetchone()[0]
        print(f"Всего записей в court_acts: {total}")
        if total == 0:
            print("Нечего импортировать.")
            return

        offset = 0
        while offset < total:
            rows = conn.execute(
                """
                SELECT decision_id, case_number, court_name, full_text
                FROM court_acts
                LIMIT ? OFFSET ?
                """,
                (BATCH, offset),
            ).fetchall()
            if not rows:
                break
            conn.executemany(
                """
                INSERT INTO court_acts_fts(decision_id, case_number, court_name, full_text)
                VALUES (?, ?, ?, ?)
                """,
                rows,
            )
            conn.commit()
            offset += len(rows)
            if offset % 1000 == 0 or offset >= total:
                print(f"Импортировано: {min(offset, total)} / {total}", flush=True)

        print("Готово.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
