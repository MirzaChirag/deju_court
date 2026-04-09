#!/usr/bin/env python3
"""
Скачивает судебные акты с courts.gov.az в SQLite (court_acts.db).
Текст из PDF извлекается через pdfplumber; при отсутствии PDF — используется caseResult.
"""

from __future__ import annotations

import argparse
import base64
import io
import json
import sqlite3
import time
from typing import Any

import httpx
import pdfplumber

LIST_URL = "https://courts.gov.az/api/v1/court-acts"
DETAIL_URL = "https://courts.gov.az/api/v1/act-detail"
DB_PATH = "court_acts.db"
DELAY_SEC = 1.0

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://courts.gov.az/courtActs",
}

LIST_HEADERS = {
    **HEADERS,
    "Content-Type": "application/json",
}


def list_body(page: int) -> dict[str, Any]:
    return {
        "page": page,
        "court_id": "",
        "judge_id": [],
        "case_id": "",
        "exec_id": [],
        "decision_id": [],
        "category_id": [],
    }


def remove_base64_fields(obj: Any) -> Any:
    """Рекурсивно удаляет поля attachmentBase64 перед сохранением в raw_json."""
    if isinstance(obj, dict):
        return {
            k: remove_base64_fields(v)
            for k, v in obj.items()
            if k != "attachmentBase64"
        }
    if isinstance(obj, list):
        return [remove_base64_fields(x) for x in obj]
    return obj


def extract_pdf_text(detail: dict[str, Any]) -> str:
    """Декодирует PDF из attachments и извлекает текст страниц."""
    att = detail.get("attachments")
    if not isinstance(att, dict):
        return ""
    b64 = att.get("attachmentBase64")
    if not isinstance(b64, str) or not b64.strip():
        return ""
    try:
        pdf_bytes = base64.b64decode(b64)
    except (ValueError, TypeError):
        return ""
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            text = "\n".join(page.extract_text() or "" for page in pdf.pages)
    except Exception:
        return ""
    return text.strip()


def build_full_text(detail: dict[str, Any]) -> str:
    text = extract_pdf_text(detail)
    if text:
        return text
    cr = detail.get("caseResult")
    if isinstance(cr, str) and cr.strip():
        return cr.strip()
    return ""


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS court_acts (
            decision_id INTEGER PRIMARY KEY,
            case_number TEXT,
            date TEXT,
            court_name TEXT,
            full_text TEXT,
            raw_json TEXT
        )
        """
    )
    conn.commit()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Скачивание актов courts.gov.az в SQLite.")
    p.add_argument(
        "--limit",
        type=int,
        default=1000,
        metavar="N",
        help="максимальное число актов (по умолчанию: 1000)",
    )
    p.add_argument(
        "--start-page",
        type=int,
        default=1,
        metavar="NUMBER",
        help="номер первой страницы списка (по умолчанию: 1)",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    limit = max(1, args.limit)
    saved = 0
    page = max(1, args.start_page)

    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    with httpx.Client(headers=HEADERS, timeout=120.0) as client:
        while saved < limit:
            try:
                r = client.post(
                    f"{LIST_URL}?page={page}",
                    headers=LIST_HEADERS,
                    json=list_body(page),
                )
                r.raise_for_status()
                payload = r.json()
            except httpx.HTTPError:
                time.sleep(DELAY_SEC)
                page += 1
                continue
            time.sleep(DELAY_SEC)

            if not payload.get("success"):
                raise RuntimeError(f"Список актов: success=false, ответ: {payload!r}")

            acts = (payload.get("data") or {}).get("acts") or []
            if not acts:
                break

            for act in acts:
                if saved >= limit:
                    break

                decision_id = act.get("decisionId")
                if decision_id is None:
                    continue

                try:
                    dr = client.get(
                        DETAIL_URL,
                        params={"decision_id": decision_id},
                        headers=HEADERS,
                    )
                    dr.raise_for_status()
                    detail_payload = dr.json()
                except httpx.HTTPError:
                    time.sleep(DELAY_SEC)
                    continue
                time.sleep(DELAY_SEC)

                if not detail_payload.get("success"):
                    continue

                raw = remove_base64_fields(detail_payload)
                raw_json_str = json.dumps(raw, ensure_ascii=False)

                detail = (detail_payload.get("data") or {}).get("detail")
                if not isinstance(detail, dict):
                    detail = {}

                case_number = act.get("caseNo")
                if case_number is not None:
                    case_number = str(case_number)

                date_val = act.get("decisionDate")
                if date_val is not None:
                    date_val = str(date_val)

                court_name = act.get("court")
                if court_name is not None:
                    court_name = str(court_name)

                full_text = build_full_text(detail)

                conn.execute(
                    """
                    INSERT OR REPLACE INTO court_acts
                    (decision_id, case_number, date, court_name, full_text, raw_json)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        int(decision_id),
                        case_number,
                        date_val,
                        court_name,
                        full_text,
                        raw_json_str,
                    ),
                )
                conn.commit()
                saved += 1
                print(f"Сохранено: {saved} / {limit}", flush=True)

            page += 1

    conn.close()
    print(f"Готово! Сохранено актов: {saved}")


if __name__ == "__main__":
    main()
