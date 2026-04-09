#!/usr/bin/env python3
"""
Веб-поиск по судебным актам через SQLite FTS5 (unicode61).
"""

from __future__ import annotations

import os
import math
import sqlite3

DB_PATH = os.environ.get('DB_PATH', '/app/court_acts.db')

print(f"DB_PATH: {DB_PATH}")
print(f"Files in /app: {os.listdir('/app')}")

from flask import Flask, abort, render_template, request, url_for

import database

database.DB_PATH = DB_PATH
from database import connect

app = Flask(__name__)

PER_PAGE = 20

MONTHS_AZ = {
    "01": "yanvar",
    "02": "fevral",
    "03": "mart",
    "04": "aprel",
    "05": "may",
    "06": "iyun",
    "07": "iyul",
    "08": "avqust",
    "09": "sentyabr",
    "10": "oktyabr",
    "11": "noyabr",
    "12": "dekabr",
}

RESULT_TYPE_LABELS = [
    ("", "Bütün növlər"),
    ("Təmin edilib", "Təmin edilib"),
    ("Qismən təmin edilib", "Qismən təmin edilib"),
    ("Rədd edilib", "Rədd edilib"),
    ("Xitam verilib", "Xitam verilib"),
]

_FTS_FORBIDDEN = frozenset('":*^(){}[]')

_filter_cache: dict[str, list] | None = None


def format_date(date_str: str | None) -> str:
    """2026-02-19T00:00:00 → 19 fevral 2026"""
    if not date_str:
        return ""
    s = str(date_str).strip()
    if "T" in s:
        s = s.split("T", 1)[0]
    parts = s.split("-")
    if len(parts) == 3 and all(p.isdigit() for p in parts):
        y, m, d = parts
        mo = MONTHS_AZ.get(m.zfill(2), m)
        return f"{int(d)} {mo} {y}"
    return s


@app.template_filter("format_date")
def format_date_filter(value: str | None) -> str:
    return format_date(value)


def _db() -> sqlite3.Connection:
    return connect(row_factory=sqlite3.Row)


def get_filter_data() -> dict[str, list]:
    global _filter_cache
    if _filter_cache is None:
        conn = connect()
        try:
            courts = [
                r[0]
                for r in conn.execute(
                    """
                    SELECT DISTINCT court_name FROM court_acts
                    WHERE court_name IS NOT NULL AND trim(court_name) != ''
                    ORDER BY court_name COLLATE NOCASE
                    """
                ).fetchall()
            ]
            years = [
                r[0]
                for r in conn.execute(
                    """
                    SELECT DISTINCT substr(date, 1, 4) AS y FROM court_acts
                    WHERE date IS NOT NULL AND length(date) >= 4
                    ORDER BY y DESC
                    """
                ).fetchall()
                if r[0]
            ]
        finally:
            conn.close()
        _filter_cache = {"courts": courts, "years": years}
    return _filter_cache


def build_fts_match_query(raw: str) -> str | None:
    raw = (raw or "").strip()
    if not raw:
        return None
    if len(raw) >= 2 and raw[0] == '"' and raw[-1] == '"':
        inner = raw[1:-1].strip()
        if not inner:
            return None
        inner = inner.replace('"', '""')
        return f'"{inner}"'
    tokens: list[str] = []
    for w in raw.split():
        w = w.strip()
        if not w:
            continue
        cleaned = "".join(ch for ch in w if ch not in _FTS_FORBIDDEN)
        if not cleaned:
            continue
        tokens.append(f"{cleaned}*")
    return " ".join(tokens) if tokens else None


def _occurrence_needle(raw_query: str) -> str:
    """Строка для подсчёта вхождений в full_text (нижний регистр)."""
    s = (raw_query or "").strip()
    if len(s) >= 2 and s[0] == '"' and s[-1] == '"':
        s = s[1:-1].strip()
    return s.lower()


def _search_filters_clause(
    court: str, year: str, result: str
) -> tuple[str, list]:
    parts: list[str] = []
    params: list[str] = []
    if court:
        parts.append("c.court_name = ?")
        params.append(court)
    if year:
        parts.append("substr(c.date, 1, 4) = ?")
        params.append(year)
    if result:
        parts.append("c.full_text LIKE '%' || ? || '%'")
        params.append(result)
    return (" AND " + " AND ".join(parts)) if parts else "", params


def search(
    query: str,
    *,
    court: str = "",
    year: str = "",
    result: str = "",
    page: int = 1,
) -> tuple[list[dict], int]:
    query = (query or "").strip()
    if not query:
        return [], 0, page

    fts_q = build_fts_match_query(query)
    if not fts_q:
        return [], 0, page

    filt_sql, filt_params = _search_filters_clause(court, year, result)
    needle = _occurrence_needle(query)
    if not needle:
        needle_occ = ""
    else:
        needle_occ = needle

    count_sql = f"""
        SELECT COUNT(*) AS cnt
        FROM court_acts_fts
        JOIN court_acts c ON c.decision_id = court_acts_fts.decision_id
        WHERE court_acts_fts MATCH ?
        {filt_sql}
    """
    select_with_occ = f"""
        SELECT c.decision_id, c.case_number, c.date, c.court_name,
               snippet(court_acts_fts, 3, '<mark>', '</mark>', '...', 32) AS snippet
        FROM court_acts_fts
        JOIN court_acts c ON c.decision_id = court_acts_fts.decision_id
        WHERE court_acts_fts MATCH ?
        {filt_sql}
        ORDER BY rank ASC,
          (LENGTH(lower(coalesce(c.full_text, '')))
           - LENGTH(REPLACE(lower(coalesce(c.full_text, '')), ?, '')))
          DESC
        LIMIT ? OFFSET ?
    """
    select_plain = f"""
        SELECT c.decision_id, c.case_number, c.date, c.court_name,
               snippet(court_acts_fts, 3, '<mark>', '</mark>', '...', 32) AS snippet
        FROM court_acts_fts
        JOIN court_acts c ON c.decision_id = court_acts_fts.decision_id
        WHERE court_acts_fts MATCH ?
        {filt_sql}
        ORDER BY rank ASC
        LIMIT ? OFFSET ?
    """

    page = max(1, page)
    conn = _db()
    try:
        count_params = (fts_q, *filt_params)
        total_row = conn.execute(count_sql, count_params).fetchone()
        total = int(total_row["cnt"]) if total_row else 0

        max_page = max(1, (total + PER_PAGE - 1) // PER_PAGE) if total else 1
        page = min(page, max_page)
        offset = (page - 1) * PER_PAGE

        if needle_occ:
            sel_sql = select_with_occ
            sel_params = (fts_q, *filt_params, needle_occ, PER_PAGE, offset)
        else:
            sel_sql = select_plain
            sel_params = (fts_q, *filt_params, PER_PAGE, offset)

        try:
            rows = conn.execute(sel_sql, sel_params).fetchall()
        except sqlite3.OperationalError:
            rows = []
            total = 0
    finally:
        conn.close()

    out = [
        {
            "decision_id": r["decision_id"],
            "case_number": r["case_number"] or "",
            "date": r["date"] or "",
            "court_name": r["court_name"] or "",
            "snippet_html": r["snippet"] or "",
        }
        for r in rows
    ]
    return out, total, page


def _pagination_urls(
    q: str,
    court: str,
    year: str,
    result: str,
    page: int,
    total: int,
) -> tuple[str | None, str | None, int]:
    total_pages = max(1, (total + PER_PAGE - 1) // PER_PAGE) if total else 1
    page = max(1, min(page, total_pages))

    def args(p: int) -> dict:
        d: dict = {"q": q, "page": p}
        if court:
            d["court"] = court
        if year:
            d["year"] = year
        if result:
            d["result"] = result
        return d

    prev_u = None
    next_u = None
    if q and total:
        if page > 1:
            prev_u = url_for("search_route", **args(page - 1))
        if page < total_pages:
            next_u = url_for("search_route", **args(page + 1))
    return prev_u, next_u, total_pages


def _render_index(**kw):
    fd = get_filter_data()
    return render_template(
        "index.html",
        courts=fd["courts"],
        years=fd["years"],
        result_types=RESULT_TYPE_LABELS,
        **kw,
    )


@app.route("/")
def index():
    q = request.args.get("q", "").strip()
    court = request.args.get("court", "").strip()
    year = request.args.get("year", "").strip()
    result = request.args.get("result", "").strip()
    try:
        page = int(request.args.get("page", "1"))
    except ValueError:
        page = 1

    results: list[dict] = []
    total = 0
    prev_url = None
    next_url = None
    total_pages = 1

    if q:
        results, total, page = search(
            q, court=court, year=year, result=result, page=page
        )
        prev_url, next_url, total_pages = _pagination_urls(
            q, court, year, result, page, total
        )

    return _render_index(
        q=q,
        court=court,
        year=year,
        result=result,
        page=page,
        results=results,
        total_count=total,
        prev_url=prev_url,
        next_url=next_url,
        total_pages=total_pages,
        per_page=PER_PAGE,
    )


@app.route("/search")
def search_route():
    return index()


@app.route("/act/<int:decision_id>")
def act_detail(decision_id: int):
    conn = _db()
    try:
        row = conn.execute(
            "SELECT decision_id, case_number, date, court_name, full_text FROM court_acts WHERE decision_id = ?",
            (decision_id,),
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        abort(404)
    highlight_q = request.args.get("q", "")
    return render_template(
        "act.html",
        decision_id=row["decision_id"],
        case_number=row["case_number"] or "",
        date=row["date"] or "",
        court_name=row["court_name"] or "",
        full_text=row["full_text"] or "",
        q=highlight_q,
    )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
