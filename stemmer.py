"""
Простой азербайджанский стеммер (удаление суффиксов).
"""

from __future__ import annotations

# Суффиксы без дефиса, от самых длинных к коротким
_SUFFIXES_ORDERED: tuple[str, ...] = (
    "çılıq",
    "çilik",
    "ların",
    "lərin",
    "nın",
    "nin",
    "nun",
    "nün",
    "maq",
    "mək",
    "dan",
    "dən",
    "tan",
    "tən",
    "lar",
    "lər",
    "da",
    "də",
    "ta",
    "tə",
    "ım",
    "im",
    "um",
    "üm",
    "ın",
    "in",
    "un",
    "ün",
    "ya",
    "yə",
    "na",
    "nə",
    "ır",
    "ir",
    "ur",
    "ür",
    "ar",
    "ər",
    "dı",
    "di",
    "du",
    "dü",
    "ıb",
    "ib",
    "ub",
    "üb",
    "lı",
    "li",
    "lu",
    "lü",
    "sız",
    "siz",
    "suz",
    "süz",
    "çı",
    "çi",
    "çu",
    "çü",
    "ı",
    "i",
    "u",
    "ü",
)


def stem(word: str) -> str:
    """
    Возвращает основу слова, последовательно снимая известные суффиксы.
    Останавливается, когда длина основы < 3 или суффиксов больше не найдено.
    """
    w = word.lower().strip()
    if len(w) < 3:
        return w

    changed = True
    while changed and len(w) >= 3:
        changed = False
        for suf in _SUFFIXES_ORDERED:
            if w.endswith(suf) and len(w) - len(suf) >= 3:
                w = w[: -len(suf)]
                changed = True
                break
    return w


def expand_query(word: str) -> list[str]:
    """
    Варианты для поиска по одному слову:
    1) исходное (нижний регистр)
    2) основа (stem)
    3) три первые буквы основы — для частичного совпадения
    """
    w = word.lower().strip()
    if not w:
        return []

    out: list[str] = []
    seen: set[str] = set()

    def add(s: str) -> None:
        if s and s not in seen:
            seen.add(s)
            out.append(s)

    add(w)
    s = stem(w)
    add(s)
    if len(s) >= 3:
        add(s[:3])
    return out
