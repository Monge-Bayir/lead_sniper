from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_RAW = BASE_DIR / "data" / "raw"
DATA_RAW.mkdir(parents=True, exist_ok=True)

# --- B: признаки масштаба ---
B_PATTERNS = {
    "mentions_24_7": re.compile(r"\b24\s*/\s*7\b|круглосуточ|24\s*час", re.IGNORECASE),
    "shift_work": re.compile(
        r"\b2\s*/\s*2\b|\b3\s*/\s*3\b|сменн(ый|ая)\s+график|ночн(ые|ая)\s+смен",
        re.IGNORECASE,
    ),
}

# --- A: число рядом с поддержкой ---
A_NUMBER_NEAR_SUPPORT = re.compile(
    r"(?P<n>\d{2,4})\s*(чел(овек)?|сотрудник(ов)?|специалист(ов)?|оператор(ов)?)"
    r".{0,60}(поддержк|саппорт|support|контакт[\s-]?центр|call[\s-]?центр)",
    re.IGNORECASE | re.DOTALL,
)

A_SUPPORT_NEAR_NUMBER = re.compile(
    r"(поддержк|саппорт|support|контакт[\s-]?центр|call[\s-]?центр)"
    r".{0,60}(?P<n>\d{2,4})\s*(чел(овек)?|сотрудник(ов)?|специалист(ов)?|оператор(ов)?)",
    re.IGNORECASE | re.DOTALL,
)

TAG_RE = re.compile(r"<[^>]+>")

def strip_html(x: str) -> str:
    x = TAG_RE.sub(" ", x)
    x = re.sub(r"\s+", " ", x).strip()
    return x

def extract_a_size(text: str) -> Optional[int]:
    m = A_NUMBER_NEAR_SUPPORT.search(text)
    if m:
        return int(m.group("n"))
    m = A_SUPPORT_NEAR_NUMBER.search(text)
    if m:
        return int(m.group("n"))
    return None

def extract_b_flags(text: str) -> Dict[str, int]:
    return {k: int(bool(rx.search(text))) for k, rx in B_PATTERNS.items()}

def main() -> None:
    inp = DATA_RAW / "vacancies_support_only.csv"
    out = DATA_RAW / "support_evidence_jobs.csv"

    df = pd.read_csv(inp, dtype=str, keep_default_na=False)

    rows: List[Dict[str, Any]] = []

    for _, r in df.iterrows():
        employer_id = r.get("employer_id", "").strip()
        if not employer_id:
            continue

        desc = strip_html(r.get("description", "") or "")
        a_size = extract_a_size(desc)
        b_flags = extract_b_flags(desc)

        support_team_size_min = None
        support_evidence = None
        evidence_type = "jobs"

        # Уровень A
        if a_size is not None and a_size >= 10:
            support_team_size_min = a_size
            support_evidence = f"Прямое указание размера команды поддержки: {a_size} человек."

        # Уровень B
        elif b_flags["mentions_24_7"] or b_flags["shift_work"]:
            support_team_size_min = 10
            reasons = []
            if b_flags["mentions_24_7"]:
                reasons.append("упоминание 24/7/круглосуточно")
            if b_flags["shift_work"]:
                reasons.append("сменный график/ночные смены")
            support_evidence = "Признаки крупной поддержки: " + ", ".join(reasons) + ". По правилу B ставим минимум 10."

        if support_team_size_min is None:
            continue

        rows.append(
            {
                "employer_id": employer_id,
                "vacancy_id": r.get("vacancy_id"),
                "evidence_url": r.get("alternate_url"),
                "support_team_size_min": int(support_team_size_min),
                "support_evidence": support_evidence,
                "evidence_type": evidence_type,
                "mentions_24_7": b_flags["mentions_24_7"],
                "shift_work": b_flags["shift_work"],
            }
        )

    ev = pd.DataFrame(rows)

    # Агрегация: 1 строка = 1 компания (берём максимальную оценку)
    ev = (
        ev.sort_values(["employer_id", "support_team_size_min"], ascending=[True, False])
          .groupby("employer_id", as_index=False)
          .first()
    )

    ev.to_csv(out, index=False, encoding="utf-8")
    print(f"Saved: {len(ev)} companies -> {out}")

if __name__ == "__main__":
    main()