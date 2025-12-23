from __future__ import annotations

from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_RAW = BASE_DIR / "data" / "raw"
DATA_OUT = BASE_DIR / "data"
DATA_RAW.mkdir(parents=True, exist_ok=True)
DATA_OUT.mkdir(parents=True, exist_ok=True)

def main() -> None:
    evidence_path = DATA_RAW / "support_evidence_jobs.csv"
    employers_path = DATA_RAW / "employers_seeds.csv"
    out_path = DATA_OUT / "companies_stage1.csv"

    ev = pd.read_csv(evidence_path, dtype=str, keep_default_na=False)
    emp = pd.read_csv(employers_path, dtype=str, keep_default_na=False)

    # Мёрж по employer_id
    merged = ev.merge(
        emp[["employer_id", "employer_name", "employer_url"]],
        on="employer_id",
        how="left",
    )

    # Приводим к формату, близкому к ТЗ (пока без ИНН и сайта)
    merged = merged.rename(
        columns={
            "employer_name": "name",
            "employer_url": "source_hh_employer_url",
        }
    )

    # Пока site не знаем — оставим пустым (дальше добудем с сайта/реквизитов)
    merged["site"] = ""
    merged["source"] = "hh_api"
    merged["has_support_email"] = 0
    merged["has_contact_form"] = 0
    merged["has_online_chat"] = 0
    merged["has_messengers"] = 0
    merged["has_support_section"] = 0
    merged["has_kb_or_faq"] = 0
    merged["mentions_24_7"] = merged["mentions_24_7"].astype(int)
    # shift_work в ТЗ отдельного поля нет — можно оставить как доп. или не тащить
    # merged["shift_work"] = merged["shift_work"].astype(int)

    # Минимальный набор колонок “как в ТЗ”
    cols = [
        "employer_id",
        "name",
        "site",
        "support_team_size_min",
        "support_evidence",
        "evidence_url",
        "evidence_type",
        "source",
        "source_hh_employer_url",
        "has_support_email",
        "has_contact_form",
        "has_online_chat",
        "has_messengers",
        "has_support_section",
        "has_kb_or_faq",
        "mentions_24_7",
    ]
    merged = merged[cols]

    merged.to_csv(out_path, index=False, encoding="utf-8")
    print(f"Saved: {len(merged)} rows -> {out_path}")

if __name__ == "__main__":
    main()