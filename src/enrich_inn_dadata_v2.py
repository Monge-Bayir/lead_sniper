from __future__ import annotations

import os
import re
import time
import random
from pathlib import Path
from typing import Any, Dict, Optional, List

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

DADATA_TOKEN = os.getenv("DADATA_TOKEN", "").strip()
if not DADATA_TOKEN:
    raise RuntimeError("Нет DADATA_TOKEN в .env")

HEADERS = {
    "Authorization": f"Token {DADATA_TOKEN}",
    "Content-Type": "application/json",
    "Accept": "application/json",
    "User-Agent": "LeadSniperTest/1.0 (contact: ojdupool2004@mail.ru)",
}

LEGAL_TRASH = re.compile(
    r'["«»]|(\b(ООО|АО|ПАО|ЗАО|ОАО|ИП|ГБУЗ|ГУП|МУП|НКО)\b)|[,\.]',
    re.IGNORECASE
)

def clean_name(name: str) -> str:
    name = (name or "").strip()
    name = LEGAL_TRASH.sub(" ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name

def dadata_suggest_party(query: str, count: int = 5) -> List[Dict[str, Any]]:
    url = "https://suggestions.dadata.ru/suggestions/api/4_1/rs/suggest/party"
    payload = {
        "query": query,
        "count": count,
        # специально НЕ ставим status=["ACTIVE"], чтобы не потерять совпадения
    }
    r = requests.post(url, json=payload, headers=HEADERS, timeout=25)
    r.raise_for_status()
    return (r.json() or {}).get("suggestions", []) or []

def pick_inn_from_suggestions(sugs: List[Dict[str, Any]]) -> str:
    # берём первый, где inn есть
    for s in sugs:
        data = s.get("data") or {}
        inn = (data.get("inn") or "").strip()
        if inn:
            return inn
    return ""

def main() -> None:
    inp = DATA_DIR / "companies_stage4_v2.csv"
    out = DATA_DIR / "companies_stage5_v2.csv"

    df = pd.read_csv(inp, dtype=str, keep_default_na=False)

    filled_before = int((df["inn"].astype(str) != "").sum())

    for i, r in df.iterrows():
        if (r.get("inn") or "").strip():
            continue

        raw_name = (r.get("name") or "").strip()
        site = (r.get("site") or "").strip()
        q1 = raw_name
        q2 = clean_name(raw_name)

        try:
            sugs = dadata_suggest_party(q1, count=5)
            inn = pick_inn_from_suggestions(sugs)

            # если не нашли — попробуем очищенное имя
            if not inn and q2 and q2 != q1:
                time.sleep(0.35 + random.random() * 0.25)
                sugs = dadata_suggest_party(q2, count=5)
                inn = pick_inn_from_suggestions(sugs)

            if inn:
                df.at[i, "inn"] = inn
                df.at[i, "inn_source"] = "dadata"

        except Exception as e:
            print("Failed:", raw_name, site, repr(e))

        time.sleep(0.4 + random.random() * 0.3)

    df.to_csv(out, index=False, encoding="utf-8")
    filled_after = int((df["inn"].astype(str) != "").sum())
    print(f"Saved: {len(df)} rows -> {out}")
    print("INN filled before:", filled_before)
    print("INN filled after: ", filled_after)

if __name__ == "__main__":
    main()