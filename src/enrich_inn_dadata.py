from __future__ import annotations

import os
import time
import random
from pathlib import Path
from typing import Any, Dict, Optional

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

def dadata_suggest_party(name: str) -> Optional[Dict[str, Any]]:
    url = "https://suggestions.dadata.ru/suggestions/api/4_1/rs/suggest/party"
    payload = {
        "query": name,
        "count": 5,
        "status": ["ACTIVE"],  # активные
    }
    r = requests.post(url, json=payload, headers=HEADERS, timeout=25)
    r.raise_for_status()
    data = r.json()
    suggestions = data.get("suggestions", [])
    return suggestions[0] if suggestions else None

def main() -> None:
    inp = DATA_DIR / "companies_stage4_v2.csv"
    out = DATA_DIR / "companies_stage5.csv"

    df = pd.read_csv(inp, dtype=str, keep_default_na=False)

    filled_before = int((df["inn"].astype(str) != "").sum())

    for i, r in df.iterrows():
        if (r.get("inn") or "").strip():
            continue

        name = (r.get("name") or "").strip()
        if not name:
            continue

        try:
            sug = dadata_suggest_party(name)
            if sug:
                inn = (sug.get("data") or {}).get("inn", "") or ""
                if inn:
                    df.at[i, "inn"] = inn
                    df.at[i, "inn_source"] = "dadata"
        except Exception as e:
            print("Failed:", name, repr(e))

        time.sleep(0.4 + random.random() * 0.3)

    df.to_csv(out, index=False, encoding="utf-8")
    filled_after = int((df["inn"].astype(str) != "").sum())
    print(f"Saved: {len(df)} rows -> {out}")
    print("INN filled before:", filled_before)
    print("INN filled after: ", filled_after)

if __name__ == "__main__":
    main()