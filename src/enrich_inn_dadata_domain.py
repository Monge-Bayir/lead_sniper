from __future__ import annotations

import os
import re
import time
import random
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import urlparse

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

LEGAL_TRASH = re.compile(r'["«»]|(\b(ООО|АО|ПАО|ЗАО|ОАО|ИП|ГБУЗ|ГУП|МУП|НКО)\b)|[,\.]', re.IGNORECASE)

def clean_name(name: str) -> str:
    name = (name or "").strip()
    name = LEGAL_TRASH.sub(" ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name

def domain_from_site(site: str) -> str:
    site = (site or "").strip()
    if not site:
        return ""
    if not site.startswith("http"):
        site = "https://" + site
    try:
        return urlparse(site).netloc.lower()
    except Exception:
        return ""

def dadata_suggest_party(query: str, count: int = 5) -> List[Dict[str, Any]]:
    url = "https://suggestions.dadata.ru/suggestions/api/4_1/rs/suggest/party"
    payload = {"query": query, "count": count}
    r = requests.post(url, json=payload, headers=HEADERS, timeout=25)
    r.raise_for_status()
    return (r.json() or {}).get("suggestions", []) or []

def pick_inn(sugs: List[Dict[str, Any]]) -> str:
    for s in sugs:
        inn = ((s.get("data") or {}).get("inn") or "").strip()
        if inn:
            return inn
    return ""

def main() -> None:
    inp = DATA_DIR / "companies_stage1.csv"
    out = DATA_DIR / "companies_stage6_dadata_domain.csv"

    df = pd.read_csv(inp, dtype=str, keep_default_na=False)
    before = int((df["inn"].astype(str) != "").sum())

    for i, r in df.iterrows():
        if (r.get("inn") or "").strip():
            continue

        name = (r.get("name") or "").strip()
        site = (r.get("site") or "").strip()
        dom = domain_from_site(site)

        try:
            inn = ""

            # 1) домен (самый сильный запрос)
            if dom:
                sugs = dadata_suggest_party(dom, count=8)
                inn = pick_inn(sugs)

            # 2) сырое имя
            if not inn and name:
                sugs = dadata_suggest_party(name, count=8)
                inn = pick_inn(sugs)

            # 3) очищенное имя
            if not inn:
                cn = clean_name(name)
                if cn and cn != name:
                    sugs = dadata_suggest_party(cn, count=8)
                    inn = pick_inn(sugs)

            if inn:
                df.at[i, "inn"] = inn
                df.at[i, "inn_source"] = "dadata"

        except Exception as e:
            print("Failed:", name, dom, repr(e))

        time.sleep(0.4 + random.random() * 0.3)

    df.to_csv(out, index=False, encoding="utf-8")
    after = int((df["inn"].astype(str) != "").sum())
    print(f"Saved: {len(df)} rows -> {out}")
    print("INN filled before:", before)
    print("INN filled after: ", after)

if __name__ == "__main__":
    main()