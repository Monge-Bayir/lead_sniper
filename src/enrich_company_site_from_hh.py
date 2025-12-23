from __future__ import annotations

import re
import time
import random
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DATA_RAW = BASE_DIR / "data" / "raw"
DATA_DIR.mkdir(parents=True, exist_ok=True)
DATA_RAW.mkdir(parents=True, exist_ok=True)

CONTACT_EMAIL = "ojdupool2004@mail.ru"

# простой regex для поиска внешних ссылок (не hh.ru)
URL_RE = re.compile(r'https?://[^\s"\']+')

def get_html(url: str) -> str:
    headers = {
        "User-Agent": f"LeadSniperTest/1.0 (contact: {CONTACT_EMAIL})",
        "Accept": "text/html,application/xhtml+xml",
    }
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.text

def pick_company_site(html: str) -> Optional[str]:
    # 1) ищем явный блок "Сайт" рядом со ссылкой (приблизительно)
    # hh может меняться, поэтому делаем гибко: просто берём внешние ссылки
    urls = URL_RE.findall(html)
    # чистим мусор и оставляем внешние домены
    cleaned = []
    for u in urls:
        u = u.strip().rstrip(').,;\'"')
        if "hh.ru" in u:
            continue
        if "hhcdn.ru" in u:
            continue
        if u.startswith("https://vk.com") or u.startswith("https://t.me") or u.startswith("https://ok.ru"):
            # соцсети не считаем "site"
            continue
        cleaned.append(u)

    # убираем дубли сохраняя порядок
    seen = set()
    uniq = []
    for u in cleaned:
        if u not in seen:
            uniq.append(u)
            seen.add(u)

    return uniq[0] if uniq else None

def main() -> None:
    inp = DATA_DIR / "companies_stage1.csv"
    out = DATA_DIR / "companies_stage2.csv"

    df = pd.read_csv(inp, dtype=str, keep_default_na=False)

    sites = []
    for _, r in df.iterrows():
        hh_url = r.get("source_hh_employer_url", "").strip()
        if not hh_url:
            sites.append("")
            continue

        try:
            html = get_html(hh_url)
            site = pick_company_site(html) or ""
            sites.append(site)
            time.sleep(0.6 + random.random() * 0.4)
        except Exception as e:
            print("Failed:", hh_url, repr(e))
            sites.append("")
            time.sleep(0.8)

    df["site"] = sites
    df.to_csv(out, index=False, encoding="utf-8")
    print(f"Saved: {len(df)} rows -> {out}")
    print("Sites filled:", int((df["site"] != "").sum()))

if __name__ == "__main__":
    main()