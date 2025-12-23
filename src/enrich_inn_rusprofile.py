from __future__ import annotations

import re
import time
import random
from pathlib import Path
from typing import Optional
from urllib.parse import quote_plus, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "User-Agent": "LeadSniperTest/1.0 (contact: ojdupool2004@mail.ru)",
    "Accept": "text/html,application/xhtml+xml",
}

INN_RE = re.compile(r"\bИНН\b\D{0,40}(\d{10}|\d{12})")
URL_RE = re.compile(r"^https?://")

def domain_from_site(site: str) -> str:
    site = (site or "").strip()
    if not site:
        return ""
    if not URL_RE.match(site):
        site = "https://" + site
    try:
        p = urlparse(site)
        return p.netloc.lower()
    except Exception:
        return ""

def http_get(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=25, allow_redirects=True)
    r.raise_for_status()
    return r.text

def rusprofile_search(query: str) -> Optional[str]:
    """
    Возвращает URL первой подходящей карточки компании на rusprofile.ru (если нашли).
    Мы ищем ссылку вида /id/<digits> или /company/<...>.
    """
    q = quote_plus(query)
    url = f"https://www.rusprofile.ru/search?query={q}"
    html = http_get(url)
    soup = BeautifulSoup(html, "lxml")

    # Ищем первую ссылку, похожую на карточку компании
    for a in soup.select('a[href]'):
        href = a.get("href", "")
        if not href:
            continue
        # типичные пути карточек
        if href.startswith("/id/") or "/company/" in href:
            if href.startswith("http"):
                return href
            return "https://www.rusprofile.ru" + href

    return None

def rusprofile_extract_inn(company_url: str) -> Optional[str]:
    html = http_get(company_url)
    # Быстрый regex по всему тексту страницы
    m = INN_RE.search(html)
    if m:
        return m.group(1)
    return None

def main() -> None:
    inp = DATA_DIR / "companies_stage2.csv"
    out = DATA_DIR / "companies_stage4.csv"

    df = pd.read_csv(inp, dtype=str, keep_default_na=False)

    inns = []
    sources = []

    for _, r in df.iterrows():
        name = (r.get("name") or "").strip()
        site = (r.get("site") or "").strip()

        inn = (r.get("inn") or "").strip()  # если вдруг уже есть
        if inn:
            inns.append(inn)
            sources.append(r.get("source", ""))
            continue

        # 1) пробуем по домену (точнее)
        dom = domain_from_site(site)
        card_url = None
        found_inn = None

        try:
            if dom:
                card_url = rusprofile_search(dom)
                time.sleep(0.8 + random.random() * 0.6)

            # 2) если не нашли — по названию
            if not card_url and name:
                card_url = rusprofile_search(name)
                time.sleep(0.8 + random.random() * 0.6)

            if card_url:
                found_inn = rusprofile_extract_inn(card_url)
                time.sleep(0.8 + random.random() * 0.6)

        except Exception as e:
            print("Failed:", name, dom, repr(e))

        inns.append(found_inn or "")
        sources.append("rusprofile" if found_inn else "")

    df["inn"] = inns
    # можно хранить, откуда ИНН
    df["inn_source"] = sources

    df.to_csv(out, index=False, encoding="utf-8")
    print(f"Saved: {len(df)} rows -> {out}")
    print("INN filled:", int((df["inn"].astype(str) != "").sum()))

if __name__ == "__main__":
    main()