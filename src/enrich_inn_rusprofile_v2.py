from __future__ import annotations

import re
import time
import random
from pathlib import Path
from typing import List, Optional
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
        return urlparse(site).netloc.lower()
    except Exception:
        return ""

def http_get(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=25, allow_redirects=True)
    r.raise_for_status()
    return r.text

def rusprofile_search_cards(query: str, max_cards: int = 10) -> List[str]:
    q = quote_plus(query)
    url = f"https://www.rusprofile.ru/search?query={q}"
    html = http_get(url)
    soup = BeautifulSoup(html, "lxml")

    cards: List[str] = []
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if not href:
            continue
        if href.startswith("/id/") or "/company/" in href:
            full = href if href.startswith("http") else "https://www.rusprofile.ru" + href
            if full not in cards:
                cards.append(full)
        if len(cards) >= max_cards:
            break
    return cards

def extract_inn_from_card(html: str) -> Optional[str]:
    m = INN_RE.search(html)
    if m:
        return m.group(1)
    return None

def choose_best_card_and_inn(name: str, site: str) -> tuple[str, str]:
    """
    Возвращает (inn, card_url) или ("","") если не нашли.
    Логика:
    - если есть домен, ищем карточку по домену, и выбираем ту, где домен встречается на странице
    - если домена нет или не совпало — ищем по названию (и берём первую с найденным ИНН)
    """
    dom = domain_from_site(site)

    # 1) По домену (точнее)
    if dom:
        try:
            for card_url in rusprofile_search_cards(dom, max_cards=10):
                time.sleep(0.6 + random.random() * 0.4)
                card_html = http_get(card_url)

                # проверка: домен должен встречаться в карточке
                if dom in card_html.lower():
                    inn = extract_inn_from_card(card_html)
                    if inn:
                        return inn, card_url
        except Exception:
            pass

    # 2) По названию (запасной вариант)
    if name:
        try:
            for card_url in rusprofile_search_cards(name, max_cards=10):
                time.sleep(0.6 + random.random() * 0.4)
                card_html = http_get(card_url)
                inn = extract_inn_from_card(card_html)
                if inn:
                    return inn, card_url
        except Exception:
            pass

    return "", ""

def main() -> None:
    inp = DATA_DIR / "companies_stage2.csv"
    out = DATA_DIR / "companies_stage4_v2.csv"

    df = pd.read_csv(inp, dtype=str, keep_default_na=False)

    inns = []
    inn_sources = []
    rusprofile_urls = []

    for _, r in df.iterrows():
        name = (r.get("name") or "").strip()
        site = (r.get("site") or "").strip()

        inn, card_url = choose_best_card_and_inn(name, site)

        inns.append(inn)
        rusprofile_urls.append(card_url)
        inn_sources.append("rusprofile" if inn else "")

        time.sleep(0.6 + random.random() * 0.4)

    df["inn"] = inns
    df["inn_source"] = inn_sources
    df["rusprofile_url"] = rusprofile_urls

    df.to_csv(out, index=False, encoding="utf-8")
    print(f"Saved: {len(df)} rows -> {out}")
    print("INN filled:", int((df["inn"].astype(str) != "").sum()))

if __name__ == "__main__":
    main()