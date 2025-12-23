from __future__ import annotations

import re
import time
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

CONTACT_EMAIL = "ojdupool2004@mail.ru"

HEADERS = {
    "User-Agent": f"LeadSniperTest/1.0 (contact: {CONTACT_EMAIL})",
    "Accept": "text/html,application/xhtml+xml",
}

TIMEOUT = 25

TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")
TAXID_JSON_RE = re.compile(r'"taxID"\s*:\s*"(\d{10}|\d{12})"', re.IGNORECASE)
INN_RE = re.compile(r"(ИНН|INN)\D{0,40}(\d{10}|\d{12})", re.IGNORECASE)


EMAIL_RE = re.compile(r"([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})", re.IGNORECASE)

# признаки
MENTIONS_24_7_RE = re.compile(r"\b24\s*/\s*7\b|круглосуточ|24\s*час", re.IGNORECASE)
FORM_RE = re.compile(r"<form\b", re.IGNORECASE)

# мессенджеры
MESSENGERS_RE = re.compile(r"(t\.me/|telegram\.me/|wa\.me/|api\.whatsapp\.com/|viber\.com/)", re.IGNORECASE)

# чат-вендоры (по скриптам/ключевым словам)
CHAT_VENDORS = {
    "jivo": re.compile(r"jivo|jivosite", re.IGNORECASE),
    "livetex": re.compile(r"livetex", re.IGNORECASE),
    "intercom": re.compile(r"intercom", re.IGNORECASE),
    "zendesk": re.compile(r"zendesk|zopim", re.IGNORECASE),
    "freshchat": re.compile(r"freshchat|freshworks", re.IGNORECASE),
    "chatra": re.compile(r"chatra", re.IGNORECASE),
    "yandex_chat": re.compile(r"yandex\.ru/chat|яндекс\.чат", re.IGNORECASE),
    "bitrix": re.compile(r"bitrix|битрикс", re.IGNORECASE),
}

SUPPORT_HINT_RE = re.compile(r"(поддержк|support|help|контакт|обратн(ая|ой)\s+связ)", re.IGNORECASE)
KB_HINT_RE = re.compile(r"(faq|база знаний|knowledge\s*base|помощ(ь|и)|инструкц|стат(ья|ьи))", re.IGNORECASE)

# какие страницы пробуем дополнительно
PATH_CANDIDATES = [
    "",  # главная
    "/contacts", "/contact", "/kontakty", "/kontakti",
    "/support", "/help", "/faq", "/kb", "/knowledge", "/hc",
    "/about", "/company", "/o-kompanii",
    "/rekvizity", "/requisites", "/legal",
    "/privacy", "/terms",
]

@dataclass
class SiteFeatures:
    inn: str = ""
    has_support_email: int = 0
    has_contact_form: int = 0
    has_online_chat: int = 0
    has_messengers: int = 0
    has_support_section: int = 0
    has_kb_or_faq: int = 0
    mentions_24_7: int = 0
    support_email: str = ""
    support_url: str = ""
    kb_url: str = ""
    chat_vendor: str = ""

def norm_text(html: str) -> str:
    text = TAG_RE.sub(" ", html)
    text = WS_RE.sub(" ", text).strip()
    return text

def safe_get(url: str) -> Optional[str]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
        r.raise_for_status()
        # иногда сайты отдают PDF/JSON — нас интересует только html
        ctype = (r.headers.get("content-type") or "").lower()
        if "text/html" not in ctype and "application/xhtml" not in ctype:
            return r.text  # пусть будет, иногда без content-type
        return r.text
    except Exception:
        return None

def same_domain(base: str, link: str) -> bool:
    try:
        b = urlparse(base)
        l = urlparse(link)
        if not l.netloc:
            return True
        return l.netloc == b.netloc
    except Exception:
        return False

def extract_links(html: str, base_url: str) -> List[str]:
    # грубо ищем href
    hrefs = re.findall(r'href=["\']([^"\']+)["\']', html, flags=re.IGNORECASE)
    links = []
    for h in hrefs:
        h = h.strip()
        if not h or h.startswith("#") or h.startswith("mailto:") or h.startswith("tel:"):
            continue
        full = urljoin(base_url, h)
        if same_domain(base_url, full):
            links.append(full)
    # уникальные, порядок сохраняем
    seen = set()
    out = []
    for u in links:
        if u not in seen:
            out.append(u); seen.add(u)
    return out

def pick_best_support_links(links: List[str]) -> Tuple[str, str]:
    support_url = ""
    kb_url = ""
    for u in links:
        low = u.lower()
        if not support_url and any(k in low for k in ["/support", "/help", "/contacts", "/contact", "/kontakty"]):
            support_url = u
        if not kb_url and any(k in low for k in ["/faq", "/kb", "/knowledge", "/hc", "help", "support"]):
            kb_url = u
    return support_url, kb_url

def enrich_one_site(site: str) -> SiteFeatures:
    f = SiteFeatures()
    if not site:
        return f

    if not site.startswith("http"):
        site = "https://" + site

    visited = set()
    pages_html: List[Tuple[str, str]] = []

    # 1) сначала типовые страницы
    for p in PATH_CANDIDATES:
        url = urljoin(site.rstrip("/") + "/", p.lstrip("/"))
        if url in visited:
            continue
        visited.add(url)

        html = safe_get(url)
        time.sleep(0.35 + random.random() * 0.25)
        if not html:
            continue

        pages_html.append((url, html))
        if len(pages_html) >= 6:
            break

    # 2) умный добор ссылок с уже скачанных страниц (depth=1)
    # выбираем ссылки, похожие на "реквизиты/о компании/контакты/правовая"
    keyword_links: List[str] = []
    for url, html in pages_html:
        for link in extract_links(html, url):
            low = link.lower()
            if any(k in low for k in [
                "rekviz", "requis", "legal", "privacy", "terms",
                "about", "company", "contacts", "contact", "kontakty",
                "info", "disclosure", "sveden"
            ]):
                keyword_links.append(link)

    # уникализируем
    seen = set()
    keyword_links_uniq = []
    for u in keyword_links:
        if u not in seen:
            keyword_links_uniq.append(u)
            seen.add(u)

    # докачаем ещё страниц (до 12 суммарно)
    for url in keyword_links_uniq:
        if len(pages_html) >= 12:
            break
        if url in visited:
            continue
        visited.add(url)

        html = safe_get(url)
        time.sleep(0.35 + random.random() * 0.25)
        if not html:
            continue
        pages_html.append((url, html))

    # 3) анализируем собранные страницы
    all_links: List[str] = []
    for url, html in pages_html:
        text = norm_text(html)

        # ИНН: сначала JSON-LD (очень часто так)
        if not f.inn:
            mj = TAXID_JSON_RE.search(html)
            if mj:
                f.inn = mj.group(1)

        # ИНН: потом обычный текст
        if not f.inn:
            m = INN_RE.search(text)
            if m:
                f.inn = m.group(2)

        # email
        emails = EMAIL_RE.findall(text)
        emails = [e.lower() for e in emails]
        support_emails = [e for e in emails if e.startswith(("support@", "help@", "client@", "service@", "info@")) or "support" in e or "help" in e]
        if support_emails and not f.support_email:
            f.support_email = support_emails[0]
            f.has_support_email = 1

        # форма
        if not f.has_contact_form and FORM_RE.search(html):
            f.has_contact_form = 1

        # 24/7
        if not f.mentions_24_7 and MENTIONS_24_7_RE.search(text):
            f.mentions_24_7 = 1

        # мессенджеры
        if not f.has_messengers and MESSENGERS_RE.search(html):
            f.has_messengers = 1

        # чат-вендор
        if not f.has_online_chat:
            for vendor, rx in CHAT_VENDORS.items():
                if rx.search(html):
                    f.has_online_chat = 1
                    f.chat_vendor = vendor
                    break

        all_links.extend(extract_links(html, url))

        if not f.has_support_section and SUPPORT_HINT_RE.search(text):
            f.has_support_section = 1
        if not f.has_kb_or_faq and KB_HINT_RE.search(text):
            f.has_kb_or_faq = 1

    if all_links:
        support_url, kb_url = pick_best_support_links(all_links)
        if support_url and not f.support_url:
            f.support_url = support_url
        if kb_url and not f.kb_url:
            f.kb_url = kb_url

    return f

def main() -> None:
    inp = DATA_DIR / "companies_stage2.csv"
    out = DATA_DIR / "companies_stage3.csv"

    df = pd.read_csv(inp, dtype=str, keep_default_na=False)

    inns = []
    has_support_email = []
    has_contact_form = []
    has_online_chat = []
    has_messengers = []
    has_support_section = []
    has_kb_or_faq = []
    mentions_24_7 = []
    support_email = []
    support_url = []
    kb_url = []
    chat_vendor = []

    for _, r in df.iterrows():
        site = (r.get("site") or "").strip()
        f = enrich_one_site(site)

        inns.append(f.inn)
        has_support_email.append(f.has_support_email)
        has_contact_form.append(f.has_contact_form)
        has_online_chat.append(f.has_online_chat)
        has_messengers.append(f.has_messengers)
        has_support_section.append(f.has_support_section)
        has_kb_or_faq.append(f.has_kb_or_faq)
        mentions_24_7.append(f.mentions_24_7)
        support_email.append(f.support_email)
        support_url.append(f.support_url)
        kb_url.append(f.kb_url)
        chat_vendor.append(f.chat_vendor)

    df["inn"] = inns
    df["has_support_email"] = has_support_email
    df["has_contact_form"] = has_contact_form
    df["has_online_chat"] = has_online_chat
    df["has_messengers"] = has_messengers
    df["has_support_section"] = has_support_section
    df["has_kb_or_faq"] = has_kb_or_faq
    df["mentions_24_7"] = mentions_24_7

    df["support_email"] = support_email
    df["support_url"] = support_url
    df["kb_url"] = kb_url
    df["chat_vendor"] = chat_vendor

    df.to_csv(out, index=False, encoding="utf-8")
    print(f"Saved: {len(df)} rows -> {out}")
    print("INN filled:", int((df["inn"].astype(str) != "").sum()))

if __name__ == "__main__":
    main()