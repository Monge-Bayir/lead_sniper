from __future__ import annotations

import time
import random
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from tqdm import tqdm

HH_API = "https://api.hh.ru"

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_RAW = BASE_DIR / "data" / "raw"
DATA_RAW.mkdir(parents=True, exist_ok=True)

CONTACT_EMAIL = "ojdupool2004@mail.ru"

def get_hh(url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    headers = {
        "HH-User-Agent": f"LeadSniperTest/1.0 (contact: {CONTACT_EMAIL})",
        "User-Agent": f"LeadSniperTest/1.0 (contact: {CONTACT_EMAIL})",
        "Accept": "application/json",
    }
    r = requests.get(url, params=params, headers=headers, timeout=30)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, dict):
        raise ValueError(f"Unexpected JSON type: {type(data).__name__}")
    return data

def normalize_vacancy_id(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    if not s or s.lower() == "nan":
        return None
    if s.endswith(".0"):
        s = s[:-2]
    s = s.strip()
    return s if s.isdigit() else None

def fetch_vacancy_detail(vacancy_id: str) -> Dict[str, Any]:
    return get_hh(f"{HH_API}/vacancies/{vacancy_id}")

def main() -> None:
    seeds_path = DATA_RAW / "vacancies_seeds.csv"
    out_path = DATA_RAW / "vacancies_details.csv"

    seeds = pd.read_csv(seeds_path, dtype=str, keep_default_na=False)
    seeds["vacancy_id_norm"] = seeds["vacancy_id"].apply(normalize_vacancy_id)
    seeds = seeds.dropna(subset=["vacancy_id_norm"]).copy()

    # если файл деталей уже есть — не качаем повторно
    if out_path.exists():
        existing = pd.read_csv(out_path, dtype=str, keep_default_na=False)
        if "vacancy_id" in existing.columns:
            done = set(existing["vacancy_id"].astype(str).tolist())
        else:
            done = set()
    else:
        existing = pd.DataFrame()
        done = set()

    todo = [vid for vid in seeds["vacancy_id_norm"].tolist() if vid not in done]
    print("Already have:", len(done))
    print("To fetch:", len(todo))

    rows: List[Dict[str, Any]] = []
    for vid in tqdm(todo, desc="Fetch vacancy details"):
        try:
            v = fetch_vacancy_detail(vid)
            rows.append({
                "vacancy_id": v.get("id"),
                "name": v.get("name"),
                "employer_id": (v.get("employer") or {}).get("id"),
                "published_at": v.get("published_at"),
                "description": v.get("description"),
                "schedule": (v.get("schedule") or {}).get("name"),
                "employment": (v.get("employment") or {}).get("name"),
                "alternate_url": v.get("alternate_url"),
            })
            time.sleep(0.25 + random.random() * 0.25)
        except requests.HTTPError as e:
            rows.append({"vacancy_id": vid, "error": str(e), "status": getattr(e.response, "status_code", None)})
            time.sleep(0.6)
        except Exception as e:
            rows.append({"vacancy_id": vid, "error": repr(e)})
            time.sleep(0.6)

    new_df = pd.DataFrame(rows)

    if not existing.empty:
        out = pd.concat([existing, new_df], ignore_index=True)
    else:
        out = new_df

    out.to_csv(out_path, index=False, encoding="utf-8")
    errors = int(out["error"].notna().sum()) if "error" in out.columns else 0
    print(f"Saved total: {len(out)} -> {out_path}")
    print("Total errors:", errors)

if __name__ == "__main__":
    main()