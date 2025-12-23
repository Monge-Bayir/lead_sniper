import time
import random
from typing import Dict, Any, List
from tqdm import tqdm

import pandas as pd
import requests

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent   # корень проекта (analytics_vacancies)
DATA_RAW = BASE_DIR / "data" / "raw"
DATA_RAW.mkdir(parents=True, exist_ok=True)


HH_API = 'https://api.hh.ru'
QUERIES = [
    'служба поддержки',
    'helpdesk',
    'контакт-центр',
    'техподдержка',
    'customer support'
]


def get_hh(url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    #говорит hh.ru что мы не робот, а пользователь
    headers = {
        "HH-User-Agent": "LeadSniperTest/1.0 (contact: ojdupool2004@mail.ru)",
        "User-Agent": "LeadSniperTest/1.0 (contact: ojdupool2004@mail.ru)",
    }

    r = requests.get(url, params=params, headers=headers, timeout=30)
    '''
    Если будет ошибка, то заранее выкидываем его
    •	429 → слишком много запросов
	•	403 → проблемы с User-Agent
	•	500 → ошибка на стороне HH
    '''
    r.raise_for_status()


    return r.json()


def collect_vacancies(query: str, max_page: int = 5, per_page: int = 100) -> List[Dict[str, Any]]:
    '''
    Берёт одну фразу поиска (например "техподдержка") и скачивает по ней много вакансий,
    листая страницы, пока не соберёт достаточно или пока вакансии не закончатся.
    '''

    items = []

    for page in range(max_page):
        data = get_hh(f'{HH_API}/vacancies/',
                         params={
                             'text': query,
                             'area': 113, #Россия
                             'per_page': per_page, #максимум вакансий на странице
                             'page': page,
                             'order_by': 'publication_time'
                         }
                         )

        page_items = data.get('items', [])

        if not page_items:
            break

        items.extend(page_items)

        time.sleep(0.4 + random.random() * 0.4) #делаем паузу, чтобы не выглядеть как бот

    return items


def main() -> None:
    import os
    print("CWD:", os.getcwd())
    rows = []

    for query in tqdm(QUERIES, desc='Queries'):
        vacancies = collect_vacancies(query, max_page=5, per_page=100)
        for v in vacancies:
            emp = v.get("employer") or {}
            if not emp:
                continue

            rows.append({
                "employer_id": emp.get("id"),
                "employer_name": emp.get("name"),
                "employer_url": emp.get("alternate_url"),
                "vacancy_id": v.get("id"),
                "vacancy_name": v.get("name"),
                "vacancy_url": v.get("alternate_url"),
                "query": query,
            })

        df = pd.DataFrame(rows).dropna(subset=["employer_id"])
        # уникальные работодатели
        employers = (
            df[["employer_id", "employer_name", "employer_url"]]
            .drop_duplicates(subset=["employer_id"])
            .sort_values("employer_name")
            .reset_index(drop=True)
        )

        employers.to_csv(DATA_RAW / "employers_seeds.csv", index=False, encoding="utf-8")
        df.to_csv(DATA_RAW / "vacancies_seeds.csv", index=False, encoding="utf-8")

        print(f"Saved employers: {len(employers)} -> {DATA_RAW / 'employers_seeds.csv'}")
        print(f"Saved vacancies:  {len(df)} -> {DATA_RAW / 'vacancies_seeds.csv'}")


if __name__ == '__main__':
    main()