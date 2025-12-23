import re
from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_RAW = BASE_DIR / "data" / "raw"
DATA_RAW.mkdir(parents=True, exist_ok=True)

POS = re.compile(
    r"(поддержк|call[\s-]?центр|контакт[\s-]?центр|оператор|helpdesk|support|"
    r"техподдержк|чат|обращен|тикет|SLA|линия\s*1|линия\s*2|L1|L2)",
    re.IGNORECASE,
)

NEG = re.compile(
    r"(продавец|кассир|мерчендайзер|директор\s+магазина|управляющ|врач|"
    r"кредитн(ый|ая)\s+аналитик|менеджер\s+по\s+продажам)",
    re.IGNORECASE,
)

TAG_RE = re.compile(r"<[^>]+>")

def strip_html(x: str) -> str:
    x = TAG_RE.sub(" ", x)
    x = re.sub(r"\s+", " ", x).strip()
    return x

def main() -> None:
    inp = DATA_RAW / "vacancies_details.csv"
    out = DATA_RAW / "vacancies_support_only.csv"

    df = pd.read_csv(inp, dtype=str, keep_default_na=False)

    # готовим текст для фильтра
    df["name_norm"] = df["name"].fillna("").astype(str)
    df["desc_norm"] = df["description"].fillna("").astype(str).map(strip_html)

    # условия
    is_pos = df["name_norm"].str.contains(POS) | df["desc_norm"].str.contains(POS)
    is_neg = df["name_norm"].str.contains(NEG) | df["desc_norm"].str.contains(NEG)

    filtered = df[is_pos & (~is_neg)].copy()

    # немного метрик для контроля
    print("Total details:", len(df))
    print("Kept (support-like):", len(filtered))

    filtered.to_csv(out, index=False, encoding="utf-8")
    print(f"Saved -> {out}")

if __name__ == "__main__":
    main()