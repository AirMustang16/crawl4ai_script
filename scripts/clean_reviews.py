import os
import re
import json
import orjson
import pandas as pd
from dateutil import parser as dateparser
from rapidfuzz import fuzz

RAW_DIR = "/Users/omar/projects/crawl4ai_script/data/raw"
CLEAN_DIR = "/Users/omar/projects/crawl4ai_script/data/clean"
os.makedirs(CLEAN_DIR, exist_ok=True)

def iter_jsonl(path: str):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                try:
                    yield orjson.loads(line)
                except Exception:
                    continue

def norm_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

# Patterns to remove from text (case-insensitive)
_G2_NOISE_PATTERNS = [
    r"Review collected by and hosted on G2\.com\.?",
    r"\bShow More\b|\bRead More\b|\bRead Less\b",
    r"What do you like best about [^\?]+\?",
    r"What do you dislike about [^\?]+\?",
    r"What problems is [^\?]+ solving and how is that benefiting you\?",
    r"Reasons for choosing [^\n]+",
    r"Reasons for switching to [^\n]+",
]
_G2_NOISE_RE = re.compile("|".join(_G2_NOISE_PATTERNS), flags=re.IGNORECASE)

_DEF_NOISE_RE = re.compile(r"(?i)\b(read more|show more|see more|translated by.*)\b")

def strip_noise(s: str) -> str:
    if not s:
        return s
    # Remove generic noise
    s = _DEF_NOISE_RE.sub("", s)
    # Remove G2-specific boilerplate/questions
    s = _G2_NOISE_RE.sub("", s)
    return norm_ws(s)

def parse_date(s):
    if not s:
        return None
    try:
        return dateparser.parse(s).date().isoformat()
    except Exception:
        return None

def normalize_rating(raw_text, existing_float):
    if existing_float is not None:
        try:
            v = float(existing_float)
            if v > 5 and v <= 10:
                v = v / 2.0
            return max(0.0, min(5.0, v))
        except Exception:
            pass
    txt = str(raw_text or "")
    m = re.search(r"([0-9](?:\.[0-9])?)\s*(?:/|out of)?\s*5", txt, flags=re.I)
    if m:
        try:
            v = float(m.group(1))
            return max(0.0, min(5.0, v))
        except Exception:
            return None
    m2 = re.search(r"([0-9](?:\.[0-9])?)\s*star", txt, flags=re.I)
    if m2:
        try:
            v = float(m2.group(1))
            return max(0.0, min(5.0, v))
        except Exception:
            return None
    return None

def dedupe_rows(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset=["author", "date", "title", "body"], keep="first")
    to_drop = set()
    texts = df["body"].fillna("").tolist()
    for i in range(len(texts)):
        if i in to_drop:
            continue
        for j in range(i + 1, len(texts)):
            if j in to_drop:
                continue
            if not texts[i] or not texts[j]:
                continue
            if fuzz.token_set_ratio(texts[i], texts[j]) >= 95:
                to_drop.add(j)
    if to_drop:
        df = df.drop(df.index[list(to_drop)])
    return df

def clean_file(path: str, out_prefix: str):
    rows = list(iter_jsonl(path))
    if not rows:
        print(f"[WARN] empty {path}")
        return
    df = pd.DataFrame(rows)

    for col in ["title", "body", "author", "pros", "cons"]:
        if col in df.columns:
            df[col] = df[col].fillna("").map(strip_noise)
            df[col] = df[col].replace("", pd.NA)

    if "rating" not in df.columns:
        df["rating"] = pd.NA
    if "rating_raw" not in df.columns:
        df["rating_raw"] = pd.NA
    df["rating"] = [normalize_rating(rtxt, rv) for rtxt, rv in zip(df["rating_raw"], df["rating"])]

    if "date" in df.columns:
        df["date"] = df["date"].map(parse_date)

    if "body" in df.columns:
        df = df[df["body"].fillna("").str.len() >= 40]

    for col in ["author", "date", "title", "body", "rating", "source_url", "source_domain"]:
        if col not in df.columns:
            df[col] = pd.NA

    df = dedupe_rows(df)

    if "date" in df.columns:
        df = df.sort_values(by=["date"], ascending=True, na_position="last")

    out_jsonl = os.path.join(CLEAN_DIR, f"{out_prefix}_clean.jsonl")
    out_csv = os.path.join(CLEAN_DIR, f"{out_prefix}_clean.csv")

    with open(out_jsonl, "w", encoding="utf-8") as f:
        for _, row in df.iterrows():
            f.write(orjson.dumps({k: v for k, v in row.dropna().to_dict().items()}).decode() + "\n")

    df.to_csv(out_csv, index=False)
    print(f"[OK] wrote {out_jsonl} and {out_csv} | rows={len(df)}")


def main():
    for fname in os.listdir(RAW_DIR):
        if not fname.endswith(".jsonl"):
            continue
        in_path = os.path.join(RAW_DIR, fname)
        prefix = os.path.splitext(fname)[0]
        clean_file(in_path, prefix)

if __name__ == "__main__":
    main()
