import hashlib
import re
from typing import List

import pandas as pd


def stable_hash(text: str, length: int = 12) -> str:
    return hashlib.md5(text.strip().upper().encode("utf-8")).hexdigest()[:length]


def safe_int(value, default: int = 0) -> int:
    try:
        if pd.isna(value):
            return default
    except (TypeError, ValueError):
        pass
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def safe_str(value, default: str = "") -> str:
    try:
        if pd.isna(value):
            return default
    except (TypeError, ValueError):
        pass
    return str(value).strip()


def escape_oml(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace('"', '\\"')


def oml_safe_id(raw: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]", "_", str(raw).strip())


def strip_str_cols(df: pd.DataFrame) -> pd.DataFrame:
    try:
        str_cols = df.select_dtypes(include=["object", "string"]).columns
    except Exception:
        str_cols = df.select_dtypes(include=["object"]).columns
    for col in str_cols:
        try:
            df[col] = df[col].str.strip()
        except AttributeError:
            pass
    return df


def to_int64(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df
