import pandas as pd
import re


def enforce_schema(df: pd.DataFrame) -> pd.DataFrame: 
     return df.assign(
 order_id=df["order_id"].astype("string"), 
 user_id=df["user_id"].astype("string"),
 amount=pd.to_numeric(df["amount"], errors="coerce").astype("Float64"), 
 quantity=pd.to_numeric(df["quantity"], errors="coerce").astype("Int64"), 
   )



def require_columns(df: pd.DataFrame, cols: list[str]) -> None:
    missing = [c for c in cols if c not in df.columns]
    assert not missing, f"Missing columns: {missing}"

def assert_non_empty(df: pd.DataFrame, name: str = "df") -> None:
    assert len(df) > 0, f"{name} has 0 rows"

def assert_unique_key(df: pd.DataFrame, key: str, *, allow_na: bool = False) -> None:
    if not allow_na:
        assert df[key].notna().all(), f"{key} contains NA"
    dup = df[key].duplicated(keep=False) & df[key].notna()
    assert not dup.any(), f"{key} not unique; {dup.sum()} duplicate rows"

def assert_in_range(s: pd.Series, lo=None, hi=None, name: str = "value") -> None:
    x = s.dropna()
    if lo is not None:
        assert (x >= lo).all(), f"{name} below {lo}"
    if hi is not None:
        assert (x <= hi).all(), f"{name} above {hi}"



def missingness_report(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.isna().sum()
          .rename("n_missing")
          .to_frame()
          .assign(p_missing=lambda t: t["n_missing"] / len(df))
          .sort_values("p_missing", ascending=False)
    )

def add_missing_flags(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        out[f"{c}_isna"] = out[c].isna()
    return out


_ws = re.compile(r"\s+")

def normalize_text(s: pd.Series) -> pd.Series:
    return (
        s.astype("string")
         .str.strip()
         .str.casefold()
         .str.replace(_ws, " ", regex=True)
    )

def apply_mapping(s: pd.Series, mapping: dict[str, str]) -> pd.Series:
    return s.map(lambda x: mapping.get(x, x))

def dedupe_keep_latest(df: pd.DataFrame, key_cols: list[str], ts_col: str) -> pd.DataFrame:
    return (
        df.sort_values(ts_col)
          .drop_duplicates(subset=key_cols, keep="last")
          .reset_index(drop=True)
    )
def parse_datetime(
        df : pd.DataFrame,
        col : str,
        *,
        utc : bool = True ,) -> pd.DataFrame: 
        dt = pd.to_datetime(df[col], errors="coerce", utc=utc)
        return df.assign(**{col : dt})

def add_time_parts(df: pd.DataFrame, ts_col: str) -> pd.DataFrame:
    ts = df[ts_col]
    return df.assign(
        date=ts.dt.date,
        year=ts.dt.year,
        month=ts.dt.to_period("M").astype("string"),
        dow=ts.dt.day_name(),
        hour=ts.dt.hour,
    )

def iqr_bounds(s, k=1.5):
    q1 = s.quantile(0.25)
    q3 = s.quantile(0.75)
    iqr = q3 - q1
    return float(q1 - k*iqr), float(q3 + k*iqr)

def winsorize(s, lo=0.01, hi=0.99):
    a, b = s.quantile(lo), s.quantile(hi)
    return s.clip(lower=a, upper=b)


