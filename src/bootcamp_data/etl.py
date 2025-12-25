from __future__ import annotations

from dataclasses import dataclass, asdict
from pathlib import Path
import json
import logging

import pandas as pd
#___________________________import function_________________________________
from bootcamp_data.io import read_orders_csv, read_users_csv, write_parquet
from bootcamp_data.transforms import (
    enforce_schema,
    require_columns,
    assert_non_empty,
    assert_unique_key,
    add_missing_flags,
    normalize_text,
    apply_mapping,
    parse_datetime,
    add_time_parts,
    winsorize,
)
#____________________________________________________________________________
log = logging.getLogger(__name__)


@dataclass(frozen=True)
class ETLConfig:
    root: Path
    raw_orders: Path
    raw_users: Path

    out_orders_clean: Path
    out_users: Path
    out_analytics: Path
    run_meta: Path

    winsor_lower_q: float = 0.01
    winsor_upper_q: float = 0.99


def load_inputs(cfg: ETLConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    
    orders = read_orders_csv(cfg.raw_orders)
    users = read_users_csv(cfg.raw_users)
    return orders, users


def transform(
    orders_raw: pd.DataFrame,
    users_raw: pd.DataFrame,
    cfg: ETLConfig,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Transform: clean + feature engineering + join.
    Returns:
      analytics (orders joined with users),
      users_clean
    """


    orders_raw = orders_raw.copy()
    users_raw = users_raw.copy()
    orders_raw.columns = [c.strip().lower() for c in orders_raw.columns]
    users_raw.columns = [c.strip().lower() for c in users_raw.columns]

  
    require_columns(orders_raw, ["order_id", "user_id", "amount", "quantity", "created_at", "status"])
    require_columns(users_raw, ["user_id", "country", "signup_date"])

    assert_non_empty(orders_raw, "orders_raw")
    assert_non_empty(users_raw, "users_raw")

    assert_unique_key(orders_raw, key="order_id")
    assert_unique_key(users_raw, key="user_id")


    orders = enforce_schema(orders_raw)

    
    orders = add_missing_flags(orders, cols=["amount", "quantity"])

    orders = parse_datetime(orders, col="created_at", utc=True)

    
    status_norm = normalize_text(orders_raw["status"])
    status_clean = apply_mapping(
        status_norm,
        mapping={
            "paid": "paid",
            "refund": "refund",
        },
    )
    orders["status_clean"] = status_clean

    
    orders = add_time_parts(orders, ts_col="created_at")

    
    orders["amount_winsor"] = winsorize(orders["amount"], lo=cfg.winsor_lower_q, hi=cfg.winsor_upper_q)

  
    users = users_raw.copy()
    users["user_id"] = users["user_id"].astype("string")
    users["country"] = normalize_text(users["country"])
    users = parse_datetime(users, col="signup_date", utc=False)

    
    analytics = orders.merge(users, on="user_id", how="left", suffixes=("", "_user"))

    return analytics, users


def load_outputs(*, analytics: pd.DataFrame, users: pd.DataFrame, cfg: ETLConfig) -> None:
    
    cfg.out_analytics.parent.mkdir(parents=True, exist_ok=True)
    cfg.out_users.parent.mkdir(parents=True, exist_ok=True)
    cfg.out_orders_clean.parent.mkdir(parents=True, exist_ok=True)

    write_parquet(users, cfg.out_users)
    write_parquet(analytics, cfg.out_analytics)

    user_side_cols = [c for c in users.columns if c != "user_id"]
    orders_clean = analytics.drop(columns=[c for c in user_side_cols if c in analytics.columns], errors="ignore")
    write_parquet(orders_clean, cfg.out_orders_clean)


def write_run_meta(
    cfg: ETLConfig,
    *,
    orders_raw: pd.DataFrame,
    users: pd.DataFrame,
    analytics: pd.DataFrame,
) -> None:
    missing_created_at = int(analytics["created_at"].isna().sum()) if "created_at" in analytics.columns else None
    country_match_rate = (1.0 - float(analytics["country"].isna().mean())) if "country" in analytics.columns else None

    meta = {
        "rows_in_orders_raw": int(len(orders_raw)),
        "rows_in_users": int(len(users)),
        "rows_out_analytics": int(len(analytics)),
        "missing_created_at": missing_created_at,
        "country_match_rate": country_match_rate,
        "columns_out": list(analytics.columns),
        "config": {k: str(v) for k, v in asdict(cfg).items()},
    }

    cfg.run_meta.parent.mkdir(parents=True, exist_ok=True)
    cfg.run_meta.write_text(json.dumps(meta, indent=2), encoding="utf-8")


def run_etl(cfg: ETLConfig) -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    log.info("Loading inputs")
    orders_raw, users_raw = load_inputs(cfg)

    log.info("Transforming (orders=%s, users=%s)", len(orders_raw), len(users_raw))
    analytics, users_clean = transform(orders_raw, users_raw, cfg)

    log.info("Writing outputs to %s", cfg.out_analytics.parent)
    load_outputs(analytics=analytics, users=users_clean, cfg=cfg)

    log.info("Writing run metadata: %s", cfg.run_meta)
    write_run_meta(cfg, orders_raw=orders_raw, users=users_clean, analytics=analytics)

    log.info("Done")
