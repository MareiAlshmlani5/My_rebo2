import pandas as pd
from pathlib import Path
import sys
import logging

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
REPORTS = ROOT / "reports"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
#___________________________import function__________________________
from bootcamp_data.transforms import require_columns

from bootcamp_data.transforms import assert_non_empty

from bootcamp_data.transforms import enforce_schema

from bootcamp_data.transforms import missingness_report

from bootcamp_data.transforms import add_missing_flags

from bootcamp_data.transforms import normalize_text

from bootcamp_data.io import write_parquet

from bootcamp_data.config import make_paths

from bootcamp_data.quality import  assert_in_range
#_______________________




p = make_paths(ROOT)

log = logging.getLogger(__name__) 
def main() -> None: 
    
   

    orders = pd.read_parquet(p.processed /"orders.parquet")
    users = pd.read_parquet(p.processed /"users.parquet")

    add_missing_flags(orders, ["amount"])
    add_missing_flags(orders, ["quantity"])

    orders_list = ["order_id","user_id","amount","quantity","created_at"]
    users_list = ["user_id","country" ]
    require_columns(orders ,orders_list )
    require_columns(users ,users_list )

    assert_non_empty(orders)
    assert_non_empty(users)
    users.columns = users.columns.str.strip()
    orders.columns = orders.columns.str.strip()
    print(users.columns.tolist())

    enforce_schema(orders)



    status_norm = normalize_text(orders["status"])

    mapping = {
        "paid": "paid",
        "refund": "refund",
        "refunded": "refund",
    }
    def apply_mapping(s, mapping): 
        return s.map(lambda x: mapping.get(x, x))
    
    status_clean = apply_mapping(status_norm, mapping)

    orders_clean = (
        orders
        .assign(status_clean=status_clean)
        .pipe(add_missing_flags, cols=["amount", "quantity"])
    )

    orders = orders.assign(status_norm=normalize_text(orders["status"]))
    
    orders = orders.assign (status_clean=apply_mapping(orders["status_norm"], mapping),)

    print(orders["status_clean"].unique())

    

    write_parquet(orders_clean, p.processed / "orders_clean.parquet")
    write_parquet(users, p.processed / "users.parquet")

    log.info("Wrote processed outputs: %s", p.processed)



    #_____________________________________________  here we save our reports as parquet in reports file   _______________________________
    report_orders = missingness_report(orders)
    report_users = missingness_report(users)
    report_orders.to_parquet(REPORTS / "orders_missingness.parquet", index=True)
    report_users.to_parquet(REPORTS / "users_missingness.parquet", index=True)

    assert_in_range(orders_clean["amount"], lo=0, name="amount")
    assert_in_range(orders_clean["quantity"], lo=0, name="quantity")
if __name__ == "__main__":  main()
