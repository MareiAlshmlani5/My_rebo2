import pandas as pd
from pathlib import Path
import sys
# data = {

#     'City': ['New York', 'Los Angeles', 'Chicago', 'New York', 'Chicago', 'New York'],
#         'Age': [30, 45, 25, 30, 25, 30]
        
#         }
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from bootcamp_data.config import make_paths

p = make_paths(ROOT)

df = pd.read_parquet(p.processed /"orders.parquet")




def assert_unique_key(df,key,allow_na = False):
    if not allow_na:
        assert df[key].notna().all(), f"{key} contins NA"
    dup = df[key].duplicated(keep = False) & df[key].notna()
    assert not dup.any(), f"{key} not unique ; {dup.sum()} duplicate rows"

print(assert_unique_key(df,'order_id'))

def missingness_report(df):
    n = len(df)
    return(
        df.isna().sum().rename("n_missing").to_frame().assign(p_missing=lambda t:t["n_missing"]/n).sort_values("p_missing", ascending = False)

    )
# print(missingness_report(df))



def add_missing_flags(df , cols):
    out = df.copy
    for c in cols:
     out[f"{c}= _isna"] = out[c].isna()
     return out
    
    

