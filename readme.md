## Project structure

week2-data-work/
├── data/
│ ├── raw/
│ │ ├── orders.csv
│ │ └── users.csv
│ └── processed/
│ ├── orders_clean.parquet
│ ├── users.parquet
│ ├── analytics_table.parquet
│ └── \_run_meta.json
├── notebooks/
│ └── eda.ipynb
├── reports/
│ ├── figures/
│ │ └── \*.png
│ └── summary.md
├── scripts/
│ └── run_etl.py
└── src/
└── bootcamp_data/
├── etl.py
├── io.py
└── transforms.py

## Inputs

Place raw CSV files in:

data/raw/orders.csv

data/raw/users.csv

Expected columns

orders.csv:

order_id

user_id

amount

quantity

created_at

status

users.csv:

user_id

country

signup_date

Column names are normalized (lowercased and stripped) during ETL, so minor casing or spacing differences are handled automatically.

## How to run

# Setup

From the project root:

python -m venv .venv

Activate the environment:

Windows (PowerShell):
..venv\Scripts\activate

macOS / Linux:
source .venv/bin/activate

Install dependencies:
pip install -r requirements.txt

Run ETL

Run the ETL pipeline from the project root:

python scripts/run_etl.py

What this does:

Reads raw data from data/raw/

Validates and cleans orders and users

Adds missing-value flags and time features

Winsorizes order amounts

Joins orders with users

Writes processed outputs and run metadata

## Outputs

After running ETL, the following files are created:

data/processed/orders_clean.parquet

data/processed/users.parquet

data/processed/analytics_table.parquet

data/processed/\_run_meta.json

Figures generated during EDA are saved to:

reports/figures/\*.png

Run EDA

Open the notebook:
notebooks/eda.ipynb

Run all cells from top to bottom.

The notebook:

Loads data/processed/analytics_table.parquet

Performs exploratory data analysis (EDA)

Generates charts and exports them to reports/figures/

To export Plotly figures to PNG, ensure kaleido is installed:

pip install kaleido

Definitions

Revenue:
sum(amount_winsor) for paid orders only.

Paid order:
status_clean == "paid"

Refund:
status_clean == "refund"

Refund rate:
Number of refunded orders divided by total orders.

Time window:
Based on created_at timestamps and aggregated during EDA.

Data quality caveats

Missingness:
Some records contain missing values (e.g., amount, quantity, or created_at). Missingness flags are added during ETL.

Duplicates:
Primary keys (order_id for orders and user_id for users) are enforced as unique.

Join coverage:
Some orders may not match user records, resulting in missing country or signup_date.

Outliers:
Order amounts are right-skewed. Winsorization is applied to reduce the influence of extreme values.

Next questions

How does average order value (AOV) change over time?

Are refund rates higher for specific countries or customers?

Can users be analyzed in cohorts based on signup_date?

How sensitive are results to different winsorization thresholds?

## Notes

Do not run files inside src/bootcamp_data directly.

Always run ETL via:
python scripts/run_etl.py

## Following this README
