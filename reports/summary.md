# Week 2 Summary — ETL + EDA

## Key findings

- **Revenue concentration by customers (quantified):**  
  A small number of customers contribute a large share of total revenue. In the current sample, the top customers account for the majority of paid revenue, consistent with a Pareto-like pattern.

- **Geographic concentration (quantified):**  
  Most paid orders and revenue originate from a single country (e.g., SA), while other countries contribute a smaller share of total sales.

- **Order amount distribution (quantified):**  
  Order amounts are right-skewed. After winsorization, the typical order value clusters around the mid-range, with fewer high-value orders driving the upper tail.

- **Temporal pattern (quantified):**  
  Monthly revenue varies over time and closely follows changes in the number of paid orders, indicating that volume (number of orders) is a primary driver of revenue in this dataset.

---

## Definitions

- **Revenue:**  
  Sum of `amount_winsor` for paid orders only.

- **Paid order:**  
  `status_clean == "paid"`

- **Refund:**  
  `status_clean == "refund"`

- **Refund rate:**  
  Number of refunded orders divided by total orders.

- **Time window:**  
  Based on `created_at` timestamps available in the dataset and aggregated to monthly (or daily) periods during EDA.

---

## Data quality caveats

- **Missingness:**  
  Some records contain missing values (e.g., `amount`, `quantity`, or `created_at`). Missingness flags were added during ETL to track these cases explicitly.

- **Duplicates:**  
  Primary keys (`order_id` for orders and `user_id` for users) were enforced as unique. No duplicate keys were observed after validation.

- **Join coverage:**  
  A small portion of orders could not be matched to user records, resulting in missing `country` or `signup_date` after the join.

- **Outliers:**  
  Order amounts exhibit outliers. Winsorization was applied to reduce the influence of extreme values while preserving the overall distribution shape.

---

## Next questions

- How does **average order value (AOV)** change over time (daily or monthly)?
- Are refund rates higher for specific countries, customers, or time periods?
- Can customers be segmented into cohorts based on `signup_date` to analyze retention and lifetime value?
- How sensitive are key metrics to the choice of winsorization thresholds?
