# NammaMart ETL and ELT Pipelines

Sikandar Ali Abdul, FDAIE Module 2, Cohort 5

## What's in this repo

- `dags/nammamart_etl_sikandar.py` is the ETL pipeline. Pandas cleans the data first and only good rows go into DuckDB.
- `dags/nammamart_elt_sikandar.py` is the ELT pipeline. Every row lands in DuckDB as it is, and SQL does the cleaning afterwards.
- `include/nammamart_sikandar/` has the shared code: the list of quality checks, masking, the run audit, the Slack alert, the pandas rules and the reconciliation.
- `include/sql/sikandar/` has every SQL file for staging, marts, the dq scorecard and the business questions.
- `include/experiments_sikandar/make_experiments.py` builds the modified copies of the files I used for the Part F experiments, so the originals never had to be touched.
- `include/data/nammamart/` is the dataset copied from `assignment/nammamart_dataset/`.
- `submissions/sikandar/screenshots/` has the screenshots.

The warehouse file `include/warehouse_sikandar.duckdb` gets created on the first run. I kept it out of Git because the raw customer table inside it has real phone numbers and emails.

Both DAGs ran on Airflow 3.1.3 with DuckDB and pandas. The only change to the project setup is `duckdb>=1.4.0` in `requirements.txt`, because the ELT staging uses `MERGE INTO`.

The short version of the result: both pipelines gave exactly the same September revenue, ₹11,09,887.35 from 2,020 delivered orders. 218 of the 5,212 rows were set aside with a reason, and none of them made it into the finance tables.

## Part 0: Profiling and the data issues log

Before writing any pipeline code I profiled all six files: row counts, blanks per column, duplicate keys, min and max of the numbers and dates, and every distinct value of the category columns. The client was right, every file had problems.

A few things that stood out:

- orders.csv has 2,412 rows but only 2,400 unique order IDs. The 12 extra rows are exact copies.
- `order_status` has `Pending??` three times and `delivered` in lower case twice. `payment_mode` is spelt nine different ways, including `upi`, `UPI ` with a trailing space, `cod` and `Cashh`.
- Quantity goes down to minus 3, discount goes up to 250%, and delivery time goes up to 1,440 minutes, which is a full day for a 10-minute delivery company.
- Eight payments point to orders like ORD970544 that don't exist. All eight have the same timestamp, 2026-09-18 14:22:10, which looks like a test batch that got into the export.
- Fourteen successful payments are short of the order by exactly ₹10, ₹25, ₹50 or ₹100. That pattern is too neat to be random.
- Four products have a cost price of exactly 1.2 times the MRP, and one has an MRP of minus 203.
- The increment file is clean. It has 150 new orders and 10 old orders that changed from DELIVERED to RETURNED.

For relationships, orders to customers only breaks on the 18 blank customer IDs. Orders to stores breaks 5 times (S17, S99, S00), orders to products 9 times (P9xxx IDs), and payments to orders 8 times. 27 delivered orders have no payment at all. Every cancelled order correctly has no payment.

### Data Issues Log

| # | File | Row key | Column | Problem | Dimension | Rule | Severity |
|---|---|---|---|---|---|---|---|
| 1 | orders | ORD000539, ORD000783, ORD001110, ORD001239, ORD001326, ORD001434, ORD001441, ORD001462, ORD001549, ORD001692, ORD001870, ORD002129 | order_id | Same row exported twice | Uniqueness | Keep the first copy, quarantine the second | Critical |
| 2 | orders | ORD002255, ORD000701, ORD000997 and 15 more | customer_id | Blank | Completeness | customer_id must be present | Critical |
| 3 | orders | ORD001554, ORD001800, ORD000731, ORD001277, ORD000326, ORD001515, ORD002229, ORD000878 | delivery_minutes | Delivered but no delivery time | Completeness | Delivered orders need a delivery time | Critical |
| 4 | orders | ORD000403 (`N/A`), ORD000362 (blank), ORD001610 and ORD000534 (`2026-13-05`), ORD002012 and ORD000946 (`31/09/2026`) | order_ts | Not a real timestamp | Validity | Must parse as YYYY-MM-DD HH:MM:SS | Critical |
| 5 | orders | ORD002119, ORD001765, ORD001725 (Jan 2027), ORD001121, ORD001965 (Nov 2026), ORD001693, ORD001089 (21 Oct 2026) | order_ts | Dated after the export was taken | Timeliness | Not after the export cut-off | Critical |
| 6 | orders | ORD000500, ORD001970, ORD001637 | order_status | `Pending??` | Validity | Must be DELIVERED, CANCELLED or RETURNED | Critical |
| 7 | orders | ORD001066, ORD000153 | order_status | `delivered` in lower case | Consistency | Upper-case it and log a warning | Warning |
| 8 | orders and payments | `upi`, `UPI `, `CARD`, `cod` (8 in each file) | payment_mode | Spelling variants | Consistency | Map to the standard value and log a warning | Warning |
| 9 | orders and payments | ORD001812, ORD001414 | payment_mode | `Cashh`. The payment rows have a gateway reference, which cash orders never have | Validity | Not mapped, quarantined until the client confirms | Critical |
| 10 | orders | ORD002103 (250), ORD001468 (120), ORD001237 (150) | discount_pct | Discount above 100% | Validity | Between 0 and 100 | Critical |
| 11 | orders | ORD000469, ORD002360, ORD001724, ORD000099 (zero), ORD001296, ORD001459, ORD000867, ORD000462, ORD001124, ORD001384 (negative) | quantity | Zero or negative | Accuracy | quantity above 0 | Critical |
| 12 | orders | ORD002305, ORD002004, ORD001624 (ten times too big), ORD001755, ORD001274 (double or half) and 13 more | order_amount | Doesn't match quantity × price × (1 − discount/100) | Accuracy | Must match within one paisa | Critical |
| 13 | orders | ORD001037, ORD002101, ORD000424 (480), ORD001104, ORD000020 (900), ORD000455 (1,440) | delivery_minutes | Hours, not minutes | Accuracy | Below 120 minutes | Critical |
| 14 | orders | ORD001799 (S99), ORD002126 (S00), ORD001046, ORD001492, ORD001275 (S17) | store_id | Store doesn't exist | Consistency | Must be in the store master | Critical |
| 15 | orders | ORD000986, ORD001058, ORD001685, ORD000070, ORD000789, ORD000596, ORD000134, ORD000965, ORD002179 | product_id | P9xxx product doesn't exist | Consistency | Must be in the product master | Critical |
| 16 | orders | 71 orders for P0109, P0019, P0073, P0074, P0117 | product_id | The product itself failed its checks (rows 26 and 27) | Consistency | Must be in the clean product master | Critical |
| 17 | payments | PAY0000509, PAY0000755, PAY0001651, PAY0002000 | payment_id | Duplicate payment rows | Uniqueness | Keep the first, quarantine the copy | Critical |
| 18 | payments | PAY0002217 to PAY0002224 | order_id | Order doesn't exist | Consistency | Order must be in the order export | Critical |
| 19 | payments | PAY0000396, PAY0000127, PAY0001908, PAY0001037, PAY0000337, PAY0001488, PAY0000267 | payment_ts | Future date | Timeliness | Not after the export cut-off | Critical |
| 20 | payments | PAY0001568, PAY0002175, PAY0000751, PAY0000944, PAY0000791, PAY0000737 (negative), PAY0000116, PAY0001111, PAY0001322, PAY0000954 (zero) | amount_paid | Successful payment of zero or less | Accuracy | Successful payments above 0 | Critical |
| 21 | payments | PAY0000779 (₹2.60 against ₹102.60), PAY0000354, PAY0000323 and 11 more | amount_paid | Short by exactly ₹10, ₹25, ₹50 or ₹100 | Consistency | Must equal order_amount, goes to the reconciliation table | Warning |
| 22 | customers | C00036, C00103, C00226, C00252, C00313 | customer_id | Two rows each, with different phone and area | Uniqueness | Keep the first row | Critical |
| 23 | customers | C00337, C00045, C00327, C00193 (`N/A`), C00307, C00363, C00392, C00062, C00067 (no @), C00063, C00310, C00112 (no .com) | email | Not a valid email | Validity | name@domain.tld | Warning |
| 24 | customers | C00151, C00240, C00095, C00357, C00130, C00290, C00339 | phone | 7 to 9 digits | Validity | 10 digits starting with 6, 7, 8 or 9 | Warning |
| 25 | customers | C00132, C00083, C00166 (`5600XX`), plus 6 blank cities and 8 tiers in the wrong case (`BRONZE`, `bronze`, `SILVER`) | pincode, city, loyalty_tier | Invalid or inconsistent | Validity, Completeness, Consistency | Log a warning, title-case the tier | Warning |
| 26 | products | P0109 | mrp | MRP of minus 203 | Accuracy | MRP above 0 | Critical |
| 27 | products | P0019, P0073, P0074, P0117, P0109 | cost_price | Cost higher than MRP | Accuracy | Cost can't be more than MRP | Critical |
| 28 | products | P0030, P0140 | product_id | A second "(old)" row with a lower MRP | Uniqueness | Keep the current row | Critical |
| 29 | products | P0051, P0060, P0068 | category | Blank | Completeness | Fill with "Unknown" and log a warning | Warning |
| 30 | customers | C00400, C00398, C00038 | signup_date | Sign-up date in the future | Timeliness | Log a warning (see below) | Warning |
| 31 | stores | S07 | zone | Blank | Completeness | Fill with "Unknown" and log a warning | Warning |
| 32 | stores | S12 | capacity_orders_per_day | Zero | Accuracy | Capacity above 0 | Warning |
| 33 | increment | ORD000017, ORD000271, ORD000332, ORD000337, ORD000584, ORD000708, ORD000822, ORD001025, ORD001067, ORD001990 | order_status | Changed from DELIVERED to RETURNED | Timeliness | Upsert on order_id and keep the history | Not an error |

I changed the suggested severity in three places. A future sign-up date, a blank category and a blank zone are warnings in my pipelines, not critical. If a typo in the CRM rejected a customer, all their paid orders would get rejected with them. A missing category or zone shouldn't make revenue disappear either. It shows up under "Unknown" where someone can see it and fix it.

### Questions I'd ask the client before building

1. Four products have a cost of exactly 1.2 times the MRP, and P0109 has a negative MRP. Are these typing mistakes? For now, 71 September orders worth ₹25,075 are held out of the finance tables because I can't trust their margin.
2. `Cashh` shows up on two orders, but their payments have a gateway reference. Is it Cash on Delivery or something else?
3. Five customer IDs appear twice with different phone numbers, and there's no "last updated" date. Which one is the current record?
4. Fourteen payments are short by a round ₹10, ₹25, ₹50 or ₹100. Is the gateway applying coupons or cashback that the order system doesn't know about?
5. Is a negative quantity a return, or just a typing mistake?

## Parts A and B: The two pipelines

### ETL

The DAG starts with five extract tasks running in parallel, one per file. Each one checks the column headers first. If a column is missing or renamed, the task fails straight away without retrying, because a schema change won't fix itself. If the file is missing, it retries twice.

Then the validate tasks run. Customers, products and stores are checked in parallel, orders are checked against the clean versions of those three, and payments are checked against the orders.

Failed rows go to `include/quarantine/sikandar/<file>_failed_<run_id>.csv` with a `failure_reason` column. They also go into an `etl_rejections` table with the original row stored as JSON, which is what I used for the audit question in Part E.

The transform step cleans up the spellings, removes duplicates, masks the personal data (`pr***@example.com`, `97******21`, "Priya H."), joins everything and works out `order_date`, `net_revenue`, `gross_margin` and `within_15_min`.

The load writes a star schema: `etl_fact_orders` plus `etl_dim_customer`, `etl_dim_product` and `etl_dim_store`. The dimensions are replaced each run. The fact table uses a MERGE on order_id with a hash of each row, so a re-run only touches rows that actually changed. Any status change also gets written to `etl_order_status_history`.

After that, `reconcile_payments` builds `etl_payment_reconciliation`, `build_reports` builds `etl_daily_zone_revenue` and `etl_category_margin` and prints them in the log, and `run_audit` writes one row to `pipeline_runs`.

### ELT

Each file is loaded into a `raw_` table exactly as it came. Every column is text, plus `_loaded_at`, `_source_file`, `_run_id` and `_row_num`. DuckDB's `read_csv` does the loading, with no pandas involved.

I added a `raw_file_manifest` table that stores a fingerprint (MD5) of every file loaded. If the same file arrives again, nothing is appended. If a file has changed, it's loaded as a new batch. Raw rows are never updated or deleted.

Staging is all SQL. Every row gets a `dq_status` (PASS, WARN or FAIL) and a `failure_reason`. Personal data is masked in `stg_customers`.

`stg_orders` is kept up to date with `MERGE INTO ... ON order_id`, the base file first and then the increment. I also added a guard so that a row is only overwritten by a file with the same or a later export date. Without that, if someone re-sent the September file, it would quietly undo the October returns.

The marts are `mart_daily_store_kpis`, `mart_category_margin`, `mart_delivery_sla`, `mart_payment_reconciliation`, `mart_top_customers` and `mart_dq_rejections`. They're all built from one view, `mart_orders_clean`, which only includes rows with no critical failure.

### Do the totals match?

Yes. Both pipelines give 2,020 delivered orders and ₹11,09,887.35 for September. After the increment, both give 2,150 orders and ₹11,80,984.95. The category, store, payment mode and reconciliation results are the same row for row.

The only difference is in the row counts in `dq_results`. ETL checks every row it read in that run, which is 2,572 orders including the 12 duplicate copies. ELT checks what's in staging, which is 2,550 unique orders. The duplicates still show up in ELT's `ord_order_id_unique` check.

## Part C: Scheduling

```python
@dag(schedule="0 9 * * 1-6",
     start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
     catchup=False, max_active_runs=1,
     default_args={"retries": 2, "retry_delay": timedelta(minutes=1),
                   "execution_timeout": timedelta(minutes=10),
                   "on_failure_callback": slack_on_failure})
```

**What is the cron expression?** `0 9 * * 1-6`, which means 9:00 AM, Monday to Saturday. Because the start date is in Asia/Kolkata, the cron is read in IST.

**What time does the run show in UTC?** 03:30. With the UI set to UTC, the next run shows as 2026-10-23 03:30. With it set to IST, it shows 09:00 (screenshots 03 and 04). The Details tab of the DAG still says "Timezone: UTC", but the schedule itself is on IST, and the next run is stored as 03:30 UTC.

**What if the start_date was left in UTC?** Then the cron would be read as 9:00 UTC, which is 2:30 PM in India. Kavya would get the numbers five hours after her 9:30 meeting, which is even later than the 10:30 email they get today.

**What if the laptop or Docker is off at 9 AM?** Nothing runs, because the scheduler is off too. When it comes back, with catchup off, Airflow runs only the most recent missed run and not one for every missed day. I saw this happen. When I turned the DAGs on at 10:25 AM, Airflow straight away ran that morning's 9:00 run. The data still turns up, just late, and nobody gets told. For real use the scheduler needs to run on a machine that's always on, with an alert if there's no `pipeline_runs` row by 9:15.

**Why catchup=False?** Every run processes the full files it finds, and the loads are idempotent. If the laptop was off for three weeks, running all 18 missed days would do the same work 18 times. If someone really needs a backfill, they should choose to run it.

**Retries and timeouts.** Two retries, one minute apart, cover a file that lands a few minutes late or a short wait on the DuckDB lock. A schema change isn't retried. The 10-minute timeout is generous for a run that takes about 25 seconds, but it stops a stuck task from blocking the 9:30 meeting. `max_active_runs=1` keeps it to one writer on the warehouse file.

## Part D: Data quality checks

There are 55 checks, all listed in one place (`CHECKS` in `common.py`). The ETL runs them in pandas and the ELT runs them in SQL, with the same names in both. Critical failures are quarantined. Warnings still load, and they get logged. Every check writes a row to `dq_results` on every run.

| Dimension | Example checks | Count (critical / warning) |
|---|---|---|
| Completeness | customer_id present, delivered orders have a delivery time, order volume compared with the last run, category and zone present | 4 / 5 |
| Uniqueness | order_id, payment_id, customer_id, product_id and store_id each appear once | 5 / 0 |
| Validity | timestamps parse, status and payment mode in the allowed list, discount between 0 and 100, email, phone and pincode format | 8 / 6 |
| Accuracy | quantity above 0, order amount matches the formula, delivery under 120 minutes, MRP above 0, cost not above MRP, price drift against MRP | 6 / 4 |
| Consistency | customer, store and product exist, payment matches an order, payment amount equals order amount, spelling and casing | 4 / 7 |
| Timeliness | no future orders, payments, launch dates or store open dates, freshness of the latest order, future sign-up dates | 4 / 2 |

"Future" here means after the file's export cut-off, which is 30 September for the September files, and the date in the file name for the increment. I fixed the cut-off so September could be replayed. In a live setup it would be the run date.

On the increment run, both DAGs gave the same scorecard: 22 checks failed, 17 warned and 16 passed (screenshot 06).

## Business questions

The answers come from the warehouse after the increment run, so they cover 1 September to 1 October. ETL and ELT gave identical results.

### ETL

#### Q1. Daily net revenue by zone and by store (DELIVERED only, after discount)

```sql
SELECT f.order_date, s.zone, s.store_id, s.store_name, count(*) AS delivered_orders,
       round(sum(f.net_revenue), 2) AS net_revenue
FROM etl_fact_orders f JOIN etl_dim_store s USING (store_id)
WHERE f.order_status = 'DELIVERED'
GROUP BY ALL ORDER BY f.order_date, s.zone, s.store_id;
```

| order_date | zone | store_id | store_name | delivered_orders | net_revenue |
|---|---|---|---|---|---|
| 2026-09-01 00:00:00 | East | S08 | NammaMart Whitefield | 4 | 2984.7 |
| 2026-09-01 00:00:00 | East | S09 | NammaMart Indiranagar | 5 | 2620.6 |
| 2026-09-01 00:00:00 | East | S10 | NammaMart Marathahalli | 3 | 2141.6 |
| 2026-09-01 00:00:00 | East | S11 | NammaMart HSR Layout | 6 | 2121.15 |
| 2026-09-01 00:00:00 | North | S01 | NammaMart Hebbal | 3 | 1410.8 |
| 2026-09-01 00:00:00 | North | S02 | NammaMart Yelahanka | 6 | 2385.55 |
| 2026-09-01 00:00:00 | North | S03 | NammaMart RT Nagar | 6 | 4583.8 |
| 2026-09-01 00:00:00 | South | S04 | NammaMart Jayanagar | 1 | 1103.3 |

_Showing the first 8 of 460 rows._

#### Q2. Revenue and gross margin by product category

```sql
SELECT * FROM etl_category_margin ORDER BY net_revenue DESC;
```

| category | delivered_orders | units | net_revenue | cost | gross_margin | margin_pct |
|---|---|---|---|---|---|---|
| Staples | 302 | 560.0 | 268583.3 | 201440.16 | 67143.14 | 25.0 |
| Household | 362 | 664.0 | 211727.05 | 157374.54 | 54352.51 | 25.67 |
| Personal Care | 323 | 642.0 | 210889.1 | 163834.52 | 47054.58 | 22.31 |
| Dairy & Bakery | 405 | 760.0 | 179568.95 | 131935.95 | 47633.0 | 26.53 |
| Snacks & Beverages | 340 | 682.0 | 174700.6 | 133329.82 | 41370.78 | 23.68 |
| Fruits & Vegetables | 374 | 722.0 | 93604.2 | 71398.82 | 22205.38 | 23.72 |
| Unknown | 44 | 88.0 | 41911.75 | 36968.36 | 4943.39 | 11.79 |

#### Q3. Average delivery time per store and % of delivered orders within 15 minutes

```sql
SELECT s.store_id, s.store_name, s.zone, count(*) AS delivered_orders,
       round(avg(f.delivery_minutes), 1) AS avg_delivery_minutes,
       round(100.0 * avg(CAST(f.within_15_min AS INTEGER)), 1) AS pct_within_15_min
FROM etl_fact_orders f JOIN etl_dim_store s USING (store_id)
WHERE f.order_status = 'DELIVERED'
GROUP BY ALL ORDER BY s.store_id;
```

| store_id | store_name | zone | delivered_orders | avg_delivery_minutes | pct_within_15_min |
|---|---|---|---|---|---|
| S01 | NammaMart Hebbal | North | 145 | 20.5 | 32.4 |
| S02 | NammaMart Yelahanka | North | 142 | 21.2 | 30.3 |
| S03 | NammaMart RT Nagar | North | 140 | 20.7 | 29.3 |
| S04 | NammaMart Jayanagar | South | 134 | 21.5 | 26.9 |
| S05 | NammaMart JP Nagar | South | 135 | 21.2 | 29.6 |
| S06 | NammaMart Banashankari | South | 151 | 22.4 | 21.9 |
| S07 | NammaMart BTM Layout | Unknown | 148 | 21.1 | 33.1 |
| S08 | NammaMart Whitefield | East | 159 | 20.5 | 35.8 |
| S09 | NammaMart Indiranagar | East | 130 | 21.3 | 26.9 |
| S10 | NammaMart Marathahalli | East | 147 | 21.9 | 26.5 |
| S11 | NammaMart HSR Layout | East | 150 | 20.8 | 28.0 |
| S12 | NammaMart Rajajinagar | West | 113 | 22.0 | 23.9 |
| S13 | NammaMart Malleshwaram | West | 144 | 21.8 | 27.8 |
| S14 | NammaMart Vijayanagar | West | 164 | 21.5 | 31.1 |
| S15 | NammaMart Basaveshwaranagar | West | 148 | 22.3 | 21.6 |

#### Q4a. Revenue by payment mode

```sql
SELECT payment_mode, count(*) AS delivered_orders, round(sum(net_revenue), 2) AS net_revenue
FROM etl_fact_orders WHERE order_status = 'DELIVERED' GROUP BY 1 ORDER BY 3 DESC;
```

| payment_mode | delivered_orders | net_revenue |
|---|---|---|
| UPI | 1166 | 624688.1 |
| Card | 437 | 241764.65 |
| Cash on Delivery | 307 | 177949.5 |
| Wallet | 240 | 136582.7 |

#### Q4b. Amount that does not reconcile with the payment gateway

```sql
SELECT coalesce(mismatch_reason, 'TOTAL') AS mismatch_reason, count(*) AS mismatches,
       round(sum(abs(difference)), 2) AS amount
FROM etl_payment_reconciliation GROUP BY ROLLUP (mismatch_reason) ORDER BY grouping(mismatch_reason), 1;
```

| mismatch_reason | mismatches | amount |
|---|---|---|
| AMOUNT_MISMATCH | 12 | 660.0 |
| DELIVERED_NOT_PAID | 209 | 117677.9 |
| DUPLICATE_PAYMENT | 4 | 1330.5 |
| PAYMENT_FOR_REJECTED_ORDER | 157 | 110600.43 |
| PAYMENT_WITHOUT_ORDER | 8 | 4580.57 |
| RETURNED_NOT_REFUNDED | 11 | 4193.7 |
| TOTAL | 401 | 239043.1 |

#### Q5. Top 10 customers by spend, with loyalty tier (masked contact details only)

```sql
SELECT c.customer_id, c.customer_name_masked, c.email_masked, c.phone_masked, c.loyalty_tier,
       count(*) AS delivered_orders, round(sum(f.net_revenue), 2) AS total_spend
FROM etl_fact_orders f JOIN etl_dim_customer c USING (customer_id)
WHERE f.order_status = 'DELIVERED'
GROUP BY ALL ORDER BY total_spend DESC LIMIT 10;
```

| customer_id | customer_name_masked | email_masked | phone_masked | loyalty_tier | delivered_orders | total_spend |
|---|---|---|---|---|---|---|
| C00136 | Sanjay B. | sa***@example.com | 96******88 | Bronze | 9 | 8738.1 |
| C00283 | Nikhil R. | ni***@example.com | 91******43 | Bronze | 8 | 7934.6 |
| C00236 | Sneha N. | sn***@example.com | 90******01 | Platinum | 12 | 7831.8 |
| C00115 | Sanjay K. | sa***@example.com | 96******85 | Bronze | 7 | 7337.7 |
| C00310 | Kiran P. | ki***@example | 91******35 | Bronze | 8 | 7258.95 |
| C00265 | Vikram H. | vi***@example.com | 91******69 | Gold | 8 | 7151.3 |
| C00338 | Bhavana I. | bh***@example.com | 94******05 | Silver | 9 | 6848.55 |
| C00071 | Mahesh P. | ma***@example.com | 91******42 | Silver | 10 | 6696.15 |
| C00357 | Arjun N. | ar***@example.com | 93*****12 | Bronze | 11 | 6637.9 |
| C00209 | Kavya P. | ka***@example.com | 98******97 | Platinum | 8 | 6519.4 |

#### Q6. Rows rejected in today's run, by file and by reason

```sql
SELECT source_file, reason, count(*) AS rows_rejected
FROM (SELECT source_file, unnest(string_split(failure_reason, '; ')) AS reason
      FROM etl_rejections
      WHERE run_id = (SELECT run_id FROM pipeline_runs WHERE dag_id = 'nammamart_etl_sikandar'
                      ORDER BY end_time DESC LIMIT 1))
GROUP BY ALL ORDER BY source_file, rows_rejected DESC;
```

| source_file | reason | rows_rejected |
|---|---|---|
| customers.csv | cust_customer_id_unique | 5 |
| orders.csv | ord_product_fk | 87 |
| orders.csv | ord_customer_id_not_null | 18 |
| orders.csv | ord_amount_matches_formula | 18 |
| orders.csv | ord_order_id_unique | 12 |
| orders.csv | ord_quantity_positive | 10 |
| orders.csv | ord_delivered_has_minutes | 8 |
| orders.csv | ord_order_ts_not_future | 7 |
| orders.csv | ord_order_ts_valid | 6 |
| orders.csv | ord_delivery_minutes_lt_120 | 6 |
| orders.csv | ord_store_fk | 5 |
| orders.csv | ord_status_allowed | 3 |
| orders.csv | ord_discount_pct_range | 3 |
| orders.csv | ord_payment_mode_allowed | 2 |
| orders_2026-10-01_increment.csv | ord_product_fk | 10 |
| payments.csv | pay_success_amount_positive | 10 |
| payments.csv | pay_order_fk | 8 |
| payments.csv | pay_payment_ts_not_future | 7 |
| payments.csv | pay_payment_id_unique | 4 |
| payments.csv | pay_mode_allowed | 2 |
| products.csv | prod_cost_le_mrp | 5 |
| products.csv | prod_product_id_unique | 2 |
| products.csv | prod_mrp_positive | 1 |

### ELT

#### Q1. Daily net revenue by zone and by store (DELIVERED only, after discount)

```sql
SELECT order_date, zone, store_id, store_name, delivered_orders, net_revenue
FROM mart_daily_store_kpis ORDER BY order_date, zone, store_id;
```

| order_date | zone | store_id | store_name | delivered_orders | net_revenue |
|---|---|---|---|---|---|
| 2026-09-01 00:00:00 | East | S08 | NammaMart Whitefield | 4 | 2984.7 |
| 2026-09-01 00:00:00 | East | S09 | NammaMart Indiranagar | 5 | 2620.6 |
| 2026-09-01 00:00:00 | East | S10 | NammaMart Marathahalli | 3 | 2141.6 |
| 2026-09-01 00:00:00 | East | S11 | NammaMart HSR Layout | 6 | 2121.15 |
| 2026-09-01 00:00:00 | North | S01 | NammaMart Hebbal | 3 | 1410.8 |
| 2026-09-01 00:00:00 | North | S02 | NammaMart Yelahanka | 6 | 2385.55 |
| 2026-09-01 00:00:00 | North | S03 | NammaMart RT Nagar | 6 | 4583.8 |
| 2026-09-01 00:00:00 | South | S04 | NammaMart Jayanagar | 1 | 1103.3 |

_Showing the first 8 of 463 rows._

#### Q2. Revenue and gross margin by product category

```sql
SELECT * FROM mart_category_margin ORDER BY net_revenue DESC;
```

| category | delivered_orders | units | net_revenue | cost | gross_margin | margin_pct |
|---|---|---|---|---|---|---|
| Staples | 302 | 560.0 | 268583.3 | 201440.16 | 67143.14 | 25.0 |
| Household | 362 | 664.0 | 211727.05 | 157374.54 | 54352.51 | 25.67 |
| Personal Care | 323 | 642.0 | 210889.1 | 163834.52 | 47054.58 | 22.31 |
| Dairy & Bakery | 405 | 760.0 | 179568.95 | 131935.95 | 47633.0 | 26.53 |
| Snacks & Beverages | 340 | 682.0 | 174700.6 | 133329.82 | 41370.78 | 23.68 |
| Fruits & Vegetables | 374 | 722.0 | 93604.2 | 71398.82 | 22205.38 | 23.72 |
| Unknown | 44 | 88.0 | 41911.75 | 36968.36 | 4943.39 | 11.79 |

#### Q3. Average delivery time per store and % of delivered orders within 15 minutes

```sql
SELECT * FROM mart_delivery_sla ORDER BY store_id;
```

| store_id | store_name | zone | delivered_orders | avg_delivery_minutes | within_15_min_orders | pct_within_15_min |
|---|---|---|---|---|---|---|
| S01 | NammaMart Hebbal | North | 145 | 20.5 | 47 | 32.4 |
| S02 | NammaMart Yelahanka | North | 142 | 21.2 | 43 | 30.3 |
| S03 | NammaMart RT Nagar | North | 140 | 20.7 | 41 | 29.3 |
| S04 | NammaMart Jayanagar | South | 134 | 21.5 | 36 | 26.9 |
| S05 | NammaMart JP Nagar | South | 135 | 21.2 | 40 | 29.6 |
| S06 | NammaMart Banashankari | South | 151 | 22.4 | 33 | 21.9 |
| S07 | NammaMart BTM Layout | Unknown | 148 | 21.1 | 49 | 33.1 |
| S08 | NammaMart Whitefield | East | 159 | 20.5 | 57 | 35.8 |
| S09 | NammaMart Indiranagar | East | 130 | 21.3 | 35 | 26.9 |
| S10 | NammaMart Marathahalli | East | 147 | 21.9 | 39 | 26.5 |
| S11 | NammaMart HSR Layout | East | 150 | 20.8 | 42 | 28.0 |
| S12 | NammaMart Rajajinagar | West | 113 | 22.0 | 27 | 23.9 |
| S13 | NammaMart Malleshwaram | West | 144 | 21.8 | 40 | 27.8 |
| S14 | NammaMart Vijayanagar | West | 164 | 21.5 | 51 | 31.1 |
| S15 | NammaMart Basaveshwaranagar | West | 148 | 22.3 | 32 | 21.6 |

#### Q4a. Revenue by payment mode

```sql
SELECT payment_mode, count(*) AS delivered_orders, round(sum(net_revenue), 2) AS net_revenue
FROM mart_orders_clean WHERE order_status = 'DELIVERED' GROUP BY 1 ORDER BY 3 DESC;
```

| payment_mode | delivered_orders | net_revenue |
|---|---|---|
| UPI | 1166 | 624688.1 |
| Card | 437 | 241764.65 |
| Cash on Delivery | 307 | 177949.5 |
| Wallet | 240 | 136582.7 |

#### Q4b. Amount that does not reconcile with the payment gateway

```sql
SELECT coalesce(mismatch_reason, 'TOTAL') AS mismatch_reason, count(*) AS mismatches,
       round(sum(abs(difference)), 2) AS amount
FROM mart_payment_reconciliation GROUP BY ROLLUP (mismatch_reason) ORDER BY grouping(mismatch_reason), 1;
```

| mismatch_reason | mismatches | amount |
|---|---|---|
| AMOUNT_MISMATCH | 12 | 660.0 |
| DELIVERED_NOT_PAID | 209 | 117677.9 |
| DUPLICATE_PAYMENT | 4 | 1330.5 |
| PAYMENT_FOR_REJECTED_ORDER | 157 | 110600.43 |
| PAYMENT_WITHOUT_ORDER | 8 | 4580.57 |
| RETURNED_NOT_REFUNDED | 11 | 4193.7 |
| TOTAL | 401 | 239043.1 |

#### Q5. Top 10 customers by spend, with loyalty tier (masked contact details only)

```sql
SELECT * FROM mart_top_customers ORDER BY rank;
```

| rank | customer_id | customer_name_masked | email_masked | phone_masked | loyalty_tier | delivered_orders | total_spend |
|---|---|---|---|---|---|---|---|
| 1 | C00136 | Sanjay B. | sa***@example.com | 96******88 | Bronze | 9 | 8738.1 |
| 2 | C00283 | Nikhil R. | ni***@example.com | 91******43 | Bronze | 8 | 7934.6 |
| 3 | C00236 | Sneha N. | sn***@example.com | 90******01 | Platinum | 12 | 7831.8 |
| 4 | C00115 | Sanjay K. | sa***@example.com | 96******85 | Bronze | 7 | 7337.7 |
| 5 | C00310 | Kiran P. | ki***@example | 91******35 | Bronze | 8 | 7258.95 |
| 6 | C00265 | Vikram H. | vi***@example.com | 91******69 | Gold | 8 | 7151.3 |
| 7 | C00338 | Bhavana I. | bh***@example.com | 94******05 | Silver | 9 | 6848.55 |
| 8 | C00071 | Mahesh P. | ma***@example.com | 91******42 | Silver | 10 | 6696.15 |
| 9 | C00357 | Arjun N. | ar***@example.com | 93*****12 | Bronze | 11 | 6637.9 |
| 10 | C00209 | Kavya P. | ka***@example.com | 98******97 | Platinum | 8 | 6519.4 |

#### Q6. Rows rejected, by file and by reason (current staging state)

```sql
SELECT source_file, reason, rows_rejected FROM mart_dq_rejections ORDER BY source_file, rows_rejected DESC;
```

| source_file | reason | rows_rejected |
|---|---|---|
| customers.csv | cust_customer_id_unique | 5 |
| orders.csv | ord_product_fk | 86 |
| orders.csv | ord_amount_matches_formula | 18 |
| orders.csv | ord_customer_id_not_null | 18 |
| orders.csv | ord_order_id_unique | 12 |
| orders.csv | ord_quantity_positive | 10 |
| orders.csv | ord_delivered_has_minutes | 8 |
| orders.csv | ord_order_ts_not_future | 7 |
| orders.csv | ord_order_ts_valid | 6 |
| orders.csv | ord_delivery_minutes_lt_120 | 6 |
| orders.csv | ord_store_fk | 5 |
| orders.csv | ord_status_allowed | 3 |
| orders.csv | ord_discount_pct_range | 3 |
| orders.csv | ord_payment_mode_allowed | 2 |
| orders_2026-10-01_increment.csv | ord_product_fk | 10 |
| payments.csv | pay_success_amount_positive | 10 |
| payments.csv | pay_order_fk | 8 |
| payments.csv | pay_payment_ts_not_future | 7 |
| payments.csv | pay_payment_id_unique | 4 |
| payments.csv | pay_mode_allowed | 2 |
| products.csv | prod_cost_le_mrp | 5 |
| products.csv | prod_product_id_unique | 2 |
| products.csv | prod_mrp_positive | 1 |

About Q4b: 140 of the "delivered but not paid" rows are the new 1 October orders. The payments file only covers September, so that's expected. For September on its own, 69 delivered orders worth ₹42,386.60 have no successful payment.

## Part E: Data governance

**1. Ownership.** Orders belong to the Head of Operations, payments to Finance, customers to the CRM owner (with the data protection officer for personal data), products to category management and stores to store operations. The warehouse tables belong to the data team, but each mart has a business owner, such as Finance for revenue and reconciliation and Ops for delivery. A rule like the 120-minute limit is a business decision. Ops should propose it, and Finance and the data owner should approve it. The change then goes in as a reviewed pull request to `MAX_DELIVERY_MINUTES`, and the day it changed shows up in `dq_results`.

**2. Data dictionary.** These are the star schema tables. The ELT staging tables use the same column names.

| Column | Type | Meaning | Source | Allowed values | PII |
|---|---|---|---|---|---|
| order_id | VARCHAR | Order key, one row per order | orders.csv, increment | Unique | No |
| order_ts, order_date | TIMESTAMP, DATE | When the order was placed | orders | Not after the export cut-off | No |
| customer_id, store_id, product_id | VARCHAR | Links to the dimension tables | orders | Must exist in the clean dimension | customer_id is an ID, not a name |
| quantity, unit_price, discount_pct | BIGINT, DOUBLE, BIGINT | Order line details | orders | Quantity above 0, discount between 0 and 100 | No |
| order_amount | DOUBLE | Amount after discount | orders | Matches the formula | No |
| payment_mode | VARCHAR | Standard spelling | orders | UPI, Card, Cash on Delivery, Wallet | No |
| order_status | VARCHAR | Latest status | orders, increment | DELIVERED, CANCELLED, RETURNED | No |
| delivery_minutes | BIGINT | Time to deliver | orders | Under 120 | No |
| net_revenue | DOUBLE | order_amount if delivered, otherwise 0 | Derived | 0 or more | No |
| cost_amount, gross_margin | DOUBLE | Quantity × cost price, and revenue minus cost | Derived | | No |
| within_15_min | BOOLEAN | Delivered within 15 minutes | Derived | true, false, or blank if not delivered | No |
| dq_status, dq_warnings | VARCHAR | PASS or WARN, and which warnings | Quality checks | PASS, WARN | No |
| source_file, source_row_num, run_id, loaded_at | | Where the row came from and when | Pipeline | | No |
| customer_name_masked | VARCHAR | First name and last initial | customers.full_name | | Yes, masked |
| email_masked, phone_masked | VARCHAR | `pr***@example.com`, `97******21` | customers | | Yes, masked |
| city, area, pincode | VARCHAR | Where the customer lives | customers | Pincode is 6 digits | Can help identify someone |
| loyalty_tier, signup_date, is_active | VARCHAR, DATE, BOOLEAN | Customer profile | customers | Bronze, Silver, Gold, Platinum | No |
| product_name, category, brand, mrp, cost_price | | Product details, with "Unknown" for a blank category | products | MRP above 0, cost not above MRP | No |
| store_name, zone, area, capacity_orders_per_day | | Store details, with "Unknown" for a blank zone | stores | North, South, East, West | manager_name is an employee name |

**3. Lineage.** I picked one number from `mart_daily_store_kpis`: S05, JP Nagar, 14 September, net revenue ₹1,398.40. In the ELT, `mart_orders_clean` shows it's made of four orders: ORD001060 (₹395), ORD001041 (₹684), ORD001083 (₹93) and ORD001052 (₹226.40). Each row carries its source file, its row number in that file (836, 1697, 1712 and 1877) and its load ID, so one query on `raw_orders` brings back the exact rows as they arrived. The manifest has the file's fingerprint as proof. In the ETL I added `source_file` and `source_row_num` to the fact table, so I can trace the same four rows. But the trail ends at a CSV on disk that might have been replaced since, and the rows rejected that day are only in the quarantine file. ELT was easier, because the whole path from mart to raw is SQL inside one database.

**4. Personal data.** The personal columns are full_name, email and phone in customers. Area and pincode can help identify someone when combined with other data. manager_name in stores is employee data. I masked instead of deleting because the business still wants to recognise a top customer and join on customer_id, but nobody looking at a report should be able to contact or identify them. The unmasked values stay only in the source files and `raw_customers`. The law that applies is India's Digital Personal Data Protection Act, 2023, with its rules. It expects data to be used only for the purpose it was collected for, kept to the minimum needed, protected with reasonable security, deleted when it's no longer needed, and any breach reported. Customers also have the right to see, correct and erase their data.

**5. Access control.** Raw should be readable only by the pipeline's service account and the data engineers who need it for audits. Staging is for data engineers and analysts. Marts are for Finance, Ops and the COO. Quarantine files are for the data team and the owner of each file, so the CRM owner sees rejected customers and Finance sees rejected payments. Unmasked phone numbers should only be seen by the support process that actually calls customers. Nobody doing reporting needs them.

**6. Retention.** I'd keep raw files for 13 months, one financial year plus a month for audits. After that, archive orders and payments, because finance records may need to be kept for up to 8 years, and delete or anonymise customer data. Quarantine files can go after 90 days, or once the owner has fixed or accepted the rows. Masked customer data stays while the account is active and is deleted or anonymised after the customer leaves or asks for it. The cleanup should be its own scheduled DAG that leaves an audit row.

**7. Secrets.** The Slack webhook is read with `Variable.get("SLACK_WEBHOOK_URL")` inside the failure callback. It's set in the Airflow UI under Admin and Variables, or in `airflow_settings.yaml`, which is already in `.gitignore`. It must never be in a DAG file, because the DAGs are on GitHub and get read by every Airflow component. Anyone with the webhook can post into the company Slack, and once a key is in Git history it stays there even after you delete the line.

**8. Audit.** For the rejected order I picked ORD001459, which has a quantity of minus 3 and an amount of minus ₹1,593.

In the ETL, `etl_rejections` and the quarantine CSV show the original row, which was row 815 of orders.csv, with `failure_reason = ord_quantity_positive`. Searching for it in `etl_fact_orders` returns nothing.

In the ELT, `raw_orders` still has the row exactly as it came, with its load time and run ID. `stg_orders` shows it as FAIL with the same reason. `mart_orders_clean`, which every revenue mart reads from, returns nothing for it.

Its payment (PAY0000751, minus ₹1,593, marked SUCCESS) shows up in the reconciliation as a payment for a rejected order.

**9. Change history.** In both pipelines the 1 October file changes ORD000337 and the nine other orders from DELIVERED to RETURNED, so September revenue drops from ₹11,09,887.35 to ₹11,05,693.65, which is ₹4,193.70 less. Finance usually wants a closed month to stay the way it was reported, with the return counted in October when it actually happened. Analytics wants the latest truth. I kept both. The main tables have the current status, and `etl_order_status_history` and `stg_orders_status_history` have the old status, the old amount, the file that changed it and the run. September "as first reported" is the current revenue plus the amounts in the history table. A cleaner way would be a monthly snapshot taken when the books close.

## Part F: Observability

`run_audit` runs at the end of every run, even when something has failed (`trigger_rule="all_done"`), and writes one row to `pipeline_runs`. If the run didn't reach the end, it records FAILED and fails itself, so the run shows red in Airflow instead of a misleading green. On any task failure, a callback sends a Slack message using the webhook from Airflow Variables. Without a webhook it prints the message in the log.

I ran the experiments in this order. Run 1 was the scheduled 9:00 run with only the five September files. Run 2 was a manual re-run. For run 3 I added the increment file. For experiments 4 to 9 I used modified copies made by `make_experiments.py`, and pointed the DAGs at them through the `data_dir` and `warehouse` parameters, against a copy of the warehouse. The original files were never edited, so there was nothing to restore.

| # | Experiment | Expected | What I saw (ETL and ELT) | Where | What I'd change |
|---|---|---|---|---|---|
| 1 | Happy path | Both green, bad rows set aside | ETL took about 23 seconds and ELT about 29. The five extracts and five raw loads started within half a second of each other, and the six marts started together. Orders: 2,412 in, 2,237 passed, 175 quarantined. Payments: 2,228, 2,197, 31. Customers: 405, 400, 5. Products: 152, 145, 7. Stores: 15, 15, 0 | Graph view, `pipeline_runs`, validate logs | The marts start together but wait for each other on DuckDB's single-writer lock, up to 13 seconds. A server database wouldn't have that problem |
| 2 | Re-run | Nothing doubles | ETL logged `inserted=0 updated=0 unchanged=2237`, and the fact table stayed at 2,237. ELT recognised the same file fingerprints and appended nothing, so `raw_orders` stayed at 2,412. That's what I wanted, raw keeps each distinct file once | Load logs, `pipeline_runs` | Nothing |
| 3 | Increment | 150 new, 10 updated, no duplicates | ETL inserted 140 and updated 10. The other 10 new orders are for the products that failed their checks, so they were quarantined. ELT appended 160 raw rows and the MERGE inserted 150 and updated 10. Ten status changes were recorded in both history tables, and there were no duplicate order IDs | MERGE log, history tables | Get the five products fixed, and those 10 orders will load on the next run |
| 4 | products.csv missing | Retries, then fails | The extract task tried at 10:33:05, went to up_for_retry, and failed for good on the third try at 10:35:11. Everything after it went to upstream_failed. `run_audit` still ran, recorded FAILED and failed the run. The Slack callback fired and printed the alert | Task tries in the UI, red run | Add a file sensor that waits until about 8:45 and then alerts the file owner by name |
| 5 | Five bad rows added | Each one quarantined with the right reason | All five were caught in both pipelines: ORD900001 for a blank customer, ORD900002 for negative quantity, ORD900003 for an unknown store, ORD900004 for SHIPPED status and ORD900005 for a 2027 timestamp. Each check's failed count in `dq_results` went up by exactly one | `etl_rejections`, quarantine CSV, `stg_orders`, `dq_results` | Nothing |
| 6 | order_amount renamed to total | Fails loudly | Failed on the first try with no retry: `[Schema] orders.csv: columns changed. missing=['order_amount'] unexpected=['total']`. Nothing was loaded | Task log | Keep a schema contract for each file in Git and alert the source system's owner |
| 7 | Only 50 orders | Volume warning | The volume check warned in both (50 against 2,412 last time). ETL also quarantined 2,170 payments because their orders were missing, which made a lot of noise. The fact table and marts didn't change, because the upsert never deletes | `dq_results` | A drop that big should stop the reports from being published, not just warn |
| 8 | Price × 100 for store S05 | Something catches it | The price drift check flagged 139 rows in ETL and 137 in ELT, and the amount formula check failed 162 and 160 rows, so they were quarantined. But the two pipelines behaved differently here. ETL kept the last good S05 rows, so S05 still showed ₹82,456. In ELT, S05 dropped to ₹7,491 and South zone fell from ₹2,50,903 to ₹1,75,938, because the current staging rows were now marked FAIL | `dq_results`, marts compared with the fact table | ELT needs a stop: if failures for a store or file jump suddenly, keep yesterday's marts and send an alert |
| 9 | Latest order 30 days old | Freshness warning | The freshness check warned in both. A side effect: the future-date failures went down from 7 to 5, because moving every date back 30 days pulled two of the October orders into September | `dq_results` | Also check against today's date, and alert if the file isn't from today |
| 10 | Recovery | Only the failed task and what comes after it re-run | After putting products.csv back in the experiment 4 folder, I cleared the failed task with its downstream tasks. Airflow cleared 11 of the 17 ETL tasks and 13 of the 21 ELT tasks. The other extracts kept their 10:33 start time, the cleared ones ran again at 10:37, and the run went green | Grid view, task list | Nothing. This is why it helps to keep tasks small |

I also had a real failure while building this. DuckDB 2.0 doesn't accept the old `x -> ...` lambda syntax any more, so all four staging tasks failed after three tries on the very first run. I fixed the SQL and cleared those tasks, the same way as in experiment 10.

### The five pillars

| Pillar | How it's covered | How well |
|---|---|---|
| Freshness | Freshness check, future-date checks, the 9:00 schedule, end time in `pipeline_runs` | Medium. It looks at the data's dates but doesn't alert if the run itself is late |
| Volume | Order count compared with the last run, rows in, loaded and quarantined per run | Medium |
| Schema | Every file's headers are checked before anything is written | The strongest. Experiment 6 failed loudly and loaded nothing |
| Distribution | Price against MRP, typical delivery time, the amount formula | The weakest. It's one fixed ratio, with no normal range per store, and in experiment 8 the ELT marts still moved |
| Lineage | Source file, row number, load ID, file fingerprint and run ID on every row, and the original row kept in quarantine | Strong in ELT, partial in ETL |

## Part G: Recommendation to Sameer

**To:** Sameer, CTO
**About:** ETL or ELT for the 9 AM report

Sameer, my recommendation is ELT, with one ETL-style safeguard for Finance. Load every export into raw tables exactly as it comes, clean and model it in SQL, and only publish the finance tables when that day's quality checks pass.

Here's what I saw when I ran both on the same September data.

**Data quality.** With the same rules, both pipelines gave the same answer down to the paisa: ₹11,09,887.35, 218 rejected rows and 251 payment mismatches for September. Where ETL did better was experiment 8. When one store's prices came in a hundred times too high, ETL kept showing yesterday's good numbers. ELT's figure for that store dropped from ₹82,456 to ₹7,491, because ELT shows whatever is in today's file. So Finance's worry is real. I think the answer is a gate on ELT rather than choosing ETL.

**Audit.** ELT is clearly better here. I could trace a number on the dashboard back to the exact rows in the original file with a SQL query, and show a rejected order exactly as it arrived next to the rule it broke. In ETL, the rejected rows sit in CSV files, and the original export can be overwritten.

**Changing a rule.** Also easier in ELT. If Ops changes the 120-minute limit, I edit one SQL file and rebuild staging and the marts from raw. Nobody has to send old files again. The late returns in the 1 October file went in through the MERGE with no duplicates, and their history was kept.

**Growing 1,000 times, to about 2.4 million orders a month.** ETL passes pandas tables between tasks in memory and re-reads every file every day. ELT does the work inside the database and only loads new files. DuckDB can handle millions of rows on one machine, and the same SQL can move to Postgres, BigQuery or Snowflake later with small changes.

**Cost.** About the same. Both use only free, open-source tools. ELT keeps the raw data, which is a few GB a year at today's volume. The real cost is the machine that has to stay on to run the scheduler.

**The three biggest risks with ELT, and what I'd do about them:**

1. A bad file reaches the CFO's dashboard, like in experiment 8. Add a gate: if failures for a file or store jump past a limit, or volume drops by more than 90%, keep yesterday's finance tables, mark them as stale, and alert Ops and Finance before 9:00.
2. Personal data sits unmasked in the raw layer. Only the pipeline account should read raw, raw should be deleted after 13 months, and masking should happen in staging, which is the first layer anyone else can see.
3. Everything runs on one machine with one writer. A laptop can miss a run without anyone knowing (Part C), and DuckDB only lets one task write at a time. Tasks waited up to 13 seconds in my runs. Move Airflow to a machine that's always on, add an alert for "no run by 9:15", and move to a server database once more than one team needs to write to it.

## Screenshots

| File | What it shows |
|---|---|
| 01_etl_graph_green_run.png | ETL graph, scheduled 9:00 run, all green |
| 02_elt_graph_green_run.png | ELT graph, scheduled 9:00 run, all green |
| 03_dags_list_schedule_ist_view.png | Both DAGs with `0 9 * * 1-6` and the next run at 09:00 in IST |
| 04_dags_list_schedule_utc_view.png | The same page in UTC, next run at 03:30 |
| 05_pipeline_runs.png | `pipeline_runs` for runs 1 to 3 |
| 06_dq_results.png | `dq_results`, the checks that didn't pass and the summary by dimension |
| 07_payment_reconciliation.png | Reconciliation summary and some example mismatches |
| 08_quarantine_file.png | A quarantine file with its failure_reason column |
| exp04_*.png, exp10_*.png | The missing-file failure with its three tries, and the recovery after clearing |
