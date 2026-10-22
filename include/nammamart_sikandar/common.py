"""
include/nammamart_sikandar/common.py
------------------------------------
Shared configuration for the NammaMart ETL and ELT DAGs.

One place for:
  * paths (relative to this file, so the code runs in Astro and anywhere else)
  * the expected schema of every source file
  * the data-quality check catalogue (name, file, dimension, severity)
  * a DuckDB connection helper that waits politely for the file lock
  * the run-audit / dq_results table definitions
  * the Slack failure callback (webhook read from an Airflow Variable)
"""

from __future__ import annotations

import os
import re
import time
from datetime import datetime
from pathlib import Path

import duckdb

# ── Paths ────────────────────────────────────────────────────────────────────
INCLUDE_DIR = Path(__file__).resolve().parents[1]           # .../include
DEFAULT_DATA_DIR = INCLUDE_DIR / "data" / "nammamart"
DEFAULT_WAREHOUSE = INCLUDE_DIR / "warehouse_sikandar.duckdb"
QUARANTINE_DIR = INCLUDE_DIR / "quarantine" / "sikandar"
TMP_DIR = INCLUDE_DIR / "tmp" / "sikandar"
SQL_DIR = INCLUDE_DIR / "sql" / "sikandar"


def resolve(path_like: str | None, default: Path) -> Path:
    """Params may give a path relative to include/ (e.g. 'data/experiments/exp5')."""
    if not path_like:
        return default
    p = Path(path_like)
    return p if p.is_absolute() else INCLUDE_DIR / p


def safe_run_id(run_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)


# ── Source files and their expected columns ─────────────────────────────────
ORDER_COLS = [
    "order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity",
    "unit_price", "discount_pct", "order_amount", "payment_mode", "order_status",
    "channel", "delivery_minutes",
]
SCHEMAS = {
    "orders": ORDER_COLS,
    "payments": ["payment_id", "order_id", "payment_ts", "payment_mode", "amount_paid",
                 "payment_status", "gateway_ref"],
    "customers": ["customer_id", "full_name", "email", "phone", "city", "area", "pincode",
                  "signup_date", "loyalty_tier", "is_active"],
    "products": ["product_id", "product_name", "category", "sub_category", "brand", "mrp",
                 "cost_price", "unit", "is_perishable", "launch_date"],
    "stores": ["store_id", "store_name", "zone", "area", "pincode", "open_date",
               "manager_name", "capacity_orders_per_day"],
}
INCREMENT_GLOB = "orders_*_increment.csv"

# Business rules agreed with the client
VALID_STATUS = ("DELIVERED", "CANCELLED", "RETURNED")
VALID_PAYMENT_MODES = ("UPI", "Card", "Cash on Delivery", "Wallet")
VALID_PAYMENT_STATUS = ("SUCCESS", "FAILED", "REFUNDED")
VALID_TIERS = ("Bronze", "Silver", "Gold", "Platinum")
VALID_ZONES = ("North", "South", "East", "West")
MAX_DELIVERY_MINUTES = 120          # owner: Head of Ops (see SUBMISSION.md, governance)
SLA_MINUTES = 15
PRICE_DRIFT_RATIO = 1.5             # unit_price more than 1.5x (or below 1/1.5x) MRP = drift
VOLUME_DROP_PCT = 90

# Export cut-off per file: a record dated after the export was taken is "from the future".
# In production this would be the run's logical date; it is pinned here so September can be replayed.
DEFAULT_EXPORT_CUTOFF = "2026-09-30 23:59:59"


def export_cutoff(file_name: str) -> str:
    m = re.match(r"orders_(\d{4}-\d{2}-\d{2})_increment\.csv$", file_name)
    if m:
        return f"{m.group(1)} 23:59:59"
    return DEFAULT_EXPORT_CUTOFF


# Payment-mode spellings seen in the exports, mapped to the canonical value.
# 'Cashh' is deliberately NOT mapped: those rows carry a gateway reference, so they
# may not be cash at all. They are quarantined until the client confirms (question 2).
PAYMENT_MODE_MAP = {
    "UPI": "UPI",
    "CARD": "Card",
    "CASH ON DELIVERY": "Cash on Delivery",
    "COD": "Cash on Delivery",
    "WALLET": "Wallet",
}
NULL_TOKENS = ("", "N/A", "NA", "NULL", "NONE")

# ── Data-quality check catalogue ────────────────────────────────────────────
# (check_name, file, dimension, severity, description). Used by BOTH pipelines,
# so ETL (pandas) and ELT (SQL) report identical check names in dq_results.
CRITICAL, WARNING = "CRITICAL", "WARNING"
CHECKS = [
    # orders
    ("ord_customer_id_not_null", "orders", "Completeness", CRITICAL, "customer_id is present"),
    ("ord_delivered_has_minutes", "orders", "Completeness", CRITICAL, "DELIVERED orders have delivery_minutes"),
    ("ord_order_id_unique", "orders", "Uniqueness", CRITICAL, "order_id appears once per file"),
    ("ord_order_ts_valid", "orders", "Validity", CRITICAL, "order_ts parses as YYYY-MM-DD HH:MM:SS"),
    ("ord_status_allowed", "orders", "Validity", CRITICAL, "order_status in DELIVERED/CANCELLED/RETURNED"),
    ("ord_payment_mode_allowed", "orders", "Validity", CRITICAL, "payment_mode in UPI/Card/Cash on Delivery/Wallet"),
    ("ord_discount_pct_range", "orders", "Validity", CRITICAL, "discount_pct between 0 and 100"),
    ("ord_quantity_positive", "orders", "Accuracy", CRITICAL, "quantity > 0"),
    ("ord_amount_matches_formula", "orders", "Accuracy", CRITICAL, "order_amount = qty x price x (1 - disc/100)"),
    ("ord_delivery_minutes_lt_120", "orders", "Accuracy", CRITICAL, "delivery_minutes below 120"),
    ("ord_customer_fk", "orders", "Consistency", CRITICAL, "customer_id exists in clean customer master"),
    ("ord_store_fk", "orders", "Consistency", CRITICAL, "store_id exists in clean store master"),
    ("ord_product_fk", "orders", "Consistency", CRITICAL, "product_id exists in clean product master"),
    ("ord_order_ts_not_future", "orders", "Timeliness", CRITICAL, "order_ts not after the export cut-off"),
    ("ord_status_casing", "orders", "Consistency", WARNING, "order_status already upper-case and trimmed"),
    ("ord_payment_mode_casing", "orders", "Consistency", WARNING, "payment_mode already in canonical spelling"),
    ("ord_delivery_minutes_typical", "orders", "Accuracy", WARNING, "delivery_minutes within the normal 8-35"),
    ("ord_price_vs_mrp", "orders", "Accuracy", WARNING, "unit_price within 1.5x of the product MRP (drift)"),
    ("ord_freshness", "orders", "Timeliness", WARNING, "latest order_ts within 1 day of the export cut-off"),
    ("ord_volume", "orders", "Completeness", WARNING, "row count not down more than 90% vs previous run"),
    ("ord_store_capacity", "orders", "Accuracy", WARNING, "daily orders per store within capacity_orders_per_day"),
    # payments
    ("pay_payment_id_unique", "payments", "Uniqueness", CRITICAL, "payment_id appears once"),
    ("pay_order_id_not_null", "payments", "Completeness", CRITICAL, "order_id is present"),
    ("pay_payment_ts_valid", "payments", "Validity", CRITICAL, "payment_ts parses as a timestamp"),
    ("pay_status_allowed", "payments", "Validity", CRITICAL, "payment_status in SUCCESS/FAILED/REFUNDED"),
    ("pay_mode_allowed", "payments", "Validity", CRITICAL, "payment_mode in the allowed list"),
    ("pay_success_amount_positive", "payments", "Accuracy", CRITICAL, "SUCCESS payments have amount_paid > 0"),
    ("pay_order_fk", "payments", "Consistency", CRITICAL, "order_id exists in the order export"),
    ("pay_payment_ts_not_future", "payments", "Timeliness", CRITICAL, "payment_ts not after the export cut-off"),
    ("pay_amount_matches_order", "payments", "Consistency", WARNING, "SUCCESS amount_paid equals order_amount"),
    ("pay_gateway_ref_present", "payments", "Completeness", WARNING, "non-cash payments carry a gateway_ref"),
    ("pay_mode_casing", "payments", "Consistency", WARNING, "payment_mode already in canonical spelling"),
    # customers
    ("cust_customer_id_not_null", "customers", "Completeness", CRITICAL, "customer_id is present"),
    ("cust_customer_id_unique", "customers", "Uniqueness", CRITICAL, "one row per customer_id (first kept)"),
    ("cust_loyalty_tier_allowed", "customers", "Validity", CRITICAL, "loyalty_tier in Bronze/Silver/Gold/Platinum"),
    ("cust_email_format", "customers", "Validity", WARNING, "email looks like name@domain.tld"),
    ("cust_phone_10_digits", "customers", "Validity", WARNING, "phone is 10 digits starting 6-9"),
    ("cust_pincode_6_digits", "customers", "Validity", WARNING, "pincode is 6 digits"),
    ("cust_city_present", "customers", "Completeness", WARNING, "city is present"),
    ("cust_loyalty_tier_casing", "customers", "Consistency", WARNING, "loyalty_tier uses one casing"),
    ("cust_signup_not_future", "customers", "Timeliness", WARNING, "signup_date not after the export cut-off"),
    # products
    ("prod_product_id_unique", "products", "Uniqueness", CRITICAL, "one row per product_id (first kept)"),
    ("prod_mrp_positive", "products", "Accuracy", CRITICAL, "mrp > 0"),
    ("prod_cost_le_mrp", "products", "Accuracy", CRITICAL, "cost_price <= mrp"),
    ("prod_launch_not_future", "products", "Timeliness", CRITICAL, "launch_date not after the export cut-off"),
    ("prod_category_present", "products", "Completeness", WARNING, "category is present (else 'Unknown')"),
    ("prod_is_perishable_valid", "products", "Validity", WARNING, "is_perishable is True/False"),
    ("prod_consistent_cost_by_name", "products", "Consistency", WARNING, "product_name has no '(old)' marker"),
    # stores
    ("store_store_id_unique", "stores", "Uniqueness", CRITICAL, "one row per store_id"),
    ("store_open_date_not_future", "stores", "Timeliness", CRITICAL, "open_date not after the export cut-off"),
    ("store_zone_present", "stores", "Completeness", WARNING, "zone is present (else 'Unknown')"),
    ("store_zone_allowed", "stores", "Validity", WARNING, "zone in North/South/East/West"),
    ("store_pincode_6_digits", "stores", "Validity", WARNING, "pincode is 6 digits"),
    ("store_capacity_positive", "stores", "Accuracy", WARNING, "capacity_orders_per_day > 0"),
    ("store_consistent_area", "stores", "Consistency", WARNING, "store_name ends with its area"),
]
CHECK_META = {c[0]: {"file": c[1], "dimension": c[2], "severity": c[3], "description": c[4]} for c in CHECKS}


def checks_for(file_key: str) -> list[str]:
    return [c[0] for c in CHECKS if c[1] == file_key]


# ── DuckDB ───────────────────────────────────────────────────────────────────
def connect(path: Path | str, attempts: int = 60, wait_s: float = 2.0):
    """Open the warehouse, waiting if another task holds the single-writer lock."""
    last = None
    for _ in range(attempts):
        try:
            return duckdb.connect(str(path))
        except duckdb.IOException as e:          # "Could not set lock on file"
            last = e
            time.sleep(wait_s)
    raise last


AUDIT_DDL = """
CREATE TABLE IF NOT EXISTS dq_results (
    run_id        VARCHAR,
    dag_id        VARCHAR,
    check_name    VARCHAR,
    dimension     VARCHAR,
    severity      VARCHAR,
    rows_checked  BIGINT,
    rows_failed   BIGINT,
    status        VARCHAR,     -- PASS / WARN / FAIL
    checked_at    TIMESTAMP
);
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id            VARCHAR,
    dag_id            VARCHAR,
    start_time        TIMESTAMP,
    end_time          TIMESTAMP,
    rows_extracted    BIGINT,
    rows_loaded       BIGINT,
    rows_quarantined  BIGINT,
    status            VARCHAR
);
CREATE TABLE IF NOT EXISTS dq_check_catalog (
    check_name VARCHAR, file VARCHAR, dimension VARCHAR, severity VARCHAR, description VARCHAR
);
"""


def ensure_audit_tables(con) -> None:
    con.execute(AUDIT_DDL)
    con.execute("DELETE FROM dq_check_catalog")
    con.executemany("INSERT INTO dq_check_catalog VALUES (?, ?, ?, ?, ?)", [list(c) for c in CHECKS])


def dq_status(severity: str, rows_failed: int) -> str:
    if rows_failed == 0:
        return "PASS"
    return "FAIL" if severity == CRITICAL else "WARN"


def write_dq_results(con, run_id: str, dag_id: str, results: list[dict]) -> None:
    """results: [{check_name, rows_checked, rows_failed}]. Re-running a task replaces its rows."""
    ensure_audit_tables(con)
    now = datetime.now()
    rows = []
    for r in results:
        meta = CHECK_META[r["check_name"]]
        rows.append([run_id, dag_id, r["check_name"], meta["dimension"], meta["severity"],
                     int(r["rows_checked"]), int(r["rows_failed"]),
                     dq_status(meta["severity"], int(r["rows_failed"])), now])
    names = [r[2] for r in rows]
    if names:
        con.execute(
            "DELETE FROM dq_results WHERE run_id = ? AND dag_id = ? AND list_contains(?, check_name)",
            [run_id, dag_id, names],
        )
        con.executemany("INSERT INTO dq_results VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)


def previous_orders_volume(con, dag_id: str, run_id: str) -> int | None:
    """rows_checked of the last ord_volume check from an earlier run (volume pillar)."""
    try:
        r = con.execute(
            """SELECT rows_checked FROM dq_results
               WHERE dag_id = ? AND check_name = 'ord_volume' AND run_id <> ? AND rows_checked > 0
               ORDER BY checked_at DESC LIMIT 1""",
            [dag_id, run_id],
        ).fetchone()
        return int(r[0]) if r else None
    except duckdb.CatalogException:
        return None


def write_pipeline_run(con, row: dict) -> None:
    ensure_audit_tables(con)
    con.execute("DELETE FROM pipeline_runs WHERE run_id = ? AND dag_id = ?", [row["run_id"], row["dag_id"]])
    con.execute(
        "INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [row["run_id"], row["dag_id"], row["start_time"], row["end_time"], row["rows_extracted"],
         row["rows_loaded"], row["rows_quarantined"], row["status"]],
    )


# ── Masking (personal data) ─────────────────────────────────────────────────
def mask_email(email):
    """priya.hegde199@example.com -> pr***@example.com ; malformed -> pr*** ; missing -> None"""
    if email is None or str(email).strip().upper() in NULL_TOKENS:
        return None
    e = str(email).strip()
    if "@" in e:
        local, domain = e.split("@", 1)
        return f"{local[:2]}***@{domain}"
    return f"{e[:2]}***"


def mask_phone(phone):
    """9774349021 -> 97******21 (first 2 and last 2 digits kept)."""
    if phone is None or str(phone).strip() == "":
        return None
    p = re.sub(r"\D", "", str(phone))
    if len(p) <= 4:
        return "*" * len(p)
    return p[:2] + "*" * (len(p) - 4) + p[-2:]


def mask_name(name):
    """Priya Hegde -> Priya H."""
    if name is None or str(name).strip() == "":
        return None
    parts = str(name).split()
    return parts[0] if len(parts) == 1 else f"{parts[0]} {parts[-1][0]}."


# ── Alerting (bonus): Slack on failure, webhook from an Airflow Variable ────
def slack_on_failure(context) -> None:
    try:
        from airflow.sdk import Variable
        webhook = Variable.get("SLACK_WEBHOOK_URL", default=None)
    except Exception:
        webhook = None
    try:
        import sys
        sys.path.insert(0, str(INCLUDE_DIR))
        from slack_notifier import send_slack_alert
        ti = context.get("ti") or context.get("task_instance")
        msg = (f":red_circle: NammaMart pipeline failure\n"
               f"DAG: {ti.dag_id}  Task: {ti.task_id}  Try: {ti.try_number}\n"
               f"Run: {context.get('run_id')}\nError: {context.get('exception')}")
        send_slack_alert(msg, webhook_url=webhook)
    except Exception as e:  # alerting must never break the pipeline
        print(f"[Alert] could not send alert: {e}")
