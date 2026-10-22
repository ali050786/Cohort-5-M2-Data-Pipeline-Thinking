"""
include/nammamart_sikandar/etl_rules.py
---------------------------------------
Python (pandas) implementation of the data-quality rules for the ETL pipeline.

Every validate_* function receives the file exactly as extracted (all columns as text,
plus _source_file and _row_num) and returns:
    df       : the same rows + standardised typed columns + `critical_failures`,
               `warning_failures`, `failure_reason`, `dq_status` (PASS / WARN / FAIL)
    results  : [{check_name, rows_checked, rows_failed}] for dq_results

The SQL twin of every rule lives in include/sql/sikandar/staging/*.sql (ELT pipeline).
Check names are shared through common.CHECKS so both pipelines report the same scorecard.
"""

from __future__ import annotations

import pandas as pd

from nammamart_sikandar.common import (
    CHECK_META, CRITICAL, MAX_DELIVERY_MINUTES, NULL_TOKENS, PAYMENT_MODE_MAP,
    PRICE_DRIFT_RATIO, VALID_PAYMENT_MODES, VALID_PAYMENT_STATUS, VALID_STATUS,
    VALID_TIERS, VALID_ZONES, VOLUME_DROP_PCT, export_cutoff,
)

TS_FMT = "%Y-%m-%d %H:%M:%S"
DATE_FMT = "%Y-%m-%d"


# ── small helpers ────────────────────────────────────────────────────────────
def txt(s: pd.Series) -> pd.Series:
    """Trim, and turn '', 'N/A', 'NULL'... into missing."""
    s = s.astype("string").str.strip()
    return s.mask(s.str.upper().isin(NULL_TOKENS))


def num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(txt(s), errors="coerce")


def ts(s: pd.Series, fmt: str = TS_FMT) -> pd.Series:
    return pd.to_datetime(txt(s), format=fmt, errors="coerce")


def std_payment_mode(s: pd.Series) -> pd.Series:
    t = txt(s)
    mapped = t.str.upper().map(PAYMENT_MODE_MAP)
    return mapped.fillna(t)


def first_copy(df: pd.DataFrame, key: str, within: list[str] | None = None) -> pd.Series:
    """True for 2nd+ copies of a key (file order); the first copy is kept."""
    cols = (within or []) + [key]
    return df.sort_values("_row_num").duplicated(subset=cols, keep="first").reindex(df.index)


def _finish(df: pd.DataFrame, fails: dict[str, pd.Series], dataset_checks: dict | None = None):
    """Turn {check_name: bool Series} into reason columns + dq_results rows."""
    crit = pd.Series([[] for _ in range(len(df))], index=df.index, dtype=object)
    warn = pd.Series([[] for _ in range(len(df))], index=df.index, dtype=object)
    results = []
    for name, mask in fails.items():
        mask = mask.fillna(False).astype(bool)
        target = crit if CHECK_META[name]["severity"] == CRITICAL else warn
        for i in df.index[mask]:
            target.at[i] = target.at[i] + [name]
        results.append({"check_name": name, "rows_checked": len(df), "rows_failed": int(mask.sum())})
    for name, (checked, failed) in (dataset_checks or {}).items():
        results.append({"check_name": name, "rows_checked": checked, "rows_failed": failed})
    df["critical_failures"] = crit.apply(lambda l: "; ".join(l))
    df["warning_failures"] = warn.apply(lambda l: "; ".join(l))
    df["failure_reason"] = (crit + warn).apply(lambda l: "; ".join(l))
    df["dq_status"] = "PASS"
    df.loc[warn.apply(len) > 0, "dq_status"] = "WARN"
    df.loc[crit.apply(len) > 0, "dq_status"] = "FAIL"
    return df, results


# ── dimensions ───────────────────────────────────────────────────────────────
def validate_customers(df: pd.DataFrame):
    cutoff = pd.Timestamp(export_cutoff("customers.csv"))
    df = df.copy()
    df["customer_id_c"] = txt(df.customer_id)
    df["email_c"] = txt(df.email)
    df["phone_c"] = txt(df.phone)
    df["pincode_c"] = txt(df.pincode)
    df["loyalty_tier_c"] = txt(df.loyalty_tier).str.title()
    df["signup_date_c"] = ts(df.signup_date, DATE_FMT)
    fails = {
        "cust_customer_id_not_null": df.customer_id_c.isna(),
        "cust_customer_id_unique": first_copy(df, "customer_id_c") & df.customer_id_c.notna(),
        "cust_loyalty_tier_allowed": ~df.loyalty_tier_c.isin(VALID_TIERS),
        "cust_email_format": ~df.email_c.fillna("").str.match(r"^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$"),
        "cust_phone_10_digits": ~df.phone_c.fillna("").str.match(r"^[6-9][0-9]{9}$"),
        "cust_pincode_6_digits": ~df.pincode_c.fillna("").str.match(r"^[0-9]{6}$"),
        "cust_city_present": txt(df.city).isna(),
        "cust_loyalty_tier_casing": txt(df.loyalty_tier) != df.loyalty_tier_c,
        "cust_signup_not_future": df.signup_date_c > cutoff,
    }
    return _finish(df, fails)


def validate_products(df: pd.DataFrame):
    cutoff = pd.Timestamp(export_cutoff("products.csv"))
    df = df.copy()
    df["product_id_c"] = txt(df.product_id)
    df["mrp_c"] = num(df.mrp)
    df["cost_price_c"] = num(df.cost_price)
    df["launch_date_c"] = ts(df.launch_date, DATE_FMT)
    fails = {
        "prod_product_id_unique": first_copy(df, "product_id_c"),
        "prod_mrp_positive": ~(df.mrp_c > 0),
        "prod_cost_le_mrp": ~(df.cost_price_c <= df.mrp_c),
        "prod_launch_not_future": df.launch_date_c > cutoff,
        "prod_category_present": txt(df.category).isna(),
        "prod_is_perishable_valid": ~txt(df.is_perishable).isin(["True", "False"]),
        "prod_consistent_cost_by_name": txt(df.product_name).str.contains(r"\(old\)", regex=True),
    }
    return _finish(df, fails)


def validate_stores(df: pd.DataFrame):
    cutoff = pd.Timestamp(export_cutoff("stores.csv"))
    df = df.copy()
    df["store_id_c"] = txt(df.store_id)
    zone = txt(df.zone)
    area = txt(df.area)
    fails = {
        "store_store_id_unique": first_copy(df, "store_id_c"),
        "store_open_date_not_future": ts(df.open_date, DATE_FMT) > cutoff,
        "store_zone_present": zone.isna(),
        "store_zone_allowed": ~zone.isin(VALID_ZONES),
        "store_pincode_6_digits": ~txt(df.pincode).fillna("").str.match(r"^[0-9]{6}$"),
        "store_capacity_positive": ~(num(df.capacity_orders_per_day) > 0),
        "store_consistent_area": ~(txt(df.store_name) == ("NammaMart " + area)),
    }
    return _finish(df, fails)


# ── facts ────────────────────────────────────────────────────────────────────
def validate_orders(df, clean_customer_ids, clean_products, clean_stores, previous_volume=None):
    """
    df: orders.csv rows + any orders_<date>_increment.csv rows (column _source_file).
    clean_products: DataFrame[product_id, mrp] of products that passed critical checks.
    clean_stores: DataFrame[store_id, capacity_orders_per_day] of clean stores.
    """
    df = df.copy()
    df["cutoff"] = pd.to_datetime(df._source_file.map(export_cutoff))
    df["customer_id_c"] = txt(df.customer_id)
    df["store_id_c"] = txt(df.store_id)
    df["product_id_c"] = txt(df.product_id)
    df["order_ts_c"] = ts(df.order_ts)
    df["quantity_c"] = num(df.quantity)
    df["unit_price_c"] = num(df.unit_price)
    df["discount_pct_c"] = num(df.discount_pct)
    df["order_amount_c"] = num(df.order_amount)
    df["delivery_minutes_c"] = num(df.delivery_minutes)
    df["order_status_c"] = txt(df.order_status).str.upper()
    df["payment_mode_c"] = std_payment_mode(df.payment_mode)
    df["channel_c"] = txt(df.channel)

    expected = (df.quantity_c * df.unit_price_c * (1 - df.discount_pct_c / 100)).round(2)
    delivered = df.order_status_c == "DELIVERED"
    mrp = df.product_id_c.map(clean_products.set_index("product_id")["mrp"])
    ratio = df.unit_price_c / mrp

    fails = {
        "ord_customer_id_not_null": df.customer_id_c.isna(),
        "ord_delivered_has_minutes": delivered & df.delivery_minutes_c.isna(),
        "ord_order_id_unique": first_copy(df, "order_id", within=["_source_file"]),
        "ord_order_ts_valid": df.order_ts_c.isna(),
        "ord_status_allowed": ~df.order_status_c.isin(VALID_STATUS),
        "ord_payment_mode_allowed": ~df.payment_mode_c.isin(VALID_PAYMENT_MODES),
        "ord_discount_pct_range": ~df.discount_pct_c.between(0, 100),
        "ord_quantity_positive": ~(df.quantity_c > 0),
        "ord_amount_matches_formula": ~((expected - df.order_amount_c).abs() <= 0.01),
        "ord_delivery_minutes_lt_120": df.delivery_minutes_c >= MAX_DELIVERY_MINUTES,
        "ord_customer_fk": df.customer_id_c.notna() & ~df.customer_id_c.isin(clean_customer_ids),
        "ord_store_fk": ~df.store_id_c.isin(clean_stores.store_id),
        "ord_product_fk": ~df.product_id_c.isin(clean_products.product_id),
        "ord_order_ts_not_future": df.order_ts_c > df.cutoff,
        "ord_status_casing": txt(df.order_status) != df.order_status_c,
        "ord_payment_mode_casing": (txt(df.payment_mode) != df.payment_mode_c)
                                   & df.payment_mode_c.isin(VALID_PAYMENT_MODES),
        "ord_delivery_minutes_typical": delivered & ((df.delivery_minutes_c < 8) | (df.delivery_minutes_c > 35)),
        "ord_price_vs_mrp": (ratio > PRICE_DRIFT_RATIO) | (ratio < 1 / PRICE_DRIFT_RATIO),
    }

    # dataset-level checks (freshness, volume, store capacity)
    ds = {}
    base = df[df._source_file == "orders.csv"]
    latest = base.order_ts_c[base.order_ts_c <= base.cutoff].max() if len(base) else pd.NaT
    stale = int(pd.isna(latest) or latest < pd.Timestamp(export_cutoff("orders.csv")) - pd.Timedelta(days=1))
    ds["ord_freshness"] = (len(base), stale)
    dropped = int(previous_volume is not None and len(base) < previous_volume * (1 - VOLUME_DROP_PCT / 100))
    ds["ord_volume"] = (len(base), dropped)
    per_day = (df.assign(d=df.order_ts_c.dt.date).dropna(subset=["d"])
                 .drop_duplicates(["order_id"]).groupby(["store_id_c", "d"]).size().rename("n").reset_index())
    per_day = per_day.merge(clean_stores.rename(columns={"store_id": "store_id_c"}), on="store_id_c")
    over = int((per_day.n > per_day.capacity_orders_per_day.fillna(0)).sum())
    ds["ord_store_capacity"] = (len(per_day), over)

    out, results = _finish(df, fails, ds)
    return out, results, {"latest_order_ts": str(latest), "previous_volume": previous_volume}


def validate_payments(df: pd.DataFrame, order_ids_in_export, order_amounts: pd.Series):
    """order_amounts: Series indexed by order_id with the (latest) order_amount from the export."""
    cutoff = pd.Timestamp(export_cutoff("payments.csv"))
    df = df.copy()
    df["order_id_c"] = txt(df.order_id)
    df["payment_ts_c"] = ts(df.payment_ts)
    df["amount_paid_c"] = num(df.amount_paid)
    df["payment_status_c"] = txt(df.payment_status).str.upper()
    df["payment_mode_c"] = std_payment_mode(df.payment_mode)
    success = df.payment_status_c == "SUCCESS"
    order_amt = df.order_id_c.map(order_amounts)
    fails = {
        "pay_payment_id_unique": first_copy(df, "payment_id"),
        "pay_order_id_not_null": df.order_id_c.isna(),
        "pay_payment_ts_valid": df.payment_ts_c.isna(),
        "pay_status_allowed": ~df.payment_status_c.isin(VALID_PAYMENT_STATUS),
        "pay_mode_allowed": ~df.payment_mode_c.isin(VALID_PAYMENT_MODES),
        "pay_success_amount_positive": success & ~(df.amount_paid_c > 0),
        "pay_order_fk": ~df.order_id_c.isin(order_ids_in_export),
        "pay_payment_ts_not_future": df.payment_ts_c > cutoff,
        "pay_amount_matches_order": success & order_amt.notna() & ((df.amount_paid_c - order_amt).abs() > 0.01),
        "pay_gateway_ref_present": (df.payment_mode_c != "Cash on Delivery") & txt(df.gateway_ref).isna(),
        "pay_mode_casing": (txt(df.payment_mode) != df.payment_mode_c) & df.payment_mode_c.isin(VALID_PAYMENT_MODES),
    }
    return _finish(df, fails)
