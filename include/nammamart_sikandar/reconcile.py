"""
include/nammamart_sikandar/reconcile.py
---------------------------------------
Order <-> payment-gateway reconciliation for the ETL pipeline (pandas).
SQL twin: include/sql/sikandar/mart/mart_payment_reconciliation.sql

One output row per mismatch. difference = what the gateway shows - what the order book expects.

  DUPLICATE_PAYMENT          same payment_id sent twice by the gateway export
  PAYMENT_WITHOUT_ORDER      gateway has a payment for an order_id that is not in the order export
  PAYMENT_FOR_REJECTED_ORDER order exists in the export but failed a critical quality check
  AMOUNT_MISMATCH            SUCCESS payment amount differs from order_amount
  PAYMENT_MODE_MISMATCH      gateway mode differs from the order's payment_mode
  DELIVERED_NOT_PAID         DELIVERED order with no payment or only a FAILED payment
  RETURNED_NOT_REFUNDED      RETURNED order whose payment was not REFUNDED
  REFUND_WITHOUT_RETURN      REFUNDED payment for an order that is not RETURNED
  PAID_BUT_CANCELLED         SUCCESS payment for a CANCELLED order
"""

from __future__ import annotations

import warnings

import pandas as pd

from nammamart_sikandar.etl_rules import num, std_payment_mode, txt

COLS = ["order_id", "payment_id", "order_status", "order_amount", "payment_status",
        "amount_paid", "difference", "mismatch_reason"]


def reconcile(orders: pd.DataFrame, payments_raw: pd.DataFrame, export_order_ids: set) -> pd.DataFrame:
    p = pd.DataFrame({
        "payment_id": txt(payments_raw.payment_id),
        "order_id": txt(payments_raw.order_id),
        "payment_status": txt(payments_raw.payment_status).str.upper(),
        "pay_mode": std_payment_mode(payments_raw.payment_mode),
        "amount_paid": num(payments_raw.amount_paid),
        "_row_num": payments_raw._row_num,
    }).sort_values("_row_num")
    p = p.astype(object).where(p.notna(), None)          # plain None/NaN semantics, like SQL NULL
    p["amount_paid"] = p.amount_paid.astype(float)
    dup = p[p.duplicated("payment_id", keep="first")]
    p = p.drop_duplicates("payment_id", keep="first")

    o = orders.rename(columns={"payment_mode": "order_mode"}).astype({"order_status": object, "order_mode": object})
    m = o.merge(p, on="order_id", how="outer", indicator=True)
    out = []

    def add(df, reason, diff):
        if len(df):
            d = df.assign(mismatch_reason=reason, difference=diff(df).round(2))
            out.append(d.reindex(columns=COLS))

    add(dup.assign(order_status=None, order_amount=None), "DUPLICATE_PAYMENT", lambda d: d.amount_paid)
    pay_only = m[m._merge == "right_only"]
    add(pay_only[~pay_only.order_id.isin(export_order_ids)], "PAYMENT_WITHOUT_ORDER", lambda d: d.amount_paid)
    add(pay_only[pay_only.order_id.isin(export_order_ids)], "PAYMENT_FOR_REJECTED_ORDER", lambda d: d.amount_paid)

    both = m[m._merge != "right_only"]
    succ = (both.payment_status == "SUCCESS").fillna(False).astype(bool)
    add(both[succ & ((both.amount_paid - both.order_amount).abs() > 0.01)], "AMOUNT_MISMATCH",
        lambda d: d.amount_paid - d.order_amount)
    add(both[both.payment_id.notna() & (both.pay_mode != both.order_mode)], "PAYMENT_MODE_MISMATCH",
        lambda d: d.amount_paid * 0)
    add(both[(both.order_status == "DELIVERED") & ~succ], "DELIVERED_NOT_PAID",
        lambda d: d.amount_paid.where(d.payment_status == "SUCCESS", 0).fillna(0) - d.order_amount)
    add(both[(both.order_status == "RETURNED") & (both.payment_status != "REFUNDED")], "RETURNED_NOT_REFUNDED",
        lambda d: d.amount_paid.where(d.payment_status == "SUCCESS", 0).fillna(0))
    add(both[(both.payment_status == "REFUNDED") & (both.order_status != "RETURNED")], "REFUND_WITHOUT_RETURN",
        lambda d: -d.amount_paid)
    add(both[(both.order_status == "CANCELLED") & succ], "PAID_BUT_CANCELLED", lambda d: d.amount_paid)

    if not out:
        return pd.DataFrame(columns=COLS)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
        res = pd.concat(out, ignore_index=True)
    res["order_amount"] = res.order_amount.astype(float)
    res["amount_paid"] = res.amount_paid.astype(float)
    res["difference"] = res.difference.astype(float)
    return res.sort_values(["mismatch_reason", "order_id"]).reset_index(drop=True)
