"""
include/experiments_sikandar/make_experiments.py
------------------------------------------------
Builds the input folders for the Part F observability experiments WITHOUT touching the
original files in include/data/nammamart/. Each experiment gets:

    include/data/experiments/expNN/        modified copies of the six source files
    include/experiments_sikandar/wh/expNN.duckdb   a copy of the main warehouse (so history,
                                                   e.g. the previous volume, is available)

Trigger a DAG on an experiment with conf, for example:
    astro dev run dags trigger nammamart_etl_sikandar \
        -c '{"data_dir": "data/experiments/exp05", "warehouse": "experiments_sikandar/wh/exp05.duckdb"}'

Usage:  python include/experiments_sikandar/make_experiments.py
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pandas as pd

INCLUDE = Path(__file__).resolve().parents[1]
SRC = INCLUDE / "data" / "nammamart"
OUT = INCLUDE / "data" / "experiments"
WH_SRC = INCLUDE / "warehouse_sikandar.duckdb"
WH_OUT = INCLUDE / "experiments_sikandar" / "wh"


def fresh(name: str) -> Path:
    d = OUT / name
    if d.exists():
        shutil.rmtree(d)
    shutil.copytree(SRC, d)
    WH_OUT.mkdir(parents=True, exist_ok=True)
    if WH_SRC.exists():
        shutil.copy(WH_SRC, WH_OUT / f"{name}.duckdb")
    return d


def orders(d: Path) -> pd.DataFrame:
    return pd.read_csv(d / "orders.csv", dtype=str, keep_default_na=False)


def save(df: pd.DataFrame, d: Path, name="orders.csv"):
    df.to_csv(d / name, index=False)


def main():
    # 4. missing source file
    d = fresh("exp04")
    (d / "products.csv").unlink()

    # 5. five new bad rows
    d = fresh("exp05")
    o = orders(d)
    base = dict(order_ts="2026-09-30 12:00:00", customer_id="C00001", store_id="S01", product_id="P0001",
                quantity="1", unit_price="107.0", discount_pct="0", order_amount="107.0", payment_mode="UPI",
                order_status="DELIVERED", channel="App", delivery_minutes="12")
    bad = [
        dict(base, order_id="ORD900001", customer_id=""),                                        # null customer
        dict(base, order_id="ORD900002", quantity="-2", order_amount="-214.0"),                  # negative quantity
        dict(base, order_id="ORD900003", store_id="S77"),                                        # unknown store
        dict(base, order_id="ORD900004", order_status="SHIPPED"),                                # invalid status
        dict(base, order_id="ORD900005", order_ts="2027-03-15 10:00:00"),                        # 2027 timestamp
    ]
    save(pd.concat([o, pd.DataFrame(bad)], ignore_index=True), d)

    # 6. schema change: order_amount renamed to total
    d = fresh("exp06")
    save(orders(d).rename(columns={"order_amount": "total"}), d)

    # 7. volume anomaly: only 50 orders
    d = fresh("exp07")
    save(orders(d).head(50), d)

    # 8. distribution drift: unit_price x100 for one store (S05)
    d = fresh("exp08")
    o = orders(d)
    m = o.store_id == "S05"
    o.loc[m, "unit_price"] = (pd.to_numeric(o.loc[m, "unit_price"]) * 100).round(2).astype(str)
    save(o, d)

    # 9. freshness: every order 30 days older (latest order is ~30 days before the cut-off)
    d = fresh("exp09")
    o = orders(d)
    t = pd.to_datetime(o.order_ts, format="%Y-%m-%d %H:%M:%S", errors="coerce")
    o.loc[t.notna(), "order_ts"] = (t[t.notna()] - pd.Timedelta(days=30)).dt.strftime("%Y-%m-%d %H:%M:%S")
    save(o, d)
    print(f"experiment inputs written under {OUT}")


if __name__ == "__main__":
    main()
