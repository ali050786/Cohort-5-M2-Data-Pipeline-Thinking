"""
include/nammamart_sikandar/elt.py
---------------------------------
Helpers for the ELT pipeline: load a CSV into a raw table exactly as received (DuckDB only,
no pandas), keep a manifest of loaded files, and run the .sql files in include/sql/sikandar/.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

from nammamart_sikandar import common as C

SQL_PARAMS = {
    "default_cutoff": C.DEFAULT_EXPORT_CUTOFF,
    "max_delivery_minutes": str(C.MAX_DELIVERY_MINUTES),
    "price_drift_ratio": str(C.PRICE_DRIFT_RATIO),
    "volume_drop_pct": str(C.VOLUME_DROP_PCT),
    "sla_minutes": str(C.SLA_MINUTES),
}

MANIFEST_DDL = """
CREATE TABLE IF NOT EXISTS raw_file_manifest (
    load_id      VARCHAR,
    source_file  VARCHAR,
    file_hash    VARCHAR,
    rows_loaded  BIGINT,
    run_id       VARCHAR,
    loaded_at    TIMESTAMP
);
"""


def render(sql: str, **params) -> str:
    values = {**SQL_PARAMS, **params}
    for k, v in values.items():
        sql = sql.replace("{{" + k + "}}", str(v).replace("'", "''"))
    return sql


def run_sql(con, rel_path: str, **params) -> None:
    """Execute include/sql/sikandar/<rel_path> (macros first)."""
    macros = (C.SQL_DIR / "00_macros.sql").read_text()
    con.execute(render(macros, **params))
    sql = (C.SQL_DIR / rel_path).read_text()
    print(f"[SQL] running {rel_path}")
    con.execute(render(sql, **params))


def file_md5(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def load_raw(con, path: Path, file_key: str, run_id: str) -> dict:
    """
    Append the file to raw_<file_key> with _loaded_at, _source_file, _run_id, _row_num, _load_id.
    All columns are stored as text. A file already loaded with identical content is skipped,
    so re-runs never duplicate raw rows; a changed file is appended as a new load.
    """
    from airflow.exceptions import AirflowFailException

    if not path.exists():
        raise FileNotFoundError(f"[Load raw] source file missing: {path}")
    name = path.name
    con.execute(MANIFEST_DDL)
    digest = file_md5(path)
    seen = con.execute("SELECT load_id FROM raw_file_manifest WHERE source_file = ? AND file_hash = ?",
                       [name, digest]).fetchone()
    if seen:
        print(f"[Load raw] {name}: identical file already loaded as {seen[0]} - nothing appended")
        return {"file": name, "rows": 0, "skipped": True, "load_id": seen[0]}

    p = str(path).replace("'", "''")
    reader = f"read_csv('{p}', all_varchar = true, header = true, delim = ',', quote = '\"')"
    cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {reader}").fetchall()]
    expected = C.SCHEMAS[file_key]
    if cols != expected:
        raise AirflowFailException(
            f"[Schema] {name}: columns changed. missing={sorted(set(expected) - set(cols))} "
            f"unexpected={sorted(set(cols) - set(expected))}")

    table = f"raw_{file_key}"
    col_defs = ", ".join(f'"{c}" VARCHAR' for c in expected)
    con.execute(f"""CREATE TABLE IF NOT EXISTS {table} ({col_defs}, _loaded_at TIMESTAMP,
                    _source_file VARCHAR, _run_id VARCHAR, _row_num BIGINT, _load_id VARCHAR)""")
    load_id = f"{name}:{digest[:10]}:{run_id}"
    con.execute("SET threads = 1")            # keep file order for _row_num
    con.execute(f"""
        INSERT INTO {table}
        SELECT *, now(), ?, ?, row_number() OVER (), ? FROM {reader}""", [name, run_id, load_id])
    rows = con.execute(f"SELECT count(*) FROM {table} WHERE _load_id = ?", [load_id]).fetchone()[0]
    con.execute("INSERT INTO raw_file_manifest VALUES (?, ?, ?, ?, ?, now())", [load_id, name, digest, rows, run_id])
    print(f"[Load raw] {name}: {rows} rows appended to {table} (load_id={load_id})")
    return {"file": name, "rows": int(rows), "skipped": False, "load_id": load_id}
