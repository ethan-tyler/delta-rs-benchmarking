#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Sequence


def _load_duckdb_module() -> Any:
    import duckdb

    return duckdb


def _validate_scale_factor(raw: str) -> str:
    try:
        value = float(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"invalid scale factor '{raw}': {exc}"
        ) from exc
    if value <= 0:
        raise argparse.ArgumentTypeError(f"invalid scale factor '{raw}': must be > 0")
    if value.is_integer():
        return str(int(value))
    return raw


def _escape_sql_literal(path: Path) -> str:
    return str(path).replace("'", "''")


def _build_export_sql(output_csv: Path) -> str:
    output = _escape_sql_literal(output_csv)
    return f"""
COPY (
    SELECT
        CAST(COALESCE(ss_customer_sk, 0) AS BIGINT) AS ss_customer_sk,
        CAST(COALESCE(ss_ext_sales_price, 0.0) AS DOUBLE) AS ss_ext_sales_price,
        CAST(COALESCE(ss_item_sk, 0) AS BIGINT) AS ss_item_sk,
        CAST(COALESCE(ss_quantity, 0) AS BIGINT) AS ss_quantity,
        CAST(COALESCE(ss_sold_date_sk, 0) AS BIGINT) AS ss_sold_date_sk
    FROM store_sales
) TO '{output}' (HEADER, DELIMITER ',');
""".strip()


def _initialize_tpcds_extension(connection: Any) -> None:
    try:
        connection.execute("LOAD tpcds;")
        return
    except Exception as load_error:
        try:
            connection.execute("INSTALL tpcds;")
            connection.execute("LOAD tpcds;")
            return
        except Exception as install_error:
            raise RuntimeError(
                "failed to initialize DuckDB TPC-DS extension. "
                "Attempted `LOAD tpcds;` first, then fallback `INSTALL tpcds; LOAD tpcds;`. "
                f"load error: {load_error}; fallback error: {install_error}"
            ) from install_error


def _generate_store_sales_csv(
    duckdb_module: Any, scale_factor: str, output_csv: Path
) -> None:
    connection = duckdb_module.connect()
    _initialize_tpcds_extension(connection)

    try:
        connection.execute(f"CALL dsdgen(sf={scale_factor});")
    except Exception as exc:  # pragma: no cover - exercised by tests via fake duckdb
        raise RuntimeError(
            f"DuckDB dsdgen failed for scale factor {scale_factor}: {exc}"
        ) from exc

    try:
        connection.execute(_build_export_sql(output_csv))
    except Exception as exc:  # pragma: no cover - exercised by tests via fake duckdb
        raise RuntimeError(
            f"failed to export store_sales CSV to '{output_csv}': {exc}"
        ) from exc


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate TPC-DS store_sales CSV with DuckDB dsdgen"
    )
    parser.add_argument("--scale-factor", required=True, type=_validate_scale_factor)
    parser.add_argument("--output-csv", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    output_csv = Path(args.output_csv)
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    try:
        duckdb_module = _load_duckdb_module()
    except ImportError as exc:
        print(
            "duckdb Python package is required; install with `pip install duckdb`. "
            f"Import error: {exc}",
            file=sys.stderr,
        )
        return 2

    try:
        _generate_store_sales_csv(duckdb_module, args.scale_factor, output_csv)
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1

    if not output_csv.exists():
        print(
            f"DuckDB generation succeeded but output CSV was not created: '{output_csv}'",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
