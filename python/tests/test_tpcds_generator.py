from __future__ import annotations

import re
from pathlib import Path
from types import SimpleNamespace

from delta_bench_tpcds import generate_store_sales_csv as generator


class _FakeConnection:
    def __init__(
        self, *, fail_on: str | None = None, fail_once_on: str | None = None
    ) -> None:
        self.commands: list[str] = []
        self.fail_on = fail_on
        self.fail_once_on = fail_once_on
        self._failed_once = False

    def execute(self, sql: str) -> "_FakeConnection":
        self.commands.append(sql)
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError(f"forced failure for {self.fail_on}")
        if (
            self.fail_once_on
            and not self._failed_once
            and self.fail_once_on in sql
        ):
            self._failed_once = True
            raise RuntimeError(f"forced one-time failure for {self.fail_once_on}")
        if "COPY (" in sql:
            match = re.search(r"TO '(.+)' \(", sql)
            if not match:
                raise RuntimeError(f"missing COPY target path in SQL: {sql}")
            csv_path = Path(match.group(1).replace("''", "'"))
            csv_path.parent.mkdir(parents=True, exist_ok=True)
            csv_path.write_text(
                "ss_customer_sk,ss_ext_sales_price,ss_item_sk,ss_quantity,ss_sold_date_sk\n"
                "1,10.0,100,2,2450815\n",
                encoding="utf-8",
            )
        return self


def test_generator_runs_duckdb_contract_and_writes_csv(
    tmp_path: Path, monkeypatch
) -> None:
    fake_connection = _FakeConnection()
    fake_duckdb = SimpleNamespace(connect=lambda: fake_connection)
    monkeypatch.setattr(generator, "_load_duckdb_module", lambda: fake_duckdb)

    output_csv = tmp_path / "store_sales.csv"
    code = generator.main(
        ["--scale-factor", "1", "--output-csv", str(output_csv)],
    )

    assert code == 0
    assert output_csv.exists()
    assert fake_connection.commands[0].strip().upper().startswith("LOAD TPCDS")
    assert all("INSTALL tpcds" not in cmd for cmd in fake_connection.commands)
    assert "CALL dsdgen(sf=1)" in fake_connection.commands[1]
    copy_sql = fake_connection.commands[2]
    assert "COALESCE(ss_customer_sk, 0)" in copy_sql
    assert "COALESCE(ss_ext_sales_price, 0.0)" in copy_sql
    assert "CAST(COALESCE(ss_item_sk, 0) AS BIGINT)" in copy_sql
    assert "CAST(COALESCE(ss_quantity, 0) AS BIGINT)" in copy_sql
    assert "CAST(COALESCE(ss_sold_date_sk, 0) AS BIGINT)" in copy_sql


def test_generator_falls_back_to_install_when_load_fails_once(
    tmp_path: Path, monkeypatch
) -> None:
    fallback_connection = _FakeConnection(fail_once_on="LOAD tpcds")
    fake_duckdb = SimpleNamespace(connect=lambda: fallback_connection)
    monkeypatch.setattr(generator, "_load_duckdb_module", lambda: fake_duckdb)

    output_csv = tmp_path / "store_sales.csv"
    code = generator.main(["--scale-factor", "1", "--output-csv", str(output_csv)])

    assert code == 0
    assert output_csv.exists()
    assert fallback_connection.commands[0].strip().upper().startswith("LOAD TPCDS")
    assert fallback_connection.commands[1].strip().upper().startswith("INSTALL TPCDS")
    assert fallback_connection.commands[2].strip().upper().startswith("LOAD TPCDS")
    assert "CALL dsdgen(sf=1)" in fallback_connection.commands[3]


def test_generator_reports_missing_duckdb_dependency(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    def _raise_import_error() -> None:
        raise ImportError("No module named duckdb")

    monkeypatch.setattr(generator, "_load_duckdb_module", _raise_import_error)
    code = generator.main(
        ["--scale-factor", "1", "--output-csv", str(tmp_path / "out.csv")]
    )

    assert code != 0
    stderr = capsys.readouterr().err
    assert "duckdb Python package is required" in stderr
    assert "pip install duckdb" in stderr


def test_generator_reports_tpcds_extension_failures(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    failing_connection = _FakeConnection(fail_on="LOAD tpcds")
    fake_duckdb = SimpleNamespace(connect=lambda: failing_connection)
    monkeypatch.setattr(generator, "_load_duckdb_module", lambda: fake_duckdb)

    code = generator.main(
        ["--scale-factor", "1", "--output-csv", str(tmp_path / "out.csv")]
    )
    assert code != 0
    stderr = capsys.readouterr().err
    assert "failed to initialize DuckDB TPC-DS extension" in stderr
    assert "LOAD tpcds" in stderr
