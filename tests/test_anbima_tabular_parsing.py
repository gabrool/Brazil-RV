from __future__ import annotations

from datetime import UTC, datetime

from bralpha.parsing.anbima_tabular import parse_anbima_bytes


def test_anbima_json_parser_preserves_raw_fields_and_unknowns(repo_root):
    bronze = parse_anbima_bytes(
        b'[{"Data Referencia":"2024-01-02","Taxa":"10.5","Campo Novo":"abc"}]',
        raw_format="json",
        source_dataset="anbima_sovereign_yield_curves",
        download_timestamp_utc=datetime(2024, 1, 2, 12, tzinfo=UTC),
        raw_path=repo_root / "raw.json",
        sha256="abc",
    )

    row = bronze.row(0, named=True)
    assert row["row_index"] == 0
    assert '"Campo Novo":"abc"' in row["raw_fields_json"]
    assert row["raw_data_referencia"] == "2024-01-02"
    assert row["raw_taxa"] == "10.5"
    assert row["source"] == "anbima"


def test_anbima_csv_parser_preserves_row_order(repo_root):
    bronze = parse_anbima_bytes(
        b"Data,Valor,Extra\n2024-01-02,1,A\n2024-01-03,2,B\n",
        raw_format="csv",
        source_dataset="anbima_fixed_income_indices",
        download_timestamp_utc=datetime(2024, 1, 2, 12, tzinfo=UTC),
        raw_path=repo_root / "raw.csv",
        sha256="abc",
    )

    assert bronze["row_index"].to_list() == [0, 1]
    assert bronze["raw_valor"].to_list() == ["1", "2"]
    assert '"Extra":"B"' in bronze["raw_fields_json"].to_list()[1]


def test_anbima_txt_semicolon_parser_preserves_row_order(repo_root):
    bronze = parse_anbima_bytes(
        b"Data;Indicador;Projecao\n02/01/2024;IPCA;4,5\n03/01/2024;IGP-M;3,2\n",
        raw_format="txt_semicolon",
        source_dataset="anbima_inflation_projections",
        download_timestamp_utc=datetime(2024, 1, 2, 12, tzinfo=UTC),
        raw_path=repo_root / "raw.txt",
        sha256="abc",
    )

    assert bronze["row_index"].to_list() == [0, 1]
    assert bronze["raw_indicador"].to_list() == ["IPCA", "IGP-M"]
    assert '"Projecao":"4,5"' in bronze["raw_fields_json"].to_list()[0]
