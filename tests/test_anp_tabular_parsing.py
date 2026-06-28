from __future__ import annotations

import io
import zipfile
from datetime import UTC, datetime
from pathlib import Path

from bralpha.parsing.anp_tabular import parse_anp_tabular_bytes


def test_anp_parser_reads_semicolon_csv_as_strings():
    content = (
        "ANO;MÊS;GRANDE REGIÃO;UNIDADE DA FEDERAÇÃO;PRODUTO;VENDAS\n"
        "2024;Janeiro;Sudeste;São Paulo;GASOLINA C;001,50\n"
    ).encode()

    frame = parse_anp_tabular_bytes(
        content,
        raw_format="csv",
        source_dataset="anp_fuel_sales_monthly",
        resource_name="sales",
        resource_family="sales",
        download_timestamp_utc=datetime(2024, 2, 5, 12, tzinfo=UTC),
        raw_path=Path("raw.csv"),
        sha256="abc",
    )

    assert frame["raw_vendas"].to_list() == ["001,50"]
    assert frame["raw_unidade_da_federacao"].to_list() == ["São Paulo"]
    assert frame["year"].to_list() == [2024]
    assert frame["month"].to_list() == [1]
    assert "raw_fields_json" not in frame.columns


def test_anp_parser_falls_back_to_latin1_and_preserves_raw_columns():
    content = (
        "Regiao - Sigla;Estado - Sigla;Município;Revenda\n"
        "SE;SP;São Paulo;Posto A\n"
    ).encode("latin1")

    frame = parse_anp_tabular_bytes(
        content,
        raw_format="csv",
        source_dataset="anp_fuel_prices_weekly",
        resource_name="prices",
        resource_family="diesel_gnv_monthly",
        download_timestamp_utc=datetime(2024, 2, 5, 12, tzinfo=UTC),
        raw_path=Path("prices.csv"),
        sha256="abc",
        year=2024,
        month=1,
    )

    assert frame["raw_municipio"].to_list() == ["São Paulo"]
    assert frame["resource_family"].to_list() == ["diesel_gnv_monthly"]
    assert frame["row_index"].to_list() == [0]


def test_anp_parser_zip_csv_members_preserve_inner_filename_and_lineage():
    content = _zip_bytes(
        {
            "b.csv": "ANO;MÊS;PRODUTO;VENDAS\n2024;Fevereiro;GLP;2\n",
            "a.csv": "ANO;MÊS;PRODUTO;VENDAS\n2024;Janeiro;GLP;1\n",
        }
    )

    frame = parse_anp_tabular_bytes(
        content,
        raw_format="zip_csv",
        source_dataset="anp_fuel_sales_monthly",
        resource_name="sales",
        resource_family="sales",
        download_timestamp_utc=datetime(2024, 2, 5, 12, tzinfo=UTC),
        raw_path=Path("sales.zip"),
        sha256="abc",
    )

    assert frame["inner_filename"].to_list() == ["a.csv", "b.csv"]
    assert frame["raw_vendas"].to_list() == ["1", "2"]
    assert frame["source"].to_list() == ["anp", "anp"]
    assert frame["raw_path"].to_list() == ["sales.zip", "sales.zip"]


def test_anp_parser_mixed_csv_zip_accepts_plain_csv():
    content = "ANO,MÊS,PRODUTO,VENDAS\n2024,Janeiro,GLP,2\n".encode()

    frame = parse_anp_tabular_bytes(
        content,
        raw_format="mixed_csv_zip",
        source_dataset="anp_fuel_prices_weekly",
        resource_name="prices",
        resource_family="glp_monthly",
        download_timestamp_utc=datetime(2024, 2, 5, 12, tzinfo=UTC),
        raw_path=Path("prices.csv"),
        sha256="abc",
    )

    assert frame["raw_produto"].to_list() == ["GLP"]
    assert frame["month"].to_list() == [1]


def _zip_bytes(files: dict[str, str]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, text in files.items():
            archive.writestr(name, text.encode("latin1"))
    return buffer.getvalue()
