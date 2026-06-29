from __future__ import annotations

import io
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import pytest

from bralpha.parsing.receita_tabular import (
    ReceitaUnsupportedFormatError,
    parse_receita_tabular_bytes,
)


def test_receita_csv_semicolon_parses_as_strings_and_preserves_raw_columns():
    bronze = _parse(
        "ANO;MÊS;Código Receita;Valor Arrecadado\n2024;1;001;10,5\n".encode(),
        raw_format="csv",
    )

    assert bronze["raw_codigo_receita"].to_list() == ["001"]
    assert bronze["raw_valor_arrecadado"].to_list() == ["10,5"]
    assert bronze["year"].to_list() == [2024]
    assert "raw_fields_json" not in bronze.columns


def test_receita_latin1_fixture_parses():
    bronze = _parse(
        "ANO;MÊS;Descrição;Valor\n2024;Março;Contribuição;11,0\n".encode("latin1"),
        raw_format="txt",
    )

    assert bronze["raw_descricao"].to_list() == ["Contribuição"]


def test_receita_zip_csv_preserves_inner_filename_and_lineage():
    payload = io.BytesIO()
    with zipfile.ZipFile(payload, "w") as archive:
        archive.writestr("historico/arrecadacao.csv", "ANO;MES;VALOR\n2024;1;10\n")

    bronze = _parse(payload.getvalue(), raw_format="zip")

    assert bronze["inner_filename"].to_list() == ["historico/arrecadacao.csv"]
    assert bronze["resource_name"].to_list() == ["resultado-arrecadacao"]
    assert bronze["source"].to_list() == ["receita"]


def test_receita_xlsx_without_dependency_raises_clear_error():
    with pytest.raises(ReceitaUnsupportedFormatError, match="XLSX/ODS"):
        _parse(b"not-real-xlsx", raw_format="xlsx")


def _parse(content: bytes, *, raw_format: str):
    return parse_receita_tabular_bytes(
        content,
        raw_format=raw_format,
        source_dataset="receita_tax_collection_monthly",
        resource_name="resultado-arrecadacao",
        resource_family="tax_collection_monthly",
        download_timestamp_utc=datetime(2024, 3, 8, 12, tzinfo=UTC),
        raw_path=Path("raw.csv"),
        sha256="abc",
    )
