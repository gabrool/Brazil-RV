from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from bralpha.parsing.tesouro_tabular import TESOURO_BRONZE_BASE_COLUMNS, parse_tesouro_bytes


def test_tesouro_csv_parsing_preserves_order_raw_fields_and_unknown_columns():
    frame = parse_tesouro_bytes(
        (
            "Data Base;Tipo Titulo;Taxa Compra Manha;Coluna Nova\n"
            "02/01/2024;Tesouro Prefixado;10,5;abc\n"
            "03/01/2024;Tesouro IPCA+;5,5;xyz\n"
        ).encode("latin1"),
        raw_format="csv",
        source_dataset="tesouro_direto_prices_rates",
        download_timestamp_utc=datetime(2024, 1, 2, 12, tzinfo=UTC),
        raw_path=Path("raw.csv"),
        sha256="abc",
        resource_name="Taxas dos Titulos Ofertados",
    )

    assert frame.columns[: len(TESOURO_BRONZE_BASE_COLUMNS)] == TESOURO_BRONZE_BASE_COLUMNS
    assert frame["row_index"].to_list() == [0, 1]
    assert frame["resource_name"].to_list() == [
        "Taxas dos Titulos Ofertados",
        "Taxas dos Titulos Ofertados",
    ]
    assert frame["raw_taxa_compra_manha"].to_list() == ["10,5", "5,5"]
    assert frame["raw_coluna_nova"].to_list() == ["abc", "xyz"]
    assert json.loads(frame["raw_fields_json"].item(0)) == {
        "Coluna Nova": "abc",
        "Data Base": "02/01/2024",
        "Taxa Compra Manha": "10,5",
        "Tipo Titulo": "Tesouro Prefixado",
    }
