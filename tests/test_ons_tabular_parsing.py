from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from bralpha.parsing.ons_tabular import parse_ons_tabular_bytes


def test_ons_parser_reads_delimited_csv_as_strings_and_preserves_lineage(tmp_path):
    raw_path = tmp_path / "ONS.csv"
    content = (
        b"id_subsistema;nom_subsistema;ear_data;valor\n"
        b"SE;Sudeste;2024-01-01;1,5\n"
        b"NE;Nordeste;2024-01-02;2,5\n"
    )

    frame = parse_ons_tabular_bytes(
        content,
        raw_format="csv_annual",
        source_dataset="ons_ear_subsystem_daily",
        resource_name="EAR-2024",
        year=2024,
        download_timestamp_utc=datetime(2024, 1, 3, 12, tzinfo=UTC),
        raw_path=raw_path,
        sha256="abc",
    )

    assert frame["row_index"].to_list() == [0, 1]
    assert frame["raw_valor"].to_list() == ["1,5", "2,5"]
    assert frame["source"].to_list() == ["ons", "ons"]
    assert frame["resource_name"].to_list() == ["EAR-2024", "EAR-2024"]
    assert "raw_fields_json" not in frame.columns


def test_ons_parser_supports_latin1_and_normalized_raw_columns(tmp_path):
    content = "nom_subsistema;descrição\nSE;São Paulo\n".encode("latin1")

    frame = parse_ons_tabular_bytes(
        content,
        raw_format="csv_annual",
        source_dataset="ons_load_daily",
        resource_name="LOAD-2024",
        year=2024,
        download_timestamp_utc=datetime(2024, 1, 3, 12, tzinfo=UTC),
        raw_path=Path("load.csv"),
        sha256="def",
    )

    assert "raw_descricao" in frame.columns
    assert frame["raw_descricao"].to_list() == ["São Paulo"]
