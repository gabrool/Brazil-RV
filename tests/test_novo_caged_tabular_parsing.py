from __future__ import annotations

import io
from datetime import UTC, datetime
from pathlib import Path

import py7zr

from bralpha.parsing.novo_caged_tabular import parse_novo_caged_tabular_bytes


def test_novo_caged_parser_reads_txt_as_strings_and_preserves_lineage():
    content = "competenciamov;uf;município;salário\n202401;SP;São Paulo;001,50\n".encode()

    frame = parse_novo_caged_tabular_bytes(
        content,
        raw_format="txt",
        source_dataset="novo_caged_movements_monthly",
        resource_name="movement_records-202401",
        download_timestamp_utc=datetime(2024, 3, 5, 12, tzinfo=UTC),
        raw_path=Path("CAGEDMOV202401.txt"),
        sha256="abc",
        period="202401",
        year=2024,
        month=1,
        record_kind="movement",
    )

    assert frame["raw_salario"].to_list() == ["001,50"]
    assert frame["raw_municipio"].to_list() == ["São Paulo"]
    assert frame["period"].to_list() == ["202401"]
    assert frame["year"].to_list() == [2024]
    assert "raw_fields_json" not in frame.columns


def test_novo_caged_parser_falls_back_to_latin1_and_normalizes_raw_columns():
    content = "competência;UF;Município\n202402;RJ;Niterói\n".encode("latin1")

    frame = parse_novo_caged_tabular_bytes(
        content,
        raw_format="txt",
        source_dataset="novo_caged_movements_monthly",
        resource_name="movement_records-202402",
        download_timestamp_utc=datetime(2024, 3, 5, 12, tzinfo=UTC),
        raw_path=Path("CAGEDMOV202402.txt"),
        sha256="abc",
    )

    assert frame["raw_competencia"].to_list() == ["202402"]
    assert frame["raw_municipio"].to_list() == ["Niterói"]
    assert frame["year"].to_list() == [2024]
    assert frame["month"].to_list() == [2]


def test_novo_caged_parser_extracts_7z_txt_member_and_inner_filename():
    content = _seven_zip_bytes(
        {
            "CAGEDMOV202401.txt": (
                "competenciamov;uf;município;saldomovimentação\n"
                "202401;SP;São Paulo;1\n"
            )
        }
    )

    frame = parse_novo_caged_tabular_bytes(
        content,
        raw_format="7z_txt",
        source_dataset="novo_caged_movements_monthly",
        resource_name="movement_records-202401",
        download_timestamp_utc=datetime(2024, 3, 5, 12, tzinfo=UTC),
        raw_path=Path("CAGEDMOV202401.7z"),
        sha256="abc",
        period="202401",
        year=2024,
        month=1,
        record_kind="movement",
    )

    assert frame["inner_filename"].to_list() == ["CAGEDMOV202401.txt"]
    assert frame["raw_saldomovimentacao"].to_list() == ["1"]
    assert frame["source"].to_list() == ["novo_caged"]


def test_novo_caged_calendar_html_preserves_raw_text():
    content = "<html><li>30/06/2026 - Competência: maio de 2026;</li></html>".encode()

    frame = parse_novo_caged_tabular_bytes(
        content,
        raw_format="html",
        source_dataset="novo_caged_release_calendar",
        resource_name="official_release_calendar",
        download_timestamp_utc=datetime(2026, 1, 5, 12, tzinfo=UTC),
        raw_path=Path("calendar.html"),
        sha256="abc",
    )

    assert frame.height == 1
    assert "maio de 2026" in frame["raw_text"][0]
    assert "raw_fields_json" not in frame.columns


def _seven_zip_bytes(files: dict[str, str]) -> bytes:
    buffer = io.BytesIO()
    with py7zr.SevenZipFile(buffer, "w") as archive:
        for name, text in files.items():
            archive.writestr(text.encode("latin1"), name)
    return buffer.getvalue()
