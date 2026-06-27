from __future__ import annotations

import io
import zipfile
from datetime import UTC, datetime
from pathlib import Path

from bralpha.parsing.cvm_funds import (
    parse_cvm_fund_daily_report_bytes,
    parse_cvm_registry_bytes,
)


def test_cvm_daily_parser_preserves_zip_member_lineage_and_raw_columns():
    frame = parse_cvm_fund_daily_report_bytes(
        _zip_bytes(
            {
                "a.csv": "CNPJ_FUNDO;DT_COMPTC;VL_TOTAL\n00.000.000/0001-00;2024-01-31;10,5\n",
                "b.csv": "CNPJ_FUNDO;DT_COMPTC;VL_TOTAL\n00.000.000/0002-00;2024-02-01;11,5\n",
            }
        ),
        raw_format="zip_csv",
        source_dataset="cvm_fund_daily_reports",
        download_timestamp_utc=datetime(2024, 2, 5, 12, tzinfo=UTC),
        raw_path=Path("raw.zip"),
        sha256="abc",
    )

    assert frame.height == 2
    assert frame["inner_filename"].to_list() == ["a.csv", "b.csv"]
    assert frame["row_index"].to_list() == [0, 0]
    assert frame["fund_id"].to_list() == ["00.000.000/0001-00", "00.000.000/0002-00"]
    assert frame["raw_vl_total"].to_list() == ["10,5", "11,5"]
    assert "raw_fields_json" not in frame.columns


def test_cvm_daily_parser_supports_single_csv_payload():
    frame = parse_cvm_fund_daily_report_bytes(
        b"CNPJ_FUNDO;DT_COMPTC;VL_TOTAL\n00.000.000/0001-00;2024-01-31;10\n",
        raw_format="csv",
        source_dataset="cvm_fund_daily_reports",
        download_timestamp_utc=datetime(2024, 2, 5, 12, tzinfo=UTC),
        raw_path=Path("daily.csv"),
        sha256="abc",
    )

    assert frame.height == 1
    assert frame["inner_filename"].to_list() == [None]
    assert frame["row_index"].to_list() == [0]
    assert "raw_cnpj_fundo" in frame.columns


def test_cvm_registry_parser_reads_strings_first_and_preserves_raw_columns():
    frame = parse_cvm_registry_bytes(
        b"CNPJ_FUNDO;CD_CVM;DENOM_SOCIAL\n00.000.000/0001-00;00123;Fundo Teste\n",
        raw_format="csv",
        source_dataset="cvm_fund_registry_current",
        download_timestamp_utc=datetime(2024, 2, 5, 12, tzinfo=UTC),
        raw_path=Path("cad_fi.csv"),
        sha256="abc",
    )

    assert frame.height == 1
    assert frame["raw_cd_cvm"].to_list() == ["00123"]
    assert frame["raw_denom_social"].to_list() == ["Fundo Teste"]
    assert "source_dataset" in frame.columns


def _zip_bytes(members: dict[str, str]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, text in members.items():
            archive.writestr(name, text.encode("latin1"))
    return buffer.getvalue()
