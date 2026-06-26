from __future__ import annotations

import json
from datetime import date, datetime
from types import SimpleNamespace

import pytest

import bralpha.ingestion.b3.common as b3_common
import bralpha.ingestion.b3.reports as reports_module
from bralpha.infra.config import load_b3_dataset_registry
from bralpha.infra.hashing import sha256_bytes
from bralpha.infra.http import HttpResponse
from bralpha.ingestion.b3.reports import (
    download_daily_bulletin_chapter_for_date,
    download_fee_schedule_page,
    download_market_data_public_report_for_date,
    download_product_specs_page,
)
from bralpha.metadata.manifest import ManifestRecord
from bralpha.normalization.b3_reports import (
    FEE_SCHEDULE_COLUMNS,
    PRODUCT_SPEC_METADATA_COLUMNS,
    RAW_REPORT_METADATA_COLUMNS,
    normalize_fee_schedule_table,
    normalize_product_spec_metadata,
    normalize_raw_report_metadata,
    write_fee_schedule,
    write_product_spec_metadata,
    write_raw_report_metadata,
)
from bralpha.parsing.b3_reports import parse_fee_schedule_table

RAW_PAGE = b"<html><body><h1>B3 page</h1></body></html>"


class MockReportClient:
    def __init__(
        self,
        status_code: int = 200,
        content: bytes = RAW_PAGE,
        content_type: str = "text/html; charset=utf-8",
    ) -> None:
        self.status_code = status_code
        self.content = content
        self.content_type = content_type

    def get_bytes(self, url, params=None, headers=None):
        return HttpResponse(
            url=f"{url}?mock=1",
            status_code=self.status_code,
            headers={"content-type": self.content_type},
            content=self.content,
        )


def test_fee_schedule_page_download_success_and_failure_manifests(
    repo_root,
    tmp_path,
    monkeypatch,
):
    paths = _patch_report_paths(monkeypatch, tmp_path)

    success = download_fee_schedule_page(
        repo_root,
        fee_id="equities_spot",
        page_url="https://www.b3.com.br/fees/equities",
        client=MockReportClient(),
        downloaded_at=datetime(2024, 1, 2, 12),
    )
    failure = download_fee_schedule_page(
        repo_root,
        fee_id="equities_spot",
        page_url="https://www.b3.com.br/fees/equities",
        client=MockReportClient(status_code=503, content=b"unavailable"),
        downloaded_at=datetime(2024, 1, 2, 12),
    )
    rows = _read_manifest_rows(paths)

    assert success.success is True
    assert failure.success is False
    assert rows[0]["dataset_id"] == "b3_fee_schedules"
    assert rows[0]["sha256"] == sha256_bytes(RAW_PAGE)
    assert rows[0]["raw_path"]
    assert rows[1]["http_status"] == 503
    assert rows[1]["raw_path"] is None
    assert rows[1]["sha256"] is None
    assert rows[1]["error_message"] == "HTTP 503"


def test_fee_schedule_html_table_normalizes_and_writes_source_specific_output(tmp_path):
    html = (
        b"""
    <table>
      <tr>
        <th>produto</th><th>tipo_investidor</th><th>tipo</th>
        <th>unidade</th><th>valor</th><th>mercado</th><th>moeda</th>
      </tr>
      <tr>
        <td>DOL/WDO</td><td>Day trade</td><td>Trading fee</td>
        <td>bps</td><td>0,25</td><td>Derivatives</td><td>BRL</td>
      </tr>
    </table>
    """
    )
    bronze = parse_fee_schedule_table(
        html,
        fee_id="equities_spot",
        download_date=date(2024, 1, 2),
        download_timestamp_utc=datetime(2024, 1, 2, 12),
        raw_path="raw/b3_fee_schedules/equities.html",
        sha256="fee-hash",
        required_all=["produto", "valor"],
    )
    fees = normalize_fee_schedule_table(bronze)
    paths = write_fee_schedule(
        fees,
        tmp_path / "silver" / "b3_fee_schedules",
        primary_keys=["ref_date", "fee_id", "product", "investor_type", "fee_type"],
    )

    assert fees.columns == FEE_SCHEDULE_COLUMNS
    assert fees["available_date"].item() == date(2024, 1, 2)
    assert fees["product"].item() == "DOL/WDO"
    assert fees["investor_type"].item() == "DAY TRADE"
    assert fees["fee_type"].item() == "TRADING FEE"
    assert fees["fee_value"].item() == 0.25
    assert fees["fee_unit"].item() == "BPS"
    assert fees["market_segment"].item() == "DERIVATIVES"
    assert fees["raw_path"].item() == "raw/b3_fee_schedules/equities.html"
    assert paths == [tmp_path / "silver" / "b3_fee_schedules" / "data.parquet"]


def test_product_spec_raw_metadata_uses_manifest_fields_and_download_date(
    repo_root,
    tmp_path,
    monkeypatch,
):
    _patch_report_paths(monkeypatch, tmp_path)
    di1_page = _product_page(repo_root, "DI1")
    record = download_product_specs_page(
        repo_root,
        product_root=di1_page["product_root"],
        product_name=di1_page["product_name"],
        page_url=di1_page["page_url"],
        client=MockReportClient(content=b"<html>DI1</html>"),
        downloaded_at=datetime(2024, 1, 2, 12),
    )

    metadata = normalize_product_spec_metadata(
        record,
        product_root=di1_page["product_root"],
        product_name=di1_page["product_name"],
        page_url=di1_page["page_url"],
    )
    paths = write_product_spec_metadata(
        metadata,
        tmp_path / "silver" / "b3_product_specs_pages",
        primary_keys=["download_date", "product_root"],
    )

    assert metadata.columns == PRODUCT_SPEC_METADATA_COLUMNS
    assert metadata["download_date"].item() == date(2024, 1, 2)
    assert metadata["available_date"].item() == date(2024, 1, 2)
    assert "/tarifas/" not in metadata["page_url"].item()
    assert "/fee-schedules/" not in metadata["page_url"].item()
    assert metadata["content_type"].item() == "text/html; charset=utf-8"
    assert metadata["raw_path"].item()
    assert metadata["sha256"].item() == sha256_bytes(b"<html>DI1</html>")
    assert paths == [tmp_path / "silver" / "b3_product_specs_pages" / "data.parquet"]


def test_raw_public_report_metadata_rows_include_audit_fields(tmp_path):
    record = ManifestRecord(
        dataset_id="b3_market_data_public_reports",
        source="b3",
        source_url="https://www.b3.com.br/reports",
        request_params={"report_name": "cash_listed_market_statistics"},
        download_timestamp_utc=datetime(2024, 1, 2, 18),
        http_status=200,
        content_type="application/pdf",
        file_size_bytes=100,
        sha256="report-hash",
        raw_path="raw/reports/report.pdf",
        license_note="Public B3 report data.",
        success=True,
    )

    metadata = normalize_raw_report_metadata(
        record,
        ref_date=date(2024, 1, 2),
        report_name="cash_listed_market_statistics",
        report_category="market_data",
    )
    paths = write_raw_report_metadata(
        metadata,
        tmp_path / "silver" / "b3_market_data_public_reports",
        primary_keys=["ref_date", "report_name"],
    )

    assert metadata.columns == RAW_REPORT_METADATA_COLUMNS
    assert metadata["available_date"].item() == date(2024, 1, 2)
    assert metadata["content_type"].item() == "application/pdf"
    assert metadata["raw_path"].item() == "raw/reports/report.pdf"
    assert metadata["sha256"].item() == "report-hash"
    assert paths[0].parent.parent.name == "b3_market_data_public_reports"


def test_pending_report_download_wrappers_raise_before_http_client(repo_root, monkeypatch):
    class ExplodingClient:
        def __init__(self):
            raise AssertionError("client should not be constructed before URL validation")

    monkeypatch.setattr(b3_common, "HttpClient", ExplodingClient)

    with pytest.raises(NotImplementedError, match="no confirmed free source URL"):
        download_daily_bulletin_chapter_for_date(
            repo_root,
            ref_date=date(2024, 1, 2),
            report_section="BVBG.087.01 IndexReport",
        )
    with pytest.raises(NotImplementedError, match="no confirmed free source URL"):
        download_market_data_public_report_for_date(
            repo_root,
            ref_date=date(2024, 1, 2),
            report_name="cash_listed_market_statistics",
        )


def _patch_report_paths(monkeypatch, tmp_path):
    paths = SimpleNamespace(raw=tmp_path / "raw", manifests=tmp_path / "manifests")
    monkeypatch.setattr(
        reports_module,
        "resolve_project_paths",
        lambda repo_root, paths_config: paths,
    )
    return paths


def _read_manifest_rows(paths) -> list[dict[str, object]]:
    return [
        json.loads(line)
        for line in (paths.manifests / "b3" / "downloads.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
    ]


def _product_page(repo_root, product_root: str) -> dict[str, str]:
    pages = load_b3_dataset_registry(repo_root).get("b3_product_specs_pages").request_defaults[
        "product_pages"
    ]
    for page in pages:
        if page["product_root"] == product_root:
            return page
    raise AssertionError(f"missing product page fixture: {product_root}")
