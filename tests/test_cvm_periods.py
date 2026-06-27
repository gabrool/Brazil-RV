from __future__ import annotations

from datetime import date

import pytest

from bralpha.ingestion.cvm.periods import fund_daily_report_resources


def test_cvm_periods_use_annual_hist_for_2019():
    resources = fund_daily_report_resources(date(2019, 3, 1), date(2019, 3, 31))

    assert len(resources) == 1
    assert resources[0].filename == "inf_diario_fi_2019.zip"
    assert resources[0].period_year == 2019
    assert resources[0].period_month is None
    assert resources[0].url.endswith("/HIST/inf_diario_fi_2019.zip")


def test_cvm_periods_use_monthly_files_for_2024_jan_feb():
    resources = fund_daily_report_resources(date(2024, 1, 31), date(2024, 2, 1))

    assert [resource.filename for resource in resources] == [
        "inf_diario_fi_202401.zip",
        "inf_diario_fi_202402.zip",
    ]
    assert [resource.period_month for resource in resources] == [1, 2]
    assert all("/HIST/" not in resource.url for resource in resources)


def test_cvm_periods_handle_2020_2021_boundary():
    resources = fund_daily_report_resources(date(2020, 12, 31), date(2021, 1, 1))

    assert [resource.filename for resource in resources] == [
        "inf_diario_fi_2020.zip",
        "inf_diario_fi_202101.zip",
    ]
    assert resources[0].period_month is None
    assert resources[1].period_month == 1


def test_cvm_periods_reject_inverted_window():
    with pytest.raises(ValueError, match="start must be on or before end"):
        fund_daily_report_resources(date(2024, 2, 1), date(2024, 1, 31))
