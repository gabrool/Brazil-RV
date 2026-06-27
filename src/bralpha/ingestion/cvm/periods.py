from __future__ import annotations

from dataclasses import dataclass
from datetime import date

_DAILY_REPORT_MONTHLY_BASE = "https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS"
_DAILY_REPORT_HIST_BASE = "https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/HIST"


@dataclass(frozen=True)
class CVMResourceRequest:
    resource_name: str
    url: str
    filename: str
    period_year: int
    period_month: int | None = None


def fund_daily_report_resources(start: date, end: date) -> list[CVMResourceRequest]:
    """Return official CVM Informe Diario files intersecting a date window."""
    if start > end:
        raise ValueError("start must be on or before end")

    resources: list[CVMResourceRequest] = []
    for year in range(start.year, end.year + 1):
        year_start = date(year, 1, 1)
        year_end = date(year, 12, 31)
        if year_end < start or year_start > end:
            continue
        if year <= 2020:
            filename = f"inf_diario_fi_{year}.zip"
            resources.append(
                CVMResourceRequest(
                    resource_name=f"inf_diario_fi_hist_{year}",
                    url=f"{_DAILY_REPORT_HIST_BASE}/{filename}",
                    filename=filename,
                    period_year=year,
                )
            )
        else:
            month_start = 1 if start.year != year else start.month
            month_end = 12 if end.year != year else end.month
            for month in range(month_start, month_end + 1):
                filename = f"inf_diario_fi_{year}{month:02d}.zip"
                resources.append(
                    CVMResourceRequest(
                        resource_name=f"inf_diario_fi_{year}{month:02d}",
                        url=f"{_DAILY_REPORT_MONTHLY_BASE}/{filename}",
                        filename=filename,
                        period_year=year,
                        period_month=month,
                    )
                )
    return sorted(resources, key=lambda item: (item.period_year, item.period_month or 0))
