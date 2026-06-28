from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from html.parser import HTMLParser
from urllib.parse import urljoin

from bralpha.metadata.datasets import DatasetConfig
from bralpha.parsing.common import normalize_column_name


@dataclass(frozen=True)
class ANPResourceRequest:
    dataset_id: str
    resource_name: str
    url: str
    filename: str
    resource_family: str
    year: int | None = None
    month: int | None = None
    semester: int | None = None


def anp_fuel_price_resources(
    dataset_config: DatasetConfig,
    start: date,
    end: date,
) -> list[ANPResourceRequest]:
    if start > end:
        raise ValueError("ANP fuel price resources require start <= end")

    requests: list[ANPResourceRequest] = []
    for family in dataset_config.model_extra.get("resource_families", []):
        mode = family["mode"]
        if mode == "semiannual_zip":
            requests.extend(_semiannual_requests(dataset_config, family, start, end))
        elif mode == "monthly_csv":
            requests.extend(_monthly_requests(dataset_config, family, start, end))
        else:
            raise ValueError(f"Unsupported ANP fuel price resource mode: {mode}")
    return sorted(
        requests,
        key=lambda item: (
            item.year or 0,
            item.semester or 0,
            item.month or 0,
            item.resource_family,
        ),
    )


def anp_single_page_resource(
    dataset_config: DatasetConfig,
    page_html: str | None = None,
) -> list[ANPResourceRequest]:
    if page_html is None:
        raise ValueError("ANP single-page resource discovery requires page_html")
    selector = dataset_config.model_extra.get("link_text_contains")
    page_url = dataset_config.model_extra.get("source_page_url", "")
    if not selector:
        raise ValueError(f"Dataset has no link_text_contains: {dataset_config.dataset_id}")
    link = _select_link(page_html, str(selector), base_url=str(page_url))
    return [
        ANPResourceRequest(
            dataset_id=dataset_config.dataset_id,
            resource_name=_filename_stem(link.url),
            url=link.url,
            filename=_filename_from_url(link.url, fallback=f"{dataset_config.dataset_id}.csv"),
            resource_family=dataset_config.dataset_id,
        )
    ]


def anp_multi_page_resources(
    dataset_config: DatasetConfig,
    page_html: str | None = None,
) -> list[ANPResourceRequest]:
    if page_html is None:
        raise ValueError("ANP multi-page resource discovery requires page_html")
    selectors = dataset_config.model_extra.get("resource_link_texts", {})
    page_url = dataset_config.model_extra.get("source_page_url", "")
    if not selectors:
        raise ValueError(f"Dataset has no resource_link_texts: {dataset_config.dataset_id}")
    requests: list[ANPResourceRequest] = []
    for resource_family, selector in selectors.items():
        link = _select_link(page_html, str(selector), base_url=str(page_url))
        requests.append(
            ANPResourceRequest(
                dataset_id=dataset_config.dataset_id,
                resource_name=str(resource_family),
                url=link.url,
                filename=_filename_from_url(
                    link.url, fallback=f"{dataset_config.dataset_id}-{resource_family}.csv"
                ),
                resource_family=str(resource_family),
            )
        )
    return requests


def _semiannual_requests(
    dataset_config: DatasetConfig,
    family: dict[str, object],
    start: date,
    end: date,
) -> list[ANPResourceRequest]:
    until = _parse_date(family.get("applies_until")) or date.max
    range_start = start
    range_end = min(end, until)
    if range_start > range_end:
        return []

    requests: list[ANPResourceRequest] = []
    for year in range(range_start.year, range_end.year + 1):
        for semester in (1, 2):
            period_start = date(year, 1 if semester == 1 else 7, 1)
            period_end = date(year, 6 if semester == 1 else 12, 30 if semester == 1 else 31)
            if period_end < range_start or period_start > range_end:
                continue
            requests.append(_price_request(dataset_config, family, year=year, semester=semester))
    return requests


def _monthly_requests(
    dataset_config: DatasetConfig,
    family: dict[str, object],
    start: date,
    end: date,
) -> list[ANPResourceRequest]:
    applies_from = _parse_date(family.get("applies_from")) or date.min
    range_start = max(start, applies_from)
    if range_start > end:
        return []

    requests: list[ANPResourceRequest] = []
    year = range_start.year
    month = range_start.month
    while (year, month) <= (end.year, end.month):
        requests.append(_price_request(dataset_config, family, year=year, month=month))
        month += 1
        if month == 13:
            month = 1
            year += 1
    return requests


def _price_request(
    dataset_config: DatasetConfig,
    family: dict[str, object],
    *,
    year: int,
    month: int | None = None,
    semester: int | None = None,
) -> ANPResourceRequest:
    values = {"year": year, "month": month or 0, "semester": semester or 0}
    family_name = str(family["name"])
    filename = str(family["filename_template"]).format(**values)
    return ANPResourceRequest(
        dataset_id=dataset_config.dataset_id,
        resource_name=f"{family_name}-{year}-{semester or month:02d}",
        url=str(family["url_template"]).format(**values),
        filename=filename,
        resource_family=family_name,
        year=year,
        month=month,
        semester=semester,
    )


def _parse_date(value: object) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    text = str(value).strip()
    return date.fromisoformat(text) if text else None


@dataclass(frozen=True)
class _Link:
    text: str
    url: str


class _LinkParser(HTMLParser):
    def __init__(self, base_url: str) -> None:
        super().__init__()
        self.base_url = base_url
        self.links: list[_Link] = []
        self._href: str | None = None
        self._text_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "a":
            return
        attrs_dict = dict(attrs)
        self._href = attrs_dict.get("href")
        self._text_parts = []

    def handle_data(self, data: str) -> None:
        if self._href is not None:
            self._text_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag != "a" or self._href is None:
            return
        text = " ".join("".join(self._text_parts).split())
        self.links.append(_Link(text=text, url=urljoin(self.base_url, self._href)))
        self._href = None
        self._text_parts = []


def _select_link(page_html: str, text_contains: str, *, base_url: str) -> _Link:
    parser = _LinkParser(base_url)
    parser.feed(page_html)
    selector = normalize_column_name(text_contains)
    candidates = [
        link
        for link in parser.links
        if selector in normalize_column_name(link.text)
        and not normalize_column_name(link.text).startswith("metadados")
    ]
    if not candidates:
        raise ValueError(f"No ANP page link matched {text_contains!r}")
    csv_candidates = [link for link in candidates if ".csv" in link.url.lower()]
    return csv_candidates[0] if csv_candidates else candidates[0]


def _filename_from_url(url: str, *, fallback: str) -> str:
    filename = url.rstrip("/").rsplit("/", 1)[-1]
    return filename or fallback


def _filename_stem(url: str) -> str:
    filename = _filename_from_url(url, fallback="resource")
    return filename.rsplit(".", 1)[0]
