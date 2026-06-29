from __future__ import annotations

from dataclasses import dataclass
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse

from bralpha.metadata.datasets import DatasetConfig
from bralpha.parsing.common import normalize_column_name

STRUCTURED_EXTENSIONS = (".csv", ".txt", ".zip", ".xlsx", ".xls", ".ods")
PDF_EXTENSIONS = (".pdf",)


class ReceitaUnsupportedResourceError(ValueError):
    pass


@dataclass(frozen=True)
class ReceitaResourceRequest:
    dataset_id: str
    resource_name: str
    url: str
    filename: str
    resource_family: str
    ref_year: int | None = None
    ref_month: int | None = None


def receita_collection_resources(
    dataset_config: DatasetConfig,
    page_html: str | None = None,
) -> list[ReceitaResourceRequest]:
    if page_html is None:
        raise ValueError("Receita collection resource discovery requires page_html")
    page_url = str(dataset_config.model_extra.get("source_page_url") or "")
    selectors = [
        normalize_column_name(selector)
        for selector in dataset_config.model_extra.get("link_text_contains_any", [])
    ]
    accepted = tuple(
        str(extension).lower()
        for extension in dataset_config.model_extra.get(
            "accepted_extensions",
            STRUCTURED_EXTENSIONS,
        )
    )
    parser = _LinkParser(page_url)
    parser.feed(page_html)
    candidates = [
        link
        for link in parser.links
        if _selector_matches(link, selectors) and _extension(link.url) in accepted
    ]
    if not candidates:
        pdf_matches = [
            link
            for link in parser.links
            if _selector_matches(link, selectors) and _extension(link.url) in PDF_EXTENSIONS
        ]
        if pdf_matches:
            raise ReceitaUnsupportedResourceError(
                "Receita collection page exposes only PDF links for the configured selector"
            )
        raise ValueError("No Receita structured collection resource matched configured selectors")

    chosen = sorted(candidates, key=_resource_rank)[0]
    return [
        ReceitaResourceRequest(
            dataset_id=dataset_config.dataset_id,
            resource_name=_filename_stem(chosen.url),
            url=chosen.url,
            filename=_filename_from_url(
                chosen.url,
                fallback=f"{dataset_config.dataset_id}{_extension(chosen.url) or '.bin'}",
            ),
            resource_family="tax_collection_monthly",
        )
    ]


def receita_static_source_resources(dataset_config: DatasetConfig) -> list[ReceitaResourceRequest]:
    requests: list[ReceitaResourceRequest] = []
    for source_url in dataset_config.source_urls:
        if source_url.url_template is None:
            continue
        url, _, _, filename = source_url.render(dataset_id=dataset_config.dataset_id)
        requests.append(
            ReceitaResourceRequest(
                dataset_id=dataset_config.dataset_id,
                resource_name=source_url.name or _filename_stem(url),
                url=url,
                filename=filename
                or _filename_from_url(url, fallback=f"{dataset_config.dataset_id}.bin"),
                resource_family=source_url.name or dataset_config.dataset_id,
            )
        )
    return requests


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


def _selector_matches(link: _Link, selectors: list[str]) -> bool:
    link_text = normalize_column_name(link.text)
    if "metadados" in link_text or "metadata" in link_text:
        return False
    if not selectors:
        return True
    haystack = normalize_column_name(f"{link.text} {link.url}")
    return any(selector in haystack for selector in selectors)


def _resource_rank(link: _Link) -> tuple[int, int, str]:
    extension_rank = {
        ".csv": 0,
        ".txt": 1,
        ".zip": 2,
        ".xlsx": 3,
        ".xls": 4,
        ".ods": 5,
    }.get(_extension(link.url), 99)
    text = normalize_column_name(f"{link.text} {link.url}")
    history_rank = (
        0 if any(token in text for token in ("histor", "serie", "completo", "full")) else 1
    )
    return (extension_rank, history_rank, link.url)


def _extension(url: str) -> str:
    path = urlparse(url).path.lower()
    for extension in (*STRUCTURED_EXTENSIONS, *PDF_EXTENSIONS):
        if path.endswith(extension):
            return extension
    return ""


def _filename_from_url(url: str, *, fallback: str) -> str:
    filename = urlparse(url).path.rstrip("/").rsplit("/", 1)[-1]
    return filename or fallback


def _filename_stem(url: str) -> str:
    filename = _filename_from_url(url, fallback="resource")
    return filename.rsplit(".", 1)[0]
