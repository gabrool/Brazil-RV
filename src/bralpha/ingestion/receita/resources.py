from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urljoin, urlparse

from bralpha.metadata.datasets import DatasetConfig
from bralpha.parsing.common import normalize_column_name

STRUCTURED_EXTENSIONS = (".csv", ".txt", ".zip", ".xlsx", ".xls", ".ods")
PDF_EXTENSIONS = (".pdf",)
_RESOURCE_CONTAINER_KEYS = {"resources", "recursos", "distribution", "distributions"}
_RESOURCE_URL_KEYS = ("url", "href", "download_url", "resource_url", "link")
_RESOURCE_TEXT_KEYS = ("name", "title", "description")
_RESOURCE_FORMAT_KEYS = ("format", "mimetype", "media_type", "mediaType")


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
    return receita_collection_resources_from_html(dataset_config, page_html)


def receita_collection_resources_from_html(
    dataset_config: DatasetConfig,
    page_html: str | None = None,
) -> list[ReceitaResourceRequest]:
    if page_html is None:
        raise ValueError("Receita collection resource discovery requires page_html")
    page_url = str(dataset_config.model_extra.get("source_page_url") or "")
    parser = _LinkParser(page_url)
    parser.feed(page_html)
    return _resource_requests_from_candidates(
        dataset_config,
        [
            _ResourceCandidate(
                text=link.text,
                url=link.url,
                format_hint="",
                metadata_only=_is_metadata_resource(link.text, link.url),
            )
            for link in parser.links
        ],
    )


def receita_collection_resources_from_metadata(
    dataset_config: DatasetConfig,
    metadata_json: object,
) -> list[ReceitaResourceRequest]:
    metadata = _load_metadata_json(metadata_json)
    entries = _metadata_resource_entries(metadata)
    candidates: list[_ResourceCandidate] = []
    for entry in entries:
        url = _first_text(entry, _RESOURCE_URL_KEYS)
        if not url:
            continue
        text = " ".join(
            value
            for value in (
                _first_text(entry, _RESOURCE_TEXT_KEYS),
                _first_text(entry, _RESOURCE_FORMAT_KEYS),
            )
            if value
        )
        format_hint = _first_text(entry, _RESOURCE_FORMAT_KEYS)
        candidates.append(
            _ResourceCandidate(
                text=text or url,
                url=url,
                format_hint=format_hint,
                metadata_only=_is_metadata_resource(text, url),
            )
        )
    return _resource_requests_from_candidates(dataset_config, candidates)


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


@dataclass(frozen=True)
class _ResourceCandidate:
    text: str
    url: str
    format_hint: str = ""
    metadata_only: bool = False


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


def _resource_requests_from_candidates(
    dataset_config: DatasetConfig,
    candidates: list[_ResourceCandidate],
) -> list[ReceitaResourceRequest]:
    selectors = [
        normalize_column_name(selector)
        for selector in dataset_config.model_extra.get("link_text_contains_any", [])
    ]
    selector_matches = [
        candidate for candidate in candidates if _selector_matches(candidate, selectors)
    ]
    if selector_matches:
        candidates = selector_matches

    accepted = _accepted_extensions(dataset_config)
    rejected = _rejected_extensions(dataset_config)
    structured_candidates = [
        candidate
        for candidate in candidates
        if _candidate_extension(candidate) in accepted and not candidate.metadata_only
    ]
    metadata_only_candidates = [
        candidate
        for candidate in candidates
        if _candidate_extension(candidate) in accepted and candidate.metadata_only
    ]
    rejected_candidates = [
        candidate for candidate in candidates if _candidate_extension(candidate) in rejected
    ]

    if not structured_candidates:
        if metadata_only_candidates:
            raise ReceitaUnsupportedResourceError(
                "Receita collection discovery found only metadata/metadados structured "
                "resources; refusing to download metadata as data"
            )
        if rejected_candidates:
            raise ReceitaUnsupportedResourceError(
                "Receita collection discovery found only PDF resources for the configured "
                "selector"
            )
        raise ValueError("No Receita structured collection resource matched configured selectors")

    chosen = sorted(structured_candidates, key=_resource_rank)[0]
    return [
        ReceitaResourceRequest(
            dataset_id=dataset_config.dataset_id,
            resource_name=_filename_stem(chosen.url),
            url=chosen.url,
            filename=_filename_from_url(
                chosen.url,
                fallback=f"{dataset_config.dataset_id}{_candidate_extension(chosen) or '.bin'}",
            ),
            resource_family="tax_collection_monthly",
        )
    ]


def _load_metadata_json(metadata_json: object) -> object:
    if isinstance(metadata_json, bytes):
        return json.loads(metadata_json.decode("utf-8", errors="replace"))
    if isinstance(metadata_json, str):
        return json.loads(metadata_json)
    return metadata_json


def _metadata_resource_entries(metadata: object) -> list[Mapping[str, Any]]:
    entries: list[Mapping[str, Any]] = []

    def collect(value: object) -> None:
        if isinstance(value, Mapping):
            for key, child in value.items():
                normalized_key = normalize_column_name(str(key))
                if normalized_key in _RESOURCE_CONTAINER_KEYS:
                    entries.extend(_coerce_resource_entries(child))
                elif isinstance(child, (Mapping, Sequence)) and not isinstance(
                    child, (str, bytes, bytearray)
                ):
                    collect(child)
        elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            for child in value:
                collect(child)

    collect(metadata)
    return entries


def _coerce_resource_entries(value: object) -> list[Mapping[str, Any]]:
    if isinstance(value, Mapping):
        return [value]
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [item for item in value if isinstance(item, Mapping)]
    return []


def _first_text(entry: Mapping[str, Any], keys: Sequence[str]) -> str:
    for key in keys:
        value = entry.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _selector_matches(candidate: _ResourceCandidate, selectors: list[str]) -> bool:
    if not selectors:
        return True
    haystack = normalize_column_name(f"{candidate.text} {candidate.url}")
    return any(selector in haystack for selector in selectors)


def _is_metadata_resource(text: str, url: str) -> bool:
    haystack = normalize_column_name(f"{text} {url}")
    return "metadados" in haystack or "metadata" in haystack


def _resource_rank(candidate: _ResourceCandidate) -> tuple[int, int, str]:
    text = normalize_column_name(f"{candidate.text} {candidate.url}")
    history_rank = (
        0
        if any(
            token in text
            for token in ("histor", "serie", "completo", "completa", "full", "consolid")
        )
        else 1
    )
    extension_rank = {
        ".csv": 0,
        ".txt": 1,
        ".zip": 2,
        ".xlsx": 3,
        ".xls": 4,
        ".ods": 5,
    }.get(_candidate_extension(candidate), 99)
    return (history_rank, extension_rank, candidate.url)


def _candidate_extension(candidate: _ResourceCandidate) -> str:
    url_extension = _extension(candidate.url)
    if url_extension:
        return url_extension
    hint = normalize_column_name(candidate.format_hint)
    if "pdf" in hint:
        return ".pdf"
    if "csv" in hint:
        return ".csv"
    if "zip" in hint:
        return ".zip"
    if "xlsx" in hint:
        return ".xlsx"
    if "xls" in hint:
        return ".xls"
    if "ods" in hint:
        return ".ods"
    if "txt" in hint or "text" in hint or "plain" in hint:
        return ".txt"
    return ""


def _accepted_extensions(dataset_config: DatasetConfig) -> tuple[str, ...]:
    discovery = _resource_discovery(dataset_config)
    extensions = discovery.get("accepted_extensions") or dataset_config.model_extra.get(
        "accepted_extensions",
        STRUCTURED_EXTENSIONS,
    )
    return tuple(str(extension).lower() for extension in extensions)


def _rejected_extensions(dataset_config: DatasetConfig) -> tuple[str, ...]:
    discovery = _resource_discovery(dataset_config)
    extensions = discovery.get("rejected_extensions") or PDF_EXTENSIONS
    return tuple(str(extension).lower() for extension in extensions)


def _resource_discovery(dataset_config: DatasetConfig) -> Mapping[str, Any]:
    value = dataset_config.model_extra.get("resource_discovery") or {}
    return value if isinstance(value, Mapping) else {}


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
