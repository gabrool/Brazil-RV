from __future__ import annotations

import json
from typing import Any

from bralpha.infra.http import HttpClient
from bralpha.parsing.common import normalize_column_name

CKAN_API_BASE_URL = "https://www.tesourotransparente.gov.br/ckan/api/3/action"


class TesouroCKANError(ValueError):
    pass


def package_show(package_id: str, client: HttpClient) -> dict[str, Any]:
    if not package_id or not package_id.strip():
        raise ValueError("package_id must be a non-empty string")
    response = client.get_bytes(
        f"{CKAN_API_BASE_URL}/package_show",
        params={"id": package_id.strip()},
    )
    if not 200 <= response.status_code < 300:
        raise TesouroCKANError(
            f"CKAN package_show failed for {package_id}: HTTP {response.status_code}"
        )
    payload = json.loads(response.content.decode("utf-8-sig"))
    if not payload.get("success"):
        raise TesouroCKANError(f"CKAN package_show returned success=false for {package_id}")
    result = payload.get("result")
    if not isinstance(result, dict):
        raise TesouroCKANError(f"CKAN package_show returned no result object for {package_id}")
    return result


def select_resources(
    package_json: dict[str, Any],
    *,
    formats: list[str],
    name_contains: list[str] | None = None,
    name_contains_any: list[str] | None = None,
) -> list[dict[str, Any]]:
    resources = package_json.get("resources")
    if not isinstance(resources, list):
        raise TesouroCKANError("CKAN package JSON has no resources list")

    wanted_formats = {item.strip().upper() for item in formats if item and item.strip()}
    selected = []
    for resource in resources:
        if not isinstance(resource, dict):
            continue
        resource_format = str(resource.get("format") or "").strip().upper()
        if wanted_formats and resource_format not in wanted_formats:
            continue
        name = str(resource.get("name") or "")
        if name_contains and not _contains_all(name, name_contains):
            continue
        if name_contains_any and not _contains_any(name, name_contains_any):
            continue
        selected.append(resource)

    if not selected:
        package_name = package_json.get("name") or package_json.get("title") or "<unknown>"
        raise TesouroCKANError(f"No CKAN resources matched selectors for {package_name}")
    return sorted(selected, key=_resource_sort_key)


def resource_download_url(resource: dict[str, Any]) -> str:
    url = str(resource.get("url") or "").strip()
    if not url:
        name = resource.get("name") or resource.get("id") or "<unknown>"
        raise TesouroCKANError(f"CKAN resource has no download URL: {name}")
    return url


def _contains_all(name: str, needles: list[str]) -> bool:
    normalized_name = normalize_column_name(name)
    return all(normalize_column_name(needle) in normalized_name for needle in needles)


def _contains_any(name: str, needles: list[str]) -> bool:
    normalized_name = normalize_column_name(name)
    return any(normalize_column_name(needle) in normalized_name for needle in needles)


def _resource_sort_key(resource: dict[str, Any]) -> tuple[int, str]:
    position = resource.get("position")
    if not isinstance(position, int):
        position = 10_000
    return position, str(resource.get("name") or resource.get("id") or "")
