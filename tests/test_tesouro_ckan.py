from __future__ import annotations

import json

import pytest

from bralpha.infra.http import HttpResponse
from bralpha.ingestion.tesouro.ckan import (
    TesouroCKANError,
    package_show,
    resource_download_url,
    select_resources,
)


class MockClient:
    def __init__(self, payload: dict) -> None:
        self.payload = payload
        self.requests = []

    def get_bytes(self, url, params=None, headers=None):
        self.requests.append({"url": url, "params": params or {}, "headers": headers or {}})
        return HttpResponse(
            url=url,
            status_code=200,
            headers={"content-type": "application/json"},
            content=json.dumps(self.payload).encode(),
        )


def test_package_show_calls_ckan_action_with_package_id():
    client = MockClient({"success": True, "result": {"name": "pkg", "resources": []}})

    result = package_show("pkg", client)

    assert result["name"] == "pkg"
    assert client.requests == [
        {
            "url": "https://www.tesourotransparente.gov.br/ckan/api/3/action/package_show",
            "params": {"id": "pkg"},
            "headers": {},
        }
    ]


def test_select_resources_filters_by_format_and_normalized_name():
    package = {
        "name": "resgates",
        "resources": [
            _resource("xlsx", "Planilha", 0),
            _resource("CSV", "Recompras do Tesouro Direto", 2),
            _resource("CSV", "Vencimentos do Tesouro Direto", 1),
            _resource("CSV", "Pagamento de Cupom de Juros do Tesouro Direto", 3),
        ],
    }

    selected = select_resources(
        package,
        formats=["CSV"],
        name_contains_any=[
            "Vencimentos do Tesouro Direto",
            "Recompras do Tesouro Direto",
            "Pagamento de Cupom de Juros do Tesouro Direto",
        ],
    )

    assert [resource["name"] for resource in selected] == [
        "Vencimentos do Tesouro Direto",
        "Recompras do Tesouro Direto",
        "Pagamento de Cupom de Juros do Tesouro Direto",
    ]


def test_select_resources_raises_when_no_resource_matches():
    with pytest.raises(TesouroCKANError, match="No CKAN resources matched"):
        select_resources(
            {"name": "pkg", "resources": [_resource("XLSX", "Planilha", 0)]},
            formats=["CSV"],
            name_contains=["Estoque"],
        )


def test_resource_download_url_requires_url():
    assert resource_download_url({"name": "CSV", "url": "https://example.test/file.csv"}) == (
        "https://example.test/file.csv"
    )
    with pytest.raises(TesouroCKANError, match="no download URL"):
        resource_download_url({"name": "CSV"})


def _resource(fmt: str, name: str, position: int) -> dict[str, object]:
    return {
        "format": fmt,
        "name": name,
        "position": position,
        "url": f"https://example.test/{position}.csv",
    }
