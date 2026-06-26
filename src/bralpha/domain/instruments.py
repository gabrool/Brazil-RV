from __future__ import annotations

from dataclasses import dataclass

from bralpha.infra.config import InstrumentsConfig

DEFAULT_ROOT_ASSET_CLASS = {
    "DI1": "rates",
    "DAP": "rates",
    "DDI": "rates",
    "FRC": "rates",
    "DOL": "fx",
    "WDO": "fx",
    "IND": "equity_index",
    "WIN": "equity_index",
}


@dataclass(frozen=True)
class InstrumentRegistry:
    root_to_asset_class: dict[str, str]

    @classmethod
    def from_config(cls, config: InstrumentsConfig) -> InstrumentRegistry:
        mapping: dict[str, str] = {}
        for sleeve, sleeve_config in config.traded_sleeves.items():
            for root in [*sleeve_config.primary_roots, *sleeve_config.secondary_roots]:
                mapping[root.upper()] = sleeve
        return cls(mapping)

    def asset_class_for_root(self, root: str) -> str | None:
        return self.root_to_asset_class.get(root.strip().upper())


def asset_class_for_root(
    root: str,
    registry: InstrumentRegistry | dict[str, str] | None = None,
) -> str | None:
    normalized = root.strip().upper()
    if isinstance(registry, InstrumentRegistry):
        return registry.asset_class_for_root(normalized)
    if registry is not None:
        return registry.get(normalized)
    return DEFAULT_ROOT_ASSET_CLASS.get(normalized)
