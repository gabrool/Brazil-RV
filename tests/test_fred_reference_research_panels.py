from __future__ import annotations

from bralpha.derived.fred.reference import build_fred_series_reference
from bralpha.ingestion.fred.common import load_fred_series_config


def test_fred_series_reference_preserves_config_metadata(repo_root):
    series_config = load_fred_series_config(repo_root)

    panel = build_fred_series_reference(series_config)

    dgs10 = panel.filter(panel["series_id"] == "DGS10").row(0, named=True)
    copper = panel.filter(panel["series_id"] == "PCOPPUSDM").row(0, named=True)
    assert dgs10["feature_id"] == "fred|dgs10"
    assert dgs10["priority"] == "P0"
    assert dgs10["model_usable"] is True
    assert dgs10["availability_policy"] == "date_only_next_business_day"
    assert copper["priority"] == "P1"
    assert "IMF" in copper["notes"]
    assert panel["series_id"].n_unique() == panel.height
