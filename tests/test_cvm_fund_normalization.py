from __future__ import annotations

from datetime import datetime

import polars as pl

from bralpha.normalization.cvm_funds import (
    normalize_cvm_fund_daily_reports_to_silver,
    normalize_cvm_fund_registry_current_to_silver,
)


def test_cvm_daily_normalization_maps_official_fields_and_2bd_availability():
    bronze = pl.DataFrame(
        [
            {
                "raw_cnpj_fundo": "00.000.000/0001-00",
                "raw_tp_fundo": "FI",
                "raw_dt_comptc": "2024-01-05",
                "raw_vl_total": "1000,50",
                "raw_vl_patrim_liq": "900,25",
                "raw_vl_quota": "1,2345",
                "raw_captc_dia": "50,10",
                "raw_resg_dia": "20,05",
                "raw_nr_cotst": "123",
                "source": "cvm",
                "source_dataset": "cvm_fund_daily_reports",
                "download_timestamp_utc": datetime(2024, 1, 10, 12),
                "raw_path": "raw.zip",
                "sha256": "abc",
            }
        ]
    )

    silver = normalize_cvm_fund_daily_reports_to_silver(bronze)

    row = silver.to_dicts()[0]
    assert row["ref_date"].isoformat() == "2024-01-05"
    assert row["available_date"].isoformat() == "2024-01-09"
    assert row["availability_policy"] == "cvm_fund_daily_conservative_2bd"
    assert row["fund_id"] == "00.000.000/0001-00"
    assert row["portfolio_value"] == 1000.5
    assert row["nav"] == 900.25
    assert row["quota_value"] == 1.2345
    assert row["subscriptions"] == 50.1
    assert row["redemptions"] == 20.05
    assert row["shareholder_count"] == 123
    assert row["raw_captc_dia"] == "50,10"
    assert row["raw_resg_dia"] == "20,05"


def test_cvm_registry_current_normalization_sets_absent_optional_fields_to_null():
    bronze = pl.DataFrame(
        [
            {
                "raw_cnpj_fundo": "00.000.000/0001-00",
                "raw_denom_social": "Fundo Teste",
                "raw_cd_cvm": "00123",
                "raw_sit": "EM FUNCIONAMENTO NORMAL",
                "raw_fundo_exclusivo": "N",
                "raw_fundo_cotas": "S",
                "raw_cnpj_admin": "11.111.111/0001-11",
                "raw_admin": "Administrador Teste",
                "raw_publico_alvo": "Investidores em geral",
                "source": "cvm",
                "source_dataset": "cvm_fund_registry_current",
                "download_timestamp_utc": datetime(2024, 2, 5, 12),
                "raw_path": "cad_fi.csv",
                "sha256": "abc",
            }
        ]
    )

    silver = normalize_cvm_fund_registry_current_to_silver(bronze)
    row = silver.to_dicts()[0]

    assert row["fund_id"] == "00.000.000/0001-00"
    assert row["fund_name"] == "Fundo Teste"
    assert row["cvm_code"] == "00123"
    assert row["status"] == "EM FUNCIONAMENTO NORMAL"
    assert row["is_exclusive"] == "N"
    assert row["is_fund_of_funds"] == "S"
    assert row["public_target"] == "Investidores em geral"
    assert row["admin_id"] == "11.111.111/0001-11"
    assert row["admin_name"] == "Administrador Teste"
    assert row["is_long_term_tax"] is None
    assert row["custodian_name"] is None
    assert row["auditor_id"] is None
    assert row["auditor_name"] is None
    assert row["controller_id"] is None
    assert row["controller_name"] is None
    assert row["snapshot_date"].isoformat() == "2024-02-05"
