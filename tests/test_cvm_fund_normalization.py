from __future__ import annotations

from datetime import date, datetime

import polars as pl

from bralpha.normalization.cvm_funds import (
    CVM_DAILY_DELIVERY_METADATA_POLICY,
    CVM_DAILY_FIRST_SEEN_POLICY,
    CVM_DAILY_REFERENCE_ONLY_POLICY,
    CVM_REGISTRY_CURRENT_REFERENCE_POLICY,
    normalize_cvm_fund_daily_reports_to_silver,
    normalize_cvm_fund_registry_current_to_silver,
)
from bralpha.timing.vintages import (
    AVAILABILITY_CONSERVATIVE_HEURISTIC,
    AVAILABILITY_CURRENT_SNAPSHOT_NO_VINTAGE,
    AVAILABILITY_EXACT_SOURCE_TIMESTAMP,
    AVAILABILITY_FIRST_SEEN_DOWNLOAD_TIMESTAMP,
    REVISION_CURRENT_SNAPSHOT_REFERENCE_ONLY,
    REVISION_REVISED_USE_FIRST_SEEN,
    REVISION_REVISED_USE_VINTAGES,
)


def test_cvm_daily_normalization_uses_first_seen_snapshot_availability():
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
    assert row["available_date"].isoformat() == "2024-01-10"
    assert row["availability_policy"] == CVM_DAILY_FIRST_SEEN_POLICY
    assert row["availability_basis"] == AVAILABILITY_FIRST_SEEN_DOWNLOAD_TIMESTAMP
    assert row["revision_policy"] == REVISION_REVISED_USE_FIRST_SEEN
    assert row["first_seen_timestamp_utc"] == datetime(2024, 1, 10, 12)
    assert row["vintage_id"].startswith("cvm:cvm_fund_daily_reports:")
    assert row["model_usable"] is True
    assert row["model_usable_reason"] == CVM_DAILY_FIRST_SEEN_POLICY
    assert row["fund_id"] == "00.000.000/0001-00"
    assert row["portfolio_value"] == 1000.5
    assert row["nav"] == 900.25
    assert row["quota_value"] == 1.2345
    assert row["subscriptions"] == 50.1
    assert row["redemptions"] == 20.05
    assert row["shareholder_count"] == 123
    assert row["raw_captc_dia"] == "50,10"
    assert row["raw_resg_dia"] == "20,05"


def test_cvm_daily_current_snapshot_without_first_seen_is_reference_only():
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
                "raw_path": "raw.zip",
                "sha256": "abc",
            }
        ]
    )

    row = normalize_cvm_fund_daily_reports_to_silver(bronze).to_dicts()[0]

    assert row["available_date"] == date(2024, 1, 9)
    assert row["availability_policy"] == CVM_DAILY_REFERENCE_ONLY_POLICY
    assert row["availability_basis"] == AVAILABILITY_CONSERVATIVE_HEURISTIC
    assert row["revision_policy"] == REVISION_CURRENT_SNAPSHOT_REFERENCE_ONLY
    assert row["model_usable"] is False


def test_cvm_daily_delivery_metadata_timestamp_is_model_usable():
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
                "delivery_datetime_utc": datetime(2024, 1, 8, 12),
                "raw_path": "raw.zip",
                "sha256": "delivery-hash",
            }
        ]
    )

    row = normalize_cvm_fund_daily_reports_to_silver(bronze).to_dicts()[0]

    assert row["available_date"] == date(2024, 1, 8)
    assert row["availability_policy"] == CVM_DAILY_DELIVERY_METADATA_POLICY
    assert row["availability_basis"] == AVAILABILITY_EXACT_SOURCE_TIMESTAMP
    assert row["revision_policy"] == REVISION_REVISED_USE_VINTAGES
    assert row["release_date"] == date(2024, 1, 8)
    assert row["model_usable"] is True


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
    assert row["availability_policy"] == CVM_REGISTRY_CURRENT_REFERENCE_POLICY
    assert row["availability_basis"] == AVAILABILITY_CURRENT_SNAPSHOT_NO_VINTAGE
    assert row["revision_policy"] == REVISION_CURRENT_SNAPSHOT_REFERENCE_ONLY
    assert row["model_usable"] is False
    assert row["model_usable_reason"] == CVM_REGISTRY_CURRENT_REFERENCE_POLICY
