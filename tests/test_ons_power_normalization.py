from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl

from bralpha.normalization.ons_power import normalize_ons_to_silver
from bralpha.parsing.ons_tabular import parse_ons_tabular_bytes


def test_ons_ear_normalization_preserves_official_values_and_availability():
    bronze = _bronze(
        "ons_ear_subsystem_daily",
        (
            "id_subsistema;nom_subsistema;ear_data;ear_max_subsistema;"
            "ear_verif_subsistema_mwmes;ear_verif_subsistema_percentual\n"
            "SE;Sudeste;2024-01-05;100,5;50,25;49,98\n"
        ),
    )

    silver = normalize_ons_to_silver("ons_ear_subsystem_daily", bronze)

    assert silver["ref_date"].to_list() == [date(2024, 1, 5)]
    assert silver["available_date"].to_list() == [date(2024, 1, 8)]
    assert silver["stored_energy_mwmes"].to_list() == [50.25]
    assert silver["raw_ear_verif_subsistema_mwmes"].to_list() == ["50,25"]


def test_ons_ena_normalization_emits_long_type_rows():
    bronze = _bronze(
        "ons_ena_subsystem_daily",
        (
            "id_subsistema;nom_subsistema;ena_data;ena_bruta_regiao_mwmed;"
            "ena_bruta_regiao_percentualmlt;ena_armazenavel_regiao_mwmed;"
            "ena_armazenavel_regiao_percentualmlt\n"
            "SE;Sudeste;2024-01-02;10;11;12;13\n"
        ),
    )

    silver = normalize_ons_to_silver("ons_ena_subsystem_daily", bronze)

    assert silver.height == 4
    assert silver["ena_type"].to_list() == [
        "bruta_mwmed",
        "bruta_percentual_mlt",
        "armazenavel_mwmed",
        "armazenavel_percentual_mlt",
    ]
    assert silver["ena_value"].to_list() == [10.0, 11.0, 12.0, 13.0]


def test_ons_load_normalization_maps_static_methodology_note_buckets():
    bronze = _bronze(
        "ons_load_daily",
        (
            "id_subsistema;nom_subsistema;din_instante;val_cargaenergiamwmed\n"
            "SE;Sudeste;2021-02-28;1\n"
            "SE;Sudeste;2021-03-01;2\n"
            "SE;Sudeste;2023-04-29;3\n"
        ),
    )

    silver = normalize_ons_to_silver("ons_load_daily", bronze)

    assert silver["load_mwmed"].to_list() == [1.0, 2.0, 3.0]
    assert silver["methodology_note"].to_list() == [
        "pre_2021_03_scada_dispatched_programmed_basis",
        "2021_03_to_2023_04_28_includes_non_dispatched_generation_estimate",
        "from_2023_04_29_includes_estimated_mmgd",
    ]


def test_ons_cmo_normalization_preserves_weekly_load_blocks_without_daily_fill():
    bronze = _bronze(
        "ons_cmo_weekly",
        (
            "id_subsistema;nom_subsistema;din_instante;val_cmomediasemanal;"
            "val_cmoleve;val_cmomedia;val_cmopesada\n"
            "SE;Sudeste;2024-01-06;100;90;101;120\n"
        ),
    )

    silver = normalize_ons_to_silver("ons_cmo_weekly", bronze)

    assert silver.height == 4
    assert silver["load_block"].to_list() == ["media_semanal", "leve", "media", "pesada"]
    assert silver["cmo_brl_mwh"].to_list() == [100.0, 90.0, 101.0, 120.0]


def test_ons_energy_balance_normalization_preserves_hourly_fields():
    bronze = _bronze(
        "ons_energy_balance_subsystem",
        (
            "id_subsistema;nom_subsistema;din_instante;val_carga;val_gerhidraulica;"
            "val_gertermica;val_gereolica;val_gersolar;val_intercambio\n"
            "SE;Sudeste;2024-01-02 01:00:00;100;40;30;20;10;-5\n"
        ),
    )

    silver = normalize_ons_to_silver("ons_energy_balance_subsystem", bronze)

    assert silver["ref_datetime"].to_list() == [datetime(2024, 1, 2, 1)]
    assert silver["ref_date"].to_list() == [date(2024, 1, 2)]
    assert silver["interchange_mwmed"].to_list() == [-5.0]
    assert silver["other_generation_mwmed"].to_list() == [None]


def test_ons_interchange_normalization_preserves_directional_hourly_rows():
    bronze = _bronze(
        "ons_interchange_subsystem_hourly",
        (
            "din_instante;id_subsistema_origem;nom_subsistema_origem;"
            "id_subsistema_destino;nom_subsistema_destino;val_intercambiomwmed;"
            "val_intercambioprogmwmed\n"
            "2024-01-02 01:00:00;SE;Sudeste;NE;Nordeste;123;120\n"
        ),
    )

    silver = normalize_ons_to_silver("ons_interchange_subsystem_hourly", bronze)

    assert silver["source_subsystem"].to_list() == ["Sudeste"]
    assert silver["target_subsystem"].to_list() == ["Nordeste"]
    assert silver["interchange_mwmed"].to_list() == [123.0]
    assert silver["programmed_interchange_mwmed"].to_list() == [120.0]
    assert (
        silver.group_by(["ref_datetime", "source_subsystem", "target_subsystem"]).len().height
        == 1
    )


def _bronze(dataset_id: str, text: str) -> pl.DataFrame:
    return parse_ons_tabular_bytes(
        text.encode("utf-8"),
        raw_format="csv_annual",
        source_dataset=dataset_id,
        resource_name=f"{dataset_id}-2024",
        year=2024,
        download_timestamp_utc=datetime(2024, 1, 3, 12, tzinfo=UTC),
        raw_path=Path(f"{dataset_id}.csv"),
        sha256="abc",
    )
