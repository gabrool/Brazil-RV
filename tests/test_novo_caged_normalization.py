from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

from bralpha.normalization.novo_caged_labor import (
    normalize_novo_caged_movements_monthly,
    normalize_novo_caged_release_calendar,
)
from bralpha.parsing.novo_caged_tabular import parse_novo_caged_tabular_bytes


def test_novo_caged_movement_normalizer_maps_aliases_and_availability():
    bronze = _movement_bronze(raw_path=Path("raw-a.7z"), sha256="aaa")

    silver = normalize_novo_caged_movements_monthly(bronze)

    row = silver.to_dicts()[0]
    assert row["ref_date"] == date(2024, 1, 31)
    assert row["available_date"] == date(2024, 3, 4)
    assert row["availability_policy"] == "novo_caged_conservative_next_month_end_plus_2bd"
    assert row["state"] == "SP"
    assert row["municipality_code"] == "3550308"
    assert row["cnae_section"] == "G"
    assert row["cnae_subclass"] == "4711302"
    assert row["occupation_code"] == "411005"
    assert row["movement_sign"] == "1"
    assert row["movement_type_code"] == "10"
    assert row["wage"] == 2500.5
    assert row["contract_hours"] == 44.0
    assert row["age"] == 32
    assert row["is_apprentice"] is False
    assert row["is_intermittent"] is False
    assert row["is_part_time"] is True
    assert row["source_system"] == "eSocial"


def test_novo_caged_movement_id_excludes_raw_path_timestamp_and_hash():
    silver_a = normalize_novo_caged_movements_monthly(
        _movement_bronze(raw_path=Path("raw-a.7z"), sha256="aaa")
    )
    silver_b = normalize_novo_caged_movements_monthly(
        _movement_bronze(
            raw_path=Path("raw-b.7z"),
            sha256="bbb",
            downloaded_at=datetime(2026, 1, 5, 12, tzinfo=UTC),
        )
    )

    assert silver_a["movement_record_id"].to_list() == silver_b["movement_record_id"].to_list()
    assert silver_a["raw_path"].to_list() != silver_b["raw_path"].to_list()
    assert silver_a["sha256"].to_list() != silver_b["sha256"].to_list()


def test_novo_caged_release_calendar_normalizer_parses_official_style_rows():
    bronze = parse_novo_caged_tabular_bytes(
        (
            "<li>03/03/2026 - Competência: janeiro de 2026;</li>"
            "<li>30/06/2026 - Competência: maio de 2026;</li>"
        ).encode(),
        raw_format="html",
        source_dataset="novo_caged_release_calendar",
        resource_name="official_release_calendar",
        download_timestamp_utc=datetime(2026, 1, 5, 12, tzinfo=UTC),
        raw_path=Path("calendar.html"),
        sha256="abc",
    )

    silver = normalize_novo_caged_release_calendar(bronze)

    assert silver["ref_date"].to_list() == [date(2026, 1, 31), date(2026, 5, 31)]
    assert silver["release_date"].to_list() == [date(2026, 3, 3), date(2026, 6, 30)]
    assert silver["available_date"].to_list() == [date(2026, 3, 4), date(2026, 7, 1)]
    assert silver["availability_policy"].to_list() == [
        "novo_caged_official_release_calendar",
        "novo_caged_official_release_calendar",
    ]
    assert silver.group_by(["ref_date"]).len().height == 2


def _movement_bronze(
    *,
    raw_path: Path = Path("raw.7z"),
    sha256: str = "abc",
    downloaded_at: datetime = datetime(2024, 3, 5, 12, tzinfo=UTC),
):
    content = (
        "competência;região;uf;município;seção;subclasse;cbo2002ocupação;"
        "saldomovimentação;tipomovimentação;categoria;grau de instrução;idade;"
        "sexo;raça_cor;tipo de deficiência;tipoempregador;tipoestabelecimento;"
        "tamestabjan;horascontratuais;salário;unidadesalariocodigo;"
        "indicadoraprendiz;indtrabintermitente;indtrabparcial;origem da informação\n"
        "202401;Sudeste;SP;3550308;G;4711302;411005;1;10;101;7;32;1;2;0;"
        "0;1;5;44;2500,50;1;0;0;1;eSocial\n"
    ).encode("latin1")
    return parse_novo_caged_tabular_bytes(
        content,
        raw_format="txt",
        source_dataset="novo_caged_movements_monthly",
        resource_name="movement_records-202401",
        download_timestamp_utc=downloaded_at,
        raw_path=raw_path,
        sha256=sha256,
        period="202401",
        year=2024,
        month=1,
        record_kind="movement",
    )
