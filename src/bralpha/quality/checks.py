from __future__ import annotations

import polars as pl


class QualityCheckError(ValueError):
    pass


def check_row_count_not_zero(frame: pl.DataFrame) -> None:
    if frame.height == 0:
        raise QualityCheckError("row_count_not_zero failed")


def check_required_columns_present(frame: pl.DataFrame, required_columns: list[str]) -> None:
    missing = [column for column in required_columns if column not in frame.columns]
    if missing:
        raise QualityCheckError(f"required_columns_present failed: {missing}")


def check_no_duplicate_primary_keys(frame: pl.DataFrame, primary_keys: list[str]) -> None:
    check_required_columns_present(frame, primary_keys)
    duplicate_count = frame.group_by(primary_keys).len().filter(pl.col("len") > 1).height
    if duplicate_count:
        raise QualityCheckError(
            f"no_duplicate_primary_keys failed: {duplicate_count} duplicate keys"
        )


def check_not_null(frame: pl.DataFrame, column: str) -> None:
    check_required_columns_present(frame, [column])
    if frame.filter(pl.col(column).is_null()).height:
        raise QualityCheckError(f"{column}_not_null failed")


def check_positive_where_present(frame: pl.DataFrame, column: str) -> None:
    bad = column in frame.columns and frame.filter(
        pl.col(column).is_not_null() & (pl.col(column) <= 0)
    ).height
    if bad:
        raise QualityCheckError(f"positive_{column}_where_present failed")


def check_nonnegative_where_present(frame: pl.DataFrame, column: str) -> None:
    bad = column in frame.columns and frame.filter(
        pl.col(column).is_not_null() & (pl.col(column) < 0)
    ).height
    if bad:
        raise QualityCheckError(f"nonnegative_{column}_where_present failed")


def check_available_date_on_or_after_ref_date(frame: pl.DataFrame) -> None:
    check_required_columns_present(frame, ["ref_date", "available_date"])
    if frame.filter(pl.col("available_date") < pl.col("ref_date")).height:
        raise QualityCheckError("available_date_on_or_after_ref_date failed")


def check_rate_within_plausible_bounds(frame: pl.DataFrame, column: str = "rate") -> None:
    if column in frame.columns:
        bad = frame.filter(pl.col(column).is_not_null() & ~pl.col(column).is_between(-1.0, 2.0))
        if bad.height:
            raise QualityCheckError("rate_within_plausible_bounds failed")


def run_quality_checks(
    frame: pl.DataFrame,
    *,
    check_names: list[str],
    primary_keys: list[str],
    required_columns: list[str],
) -> None:
    for check_name in check_names:
        if check_name == "row_count_not_zero":
            check_row_count_not_zero(frame)
        elif check_name == "no_duplicate_primary_keys":
            check_no_duplicate_primary_keys(frame, primary_keys)
        elif check_name == "required_columns_present":
            check_required_columns_present(frame, required_columns)
        elif check_name in {"ref_date_not_null", "symbol_not_null"}:
            check_not_null(frame, check_name.removesuffix("_not_null"))
        elif check_name == "available_date_not_null":
            check_not_null(frame, "available_date")
        elif check_name == "available_date_on_or_after_ref_date":
            check_available_date_on_or_after_ref_date(frame)
        elif check_name == "positive_settlement_where_present":
            check_positive_where_present(frame, "settlement")
        elif check_name == "positive_index_value":
            column = "close" if "close" in frame.columns else "index_value"
            check_positive_where_present(frame, column)
        elif check_name in {"nonnegative_open_interest", "nonnegative_volume"}:
            check_nonnegative_where_present(frame, check_name.removeprefix("nonnegative_"))
        elif check_name == "nonnegative_financial_volume_where_present":
            check_nonnegative_where_present(frame, "financial_volume")
        elif check_name == "nonnegative_prices_where_present":
            for column in ["open", "high", "low", "close"]:
                check_nonnegative_where_present(frame, column)
        elif check_name == "nonnegative_weight_where_present":
            check_nonnegative_where_present(frame, "weight")
        elif check_name == "rate_within_plausible_bounds":
            check_rate_within_plausible_bounds(frame)
        elif check_name in {"downloaded_file_present", "maturity_date_present_where_required"}:
            continue
        else:
            raise QualityCheckError(f"Unknown quality check: {check_name}")
