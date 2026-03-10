"""Transform raw JSON into validated table."""

from pathlib import Path

import dataframely as dy
import polars as pl
import pydash
from loguru import logger

from polymarket_data_dolthub_jobs.path_helpers import write_final_dolt_table
from polymarket_data_dolthub_jobs.step_1_download_raw_gamma import (
    OUTPUT_EVENTS_JSON_FILE,
)

OUTPUT_FOLDER = Path("./out/") / Path(__file__).stem
OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)

OUTPUT_DATASET_PARQUET_FILE_BRONZE_GAMMA_EVENTS = (
    OUTPUT_FOLDER / "bronze_gamma_events.parquet"
)


class BronzeGammaEventsSchema(dy.Schema):
    """Schema for the Gamma events list dataset (`bronze_gamma_events` table)."""

    event_id = dy.String(primary_key=True, min_length=1, max_length=255)
    event_slug = dy.String(min_length=1, max_length=255)

    ticker = dy.String(min_length=1, max_length=255)
    title = dy.String(min_length=1, max_length=255)
    description = dy.String(nullable=True, max_length=50_000)
    resolution_source = dy.String(nullable=True, max_length=255)
    start_date = dy.String(nullable=True, max_length=255)
    creation_date = dy.String(nullable=True, max_length=255)
    end_date = dy.String(nullable=True, max_length=255)
    image = dy.String(nullable=True, max_length=500)
    icon = dy.String(nullable=True, max_length=500)
    active = dy.Bool(nullable=True)
    closed = dy.Bool(nullable=True)
    archived = dy.Bool(nullable=True)
    is_new = dy.Bool(nullable=True)
    featured = dy.Bool(nullable=True)
    restricted = dy.Bool(nullable=True)
    liquidity = dy.Float64(nullable=True)
    volume = dy.Float64(nullable=True)
    open_interest = dy.Int64(nullable=True)
    created_at = dy.String(nullable=True, max_length=255)
    updated_at = dy.String(nullable=True, max_length=255)
    competitive = dy.Float64(nullable=True)
    volume_24_hr = dy.Float64(nullable=True)
    volume_1_wk = dy.Float64(nullable=True)
    volume_1_mo = dy.Float64(nullable=True)
    volume_1_yr = dy.Float64(nullable=True)
    enable_order_book = dy.Bool(nullable=True)
    liquidity_clob = dy.Float64(nullable=True)
    neg_risk = dy.Bool(nullable=True)
    comment_count = dy.Int64(nullable=True)
    cyom = dy.Bool(nullable=True)
    show_all_outcomes = dy.Bool(nullable=True)
    show_market_images = dy.Bool(nullable=True)
    enable_neg_risk = dy.Bool(nullable=True)
    automatically_active = dy.Bool(nullable=True)
    gmp_chart_mode = dy.String(nullable=True, max_length=255)
    neg_risk_augmented = dy.Bool(nullable=True)
    cumulative_markets = dy.Bool(nullable=True)
    pending_deployment = dy.Bool(nullable=True)
    deploying = dy.Bool(nullable=True)
    requires_translation = dy.Bool(nullable=True)
    neg_risk_market_id = dy.String(nullable=True, max_length=255)
    series_slug = dy.String(nullable=True, max_length=255)
    estimate_value = dy.Bool(nullable=True)
    sort_by = dy.String(nullable=True, max_length=255)
    deploying_timestamp = dy.String(nullable=True, max_length=255)
    event_date = dy.String(nullable=True, max_length=255)
    start_time = dy.String(nullable=True, max_length=255)
    color = dy.String(nullable=True, max_length=255)
    created_by = dy.String(nullable=True, max_length=255)
    country_name = dy.String(nullable=True, max_length=255)
    election_type = dy.String(nullable=True, max_length=255)
    featured_order = dy.Int64(nullable=True)
    event_week = dy.Int64(nullable=True)
    score = dy.String(nullable=True, max_length=255)
    elapsed = dy.String(nullable=True, max_length=255)
    period = dy.String(nullable=True, max_length=255)
    live = dy.Bool(nullable=True)
    ended = dy.Bool(nullable=True)
    game_id = dy.Int64(nullable=True)
    finished_timestamp = dy.String(nullable=True, max_length=255)
    cant_estimate = dy.Bool(nullable=True)
    parent_event_id = dy.Int64(nullable=True)
    estimated_value = dy.String(nullable=True, max_length=255)
    tweet_count = dy.Int64(nullable=True)
    liquidity_amm = dy.Int64(nullable=True)

    @dy.rule(group_by=["event_slug"])
    def _unique_event_slug(self) -> pl.Expr:
        return pl.len() == 1

    @dy.rule(group_by=["event_id"])
    def _unique_event_id(self) -> pl.Expr:
        return pl.len() == 1


def rename_to_snake_case(col_name: str) -> str:
    """Convert a camelCase column name to snake_case.

    Adds underscores between digits and numbers. Lo-Dash-style conversion.
    """
    return pydash.snake_case(col_name)


def main() -> None:
    """Load the full events list dataset from API."""
    logger.info(f"Starting {Path(__file__).name}")

    df = pl.read_json(OUTPUT_EVENTS_JSON_FILE, infer_schema_length=None)
    logger.info(f"Read JSON: {df.shape}")

    df = df.drop("markets", "tags", "series", "eventCreators", "eventMetadata")

    df = df.rename(rename_to_snake_case).rename(
        {"id": "event_id", "slug": "event_slug", "new": "is_new"}
    )

    # Due to paginated fetching, we may have duplicate rows.
    # Deduplicate based on the primary key.
    # Order not too important, but the later fetch is likely slightly more up-to-date.
    df = df.unique(["event_id"], maintain_order=True, keep="last")

    logger.debug(f"Columns in fetched data: {df.schema}")

    # Add sometime-missing unimportant columns.
    # Alternatively, we could drop these columns when they appear (and drop from the
    # schema).
    for col in ("liquidity_amm",):
        if col not in df.columns:
            logger.info(f'"{col}" column not in source. Adding it with null values.')
            df = df.with_columns(pl.lit(None).alias(col))

    assert set(df.columns) == set(BronzeGammaEventsSchema.columns()), (
        f"Extra columns: {set(df.columns) - set(BronzeGammaEventsSchema.columns())}, "
        f"missing columns: {set(BronzeGammaEventsSchema.columns()) - set(df.columns)}"
    )
    BronzeGammaEventsSchema.filter(df, cast=True)[1].write_parquet(
        OUTPUT_FOLDER / "schema_failures.parquet"
    )
    df = BronzeGammaEventsSchema.validate(df, cast=True)

    # Check for any 100%-null columns (potentially remove from schema).
    null_cols = [col for col in df.columns if df[col].null_count() == df.height]
    if null_cols:
        logger.warning(
            f"The following columns are 100% null and may be candidates for removal "
            f"from the schema: {null_cols}"
        )

    df.write_parquet(OUTPUT_DATASET_PARQUET_FILE_BRONZE_GAMMA_EVENTS)
    df.write_csv(OUTPUT_FOLDER / "bronze_gamma_events.csv")

    write_final_dolt_table(df, "bronze_gamma_events")

    logger.success(f"Finished {Path(__file__).name}: {df.shape}")


if __name__ == "__main__":
    main()
