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
from polymarket_data_dolthub_jobs.step_1b_download_raw_gamma_by_id import (
    OUTPUT_EVENTS_JSON_FILE_BY_ID,
)

OUTPUT_FOLDER = Path("./out/") / Path(__file__).stem
OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)


class BronzeGammaSeriesSchema(dy.Schema):
    """Schema for the Gamma series list dataset (`bronze_gamma_series` table)."""

    series_id = dy.String(primary_key=True, min_length=1, max_length=255)
    series_slug = dy.String(min_length=1, max_length=255)

    ticker = dy.String(min_length=1, max_length=255)

    title = dy.String(min_length=1, max_length=255)
    series_type = dy.String(nullable=True, min_length=1, max_length=255)
    recurrence = dy.String(nullable=True, max_length=255)

    description = dy.String(nullable=True, max_length=50_000)
    subtitle = dy.String(nullable=True, min_length=1, max_length=255)

    image = dy.String(nullable=True, max_length=500)
    icon = dy.String(nullable=True, max_length=500)

    active = dy.Bool()
    closed = dy.Bool()
    archived = dy.Bool()
    is_new = dy.Bool(nullable=True)
    featured = dy.Bool(nullable=True)
    restricted = dy.Bool(nullable=True)
    comments_enabled = dy.Bool(nullable=True)
    competitive = dy.Float64(nullable=True)

    start_date = dy.String(nullable=True, max_length=255)
    created_by = dy.String(nullable=True, max_length=255)
    updated_by = dy.String(nullable=True, max_length=255)

    created_at = dy.String(nullable=True, max_length=255)
    updated_at = dy.String(nullable=True, max_length=255)
    published_at = dy.String(nullable=True, max_length=255)
    volume_24_hr = dy.Int64(nullable=True)
    liquidity = dy.Float64(nullable=True)
    volume = dy.Float64(nullable=True)

    comment_count = dy.Int32()
    requires_translation = dy.Bool(nullable=False)

    @dy.rule(group_by=["series_slug"])
    def _unique_series_slug(self) -> pl.Expr:
        return pl.len() == 1

    @dy.rule(group_by=["series_id"])
    def _unique_series_id(self) -> pl.Expr:
        return pl.len() == 1


class BronzeGammaEventsSeriesLinkSchema(dy.Schema):
    """Schema for the `bronze_gamma_event_series_link` table."""

    series_id = dy.String(primary_key=True, min_length=1, max_length=255)
    event_id = dy.String(primary_key=True, min_length=1, max_length=255)

    series_slug = dy.String(min_length=1, max_length=255)
    event_slug = dy.String(min_length=1, max_length=255)

    @dy.rule(group_by=["event_slug", "series_slug"])
    def _unique_event_slug_series_slug(self) -> pl.Expr:
        return pl.len() == 1

    @dy.rule(group_by=["event_id", "series_id"])
    def _unique_event_id_series_id(self) -> pl.Expr:
        return pl.len() == 1


def rename_to_snake_case(col_name: str) -> str:
    """Convert a camelCase column name to snake_case.

    Adds underscores between digits and numbers. Lo-Dash-style conversion.
    """
    return pydash.snake_case(col_name)


def main() -> None:
    """Load the full series list from API."""
    logger.info(f"Starting {Path(__file__).name}")

    df_1 = pl.read_json(OUTPUT_EVENTS_JSON_FILE, infer_schema_length=None)
    df_2 = pl.read_json(
        OUTPUT_EVENTS_JSON_FILE_BY_ID,
        infer_schema_length=None,
        schema=df_1.schema,
    )
    logger.info(f"Read JSON: df_1={df_1.shape}, df_2={df_2.shape}")
    df = pl.concat([df_1, df_2], how="diagonal")
    del df_1, df_2

    df = (
        df.select(
            event_id=pl.col("id"),
            event_slug=pl.col("slug"),
            series=pl.col("series"),  # List-of-structs column.
        )
        .filter(pl.col("series").list.len() > pl.lit(0))
        .explode("series")
        .unnest("series")
    )

    df = df.rename(rename_to_snake_case).rename(
        {"id": "series_id", "slug": "series_slug", "new": "is_new"}
    )

    # Due to paginated fetching (and the per-id refetching), we have duplicate rows.
    # Deduplicate based on the primary key.
    # Order not too important, but the later fetch is likely slightly more up-to-date.
    df_series = df.drop("event_id", "event_slug").unique(
        ["series_id"], maintain_order=True, keep="last"
    )

    df_links = df.select("event_id", "series_id", "event_slug", "series_slug").unique(
        ["event_id", "series_id"], maintain_order=True, keep="last"
    )
    del df

    logger.info(f"Loaded bronze_gamma_series: {df_series.shape}")
    logger.info(f"Loaded bronze_gamma_event_series_link: {df_links.shape}")

    logger.debug(f"Columns in fetched data: {df_series.schema}")

    # Drop any extra columns.
    drop_columns = sorted(
        set(df_series.columns) - set(BronzeGammaSeriesSchema.columns())
    )
    if drop_columns:
        logger.warning(
            f"Dropping {len(drop_columns)} extra columns that are "
            f"not in the Dataframely schema: {drop_columns}"
        )
        df_series = df_series.drop(drop_columns)

    # Nullify any long image/icon URLs.
    df_series = df_series.with_columns(
        pl.when(pl.col(col_name).str.len_bytes() > pl.lit(500))
        .then(pl.lit(None))
        .otherwise(pl.col(col_name))
        .alias(col_name)
        for col_name in ["image", "icon"]
    )

    # Add sometimes-missing unimportant columns.
    # Alternatively, we could drop these columns when they appear (and drop from the
    # schema).
    for col in ("description",):
        if col not in df_series.columns:
            logger.info(f'"{col}" column not in source. Adding it with null values.')
            df_series = df_series.with_columns(pl.lit(None).alias(col))

    assert set(df_series.columns) == set(BronzeGammaSeriesSchema.columns()), (
        f"Extra columns: {set(df_series.columns) - set(BronzeGammaSeriesSchema.columns())}, "  # noqa: E501
        f"missing columns: {set(BronzeGammaSeriesSchema.columns()) - set(df_series.columns)}"  # noqa: E501
    )
    BronzeGammaSeriesSchema.filter(df_series, cast=True)[1].write_parquet(
        OUTPUT_FOLDER / "series_schema_failures.parquet"
    )
    df_series = BronzeGammaSeriesSchema.validate(df_series, cast=True)

    # Check for any 100%-null columns (potentially remove from schema).
    null_cols = [
        col
        for col in df_series.columns
        if df_series[col].null_count() == df_series.height
    ]
    if null_cols:
        logger.warning(
            f"The following columns are 100% null and may be candidates for removal "
            f"from the schema: {null_cols}"
        )

    df_series.write_parquet(OUTPUT_FOLDER / "bronze_gamma_series.parquet")
    df_series.write_csv(OUTPUT_FOLDER / "bronze_gamma_series.csv")
    write_final_dolt_table(df_series, "bronze_gamma_series")

    # Final steps for the links table too. Much simpler.
    assert set(df_links.columns) == set(
        BronzeGammaEventsSeriesLinkSchema.column_names()
    )
    df_links = BronzeGammaEventsSeriesLinkSchema.validate(df_links, cast=True)
    df_links.write_parquet(OUTPUT_FOLDER / "bronze_gamma_event_series_link.parquet")
    df_links.write_csv(OUTPUT_FOLDER / "bronze_gamma_event_series_link.csv")
    write_final_dolt_table(df_links, "bronze_gamma_event_series_link")

    logger.success(f"Finished {Path(__file__).name}.")


if __name__ == "__main__":
    main()
