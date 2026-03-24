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


class BronzeGammaTagSchema(dy.Schema):
    """Schema for the Gamma tag list dataset (`bronze_gamma_tags` table)."""

    tag_id = dy.String(primary_key=True, min_length=1, max_length=255)
    tag_slug = dy.String(min_length=1, max_length=255)

    label = dy.String(nullable=True, min_length=1, max_length=255)

    force_show = dy.Bool(nullable=True)
    force_hide = dy.Bool(nullable=True)
    is_carousel = dy.Bool(nullable=True)
    requires_translation = dy.Bool(nullable=False)

    created_by = dy.String(nullable=True, max_length=255)
    updated_by = dy.String(nullable=True, max_length=255)

    created_at = dy.String(nullable=True, max_length=255)
    updated_at = dy.String(nullable=True, max_length=255)
    published_at = dy.String(nullable=True, max_length=255)

    @dy.rule(group_by=["tag_slug"])
    def _unique_tag_slug(self) -> pl.Expr:
        return pl.len() == 1

    @dy.rule(group_by=["tag_id"])
    def _unique_tag_id(self) -> pl.Expr:
        return pl.len() == 1


class BronzeGammaEventsTagLinkSchema(dy.Schema):
    """Schema for the `bronze_gamma_event_tag_link` table."""

    tag_id = dy.String(primary_key=True, min_length=1, max_length=255)
    event_id = dy.String(primary_key=True, min_length=1, max_length=255)

    tag_slug = dy.String(min_length=1, max_length=255)
    event_slug = dy.String(min_length=1, max_length=255)

    @dy.rule(group_by=["event_slug", "tag_slug"])
    def _unique_event_slug_tag_slug(self) -> pl.Expr:
        return pl.len() == 1

    @dy.rule(group_by=["event_id", "tag_id"])
    def _unique_event_id_tag_id(self) -> pl.Expr:
        return pl.len() == 1


def rename_to_snake_case(col_name: str) -> str:
    """Convert a camelCase column name to snake_case.

    Adds underscores between digits and numbers. Lo-Dash-style conversion.
    """
    return pydash.snake_case(col_name)


def main() -> None:
    """Load the full tag list from API."""
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
            tags=pl.col("tags"),  # List-of-structs column.
        )
        .filter(pl.col("tags").list.len() > pl.lit(0))
        .explode("tags")
        .unnest("tags")
    )

    df = df.rename(rename_to_snake_case).rename({"id": "tag_id", "slug": "tag_slug"})

    # Due to paginated fetching (and the per-id refetching), we have duplicate rows.
    # Deduplicate based on the primary key.
    # Order not too important, but the later fetch is likely slightly more up-to-date.
    df_tag = df.drop("event_id", "event_slug").unique(
        ["tag_id"], maintain_order=True, keep="last"
    )

    df_links = df.select("event_id", "tag_id", "event_slug", "tag_slug").unique(
        ["event_id", "tag_id"], maintain_order=True, keep="last"
    )
    del df

    logger.info(f"Loaded bronze_gamma_tags: {df_tag.shape}")
    logger.info(f"Loaded bronze_gamma_event_tag_link: {df_links.shape}")

    logger.debug(f"Columns in fetched data: {df_tag.schema}")

    # Drop any extra columns.
    drop_columns = sorted(set(df_tag.columns) - set(BronzeGammaTagSchema.columns()))
    if drop_columns:
        logger.warning(
            f"Dropping {len(drop_columns)} extra columns that are "
            f"not in the Dataframely schema: {drop_columns}"
        )
        df_tag = df_tag.drop(drop_columns)

    assert set(df_tag.columns) == set(BronzeGammaTagSchema.columns()), (
        f"Extra columns: {set(df_tag.columns) - set(BronzeGammaTagSchema.columns())}, "
        f"missing columns: {set(BronzeGammaTagSchema.columns()) - set(df_tag.columns)}"
    )
    BronzeGammaTagSchema.filter(df_tag, cast=True)[1].write_parquet(
        OUTPUT_FOLDER / "tag_schema_failures.parquet"
    )
    df_tag = BronzeGammaTagSchema.validate(df_tag, cast=True)

    # Check for any 100%-null columns (potentially remove from schema).
    null_cols = [
        col for col in df_tag.columns if df_tag[col].null_count() == df_tag.height
    ]
    if null_cols:
        logger.warning(
            f"The following columns are 100% null and may be candidates for removal "
            f"from the schema: {null_cols}"
        )

    df_tag.write_parquet(OUTPUT_FOLDER / "bronze_gamma_tags.parquet")
    df_tag.write_csv(OUTPUT_FOLDER / "bronze_gamma_tags.csv")
    write_final_dolt_table(df_tag, "bronze_gamma_tags")

    # Final steps for the links table too. Much simpler.
    assert set(df_links.columns) == set(BronzeGammaEventsTagLinkSchema.column_names())
    df_links = BronzeGammaEventsTagLinkSchema.validate(df_links, cast=True)
    df_links.write_parquet(OUTPUT_FOLDER / "bronze_gamma_event_tag_link.parquet")
    df_links.write_csv(OUTPUT_FOLDER / "bronze_gamma_event_tag_link.csv")
    write_final_dolt_table(df_links, "bronze_gamma_event_tag_link")

    logger.success(f"Finished {Path(__file__).name}.")


if __name__ == "__main__":
    main()
