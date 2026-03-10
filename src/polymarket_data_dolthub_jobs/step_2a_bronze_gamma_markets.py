"""Transform raw JSON into validated table."""

from pathlib import Path

import dataframely as dy
import orjson
import polars as pl
import pydash
from loguru import logger

from polymarket_data_dolthub_jobs.path_helpers import write_final_dolt_table
from polymarket_data_dolthub_jobs.step_1_download_raw_gamma import (
    OUTPUT_EVENTS_JSON_FILE,
)

OUTPUT_FOLDER = Path("./out/") / Path(__file__).stem
OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)

OUTPUT_DATASET_PARQUET_FILE_BRONZE_GAMMA_MARKETS = (
    OUTPUT_FOLDER / "bronze_gamma_markets.parquet"
)


class BronzeGammaMarketsSchema(dy.Schema):
    """Schema for the Gamma markets list dataset (`bronze_gamma_markets` table)."""

    market_id = dy.String(primary_key=True, min_length=1, max_length=255)
    market_slug = dy.String(min_length=1, max_length=255)
    event_id = dy.String(min_length=1, max_length=255)
    event_slug = dy.String(min_length=1, max_length=255)
    question = dy.String(min_length=1, max_length=255)
    condition_id = dy.String(min_length=66, max_length=66)  # "0x" + 64 hex bytes
    resolution_source = dy.String(nullable=True, max_length=255)
    end_date = dy.String(nullable=True, max_length=255)
    liquidity = dy.String(nullable=True, max_length=255)
    start_date = dy.String(nullable=True, max_length=255)
    image = dy.String(nullable=True, max_length=500)
    icon = dy.String(nullable=True, max_length=500)
    description = dy.String(nullable=True, max_length=50_000)
    outcomes = dy.String(nullable=True, max_length=255)
    outcome_prices = dy.String(nullable=True, max_length=255)
    active = dy.Bool(nullable=True)
    closed = dy.Bool(nullable=True)
    closed_time = dy.String(nullable=True, max_length=255)
    market_maker_address = dy.String(nullable=True, max_length=255)
    created_at = dy.String(nullable=True, max_length=255)
    updated_at = dy.String(nullable=True, max_length=255)
    # Renamed from `new` to avoid keyword conflict in SQL.
    is_new = dy.Bool(nullable=True)
    featured = dy.Bool(nullable=True)
    submitted_by = dy.String(nullable=True, max_length=255)
    archived = dy.Bool(nullable=True)
    resolved_by = dy.String(nullable=True, max_length=255)
    restricted = dy.Bool(nullable=True)
    group_item_title = dy.String(nullable=True, max_length=255)
    question_id = dy.String(nullable=True, max_length=255)
    enable_order_book = dy.Bool(nullable=True)
    order_price_min_tick_size = dy.Float64(nullable=True)
    order_min_size = dy.Int64(nullable=True)
    liquidity_num = dy.Float64(nullable=True)
    end_date_iso = dy.String(nullable=True, max_length=255)
    start_date_iso = dy.String(nullable=True, max_length=255)
    has_reviewed_dates = dy.Bool(nullable=True)
    game_start_time = dy.String(nullable=True, max_length=255)
    seconds_delay = dy.Int64(nullable=True)
    clob_token_ids = dy.String(nullable=True, max_length=255)
    uma_bond = dy.String(nullable=True, max_length=255)
    uma_reward = dy.String(nullable=True, max_length=255)
    liquidity_clob = dy.Float64(nullable=True)
    custom_liveness = dy.Int64(nullable=True)
    accepting_orders = dy.Bool(nullable=True)
    neg_risk = dy.Bool(nullable=True)
    neg_risk_request_id = dy.String(nullable=True, max_length=255)
    ready = dy.Bool(nullable=True)
    funded = dy.Bool(nullable=True)
    accepting_orders_timestamp = dy.String(nullable=True, max_length=255)
    cyom = dy.Bool(nullable=True)
    competitive = dy.Float64(nullable=True)
    pager_duty_notification_enabled = dy.Bool(nullable=True)
    approved = dy.Bool(nullable=True)
    rewards_min_size = dy.Int64(nullable=True)
    rewards_max_spread = dy.Float64(nullable=True)
    spread = dy.Float64(nullable=True)
    best_bid = dy.Float64(nullable=True)
    best_ask = dy.Float64(nullable=True)
    automatically_active = dy.Bool(nullable=True)
    automatically_resolved = dy.Bool(nullable=True)
    clear_book_on_start = dy.Bool(nullable=True)
    manual_activation = dy.Bool(nullable=True)
    neg_risk_other = dy.Bool(nullable=True)
    game_id = dy.String(nullable=True, max_length=255)
    sports_market_type = dy.String(nullable=True, max_length=255)
    uma_resolution_status = dy.String(nullable=True, max_length=255)
    uma_resolution_statuses = dy.String(nullable=True, max_length=255)
    uma_end_date = dy.String(nullable=True, max_length=255)
    pending_deployment = dy.Bool(nullable=True)
    deploying = dy.Bool(nullable=True)
    deploying_timestamp = dy.String(nullable=True, max_length=255)
    rfq_enabled = dy.Bool(nullable=True)
    event_start_time = dy.String(nullable=True, max_length=255)
    holding_rewards_enabled = dy.Bool(nullable=True)
    fees_enabled = dy.Bool(nullable=True)
    requires_translation = dy.Bool(nullable=True)
    fee_type = dy.String(nullable=True, max_length=255)
    line = dy.Float64(nullable=True)
    group_item_threshold = dy.String(nullable=True, max_length=255)
    group_item_range = dy.String(nullable=True, max_length=255)
    maker_base_fee = dy.Int64(nullable=True)
    taker_base_fee = dy.Int64(nullable=True)
    show_gmp_series = dy.Bool(nullable=True)
    show_gmp_outcome = dy.Bool(nullable=True)
    maker_rebates_fee_share_bps = dy.Int64(nullable=True)
    volume = dy.String(nullable=True, max_length=255)
    volume_num = dy.Float64(nullable=True)
    volume_clob = dy.Float64(nullable=True)
    last_trade_price = dy.Float64(nullable=True)
    volume_24_hr = dy.Int64(nullable=True)
    volume_1_wk = dy.Int64(nullable=True)
    volume_1_mo = dy.Int64(nullable=True)
    volume_1_yr = dy.Int64(nullable=True)
    volume_24_hr_amm = dy.Int64(nullable=True)
    volume_1_wk_amm = dy.Int64(nullable=True)
    volume_1_mo_amm = dy.Int64(nullable=True)
    volume_1_yr_amm = dy.Int64(nullable=True)
    volume_24_hr_clob = dy.Int64(nullable=True)
    volume_1_wk_clob = dy.Int64(nullable=True)
    volume_1_mo_clob = dy.Int64(nullable=True)
    volume_1_yr_clob = dy.Int64(nullable=True)
    volume_amm = dy.Int64(nullable=True)
    liquidity_amm = dy.Int64(nullable=True)
    one_day_price_change = dy.Int64(nullable=True)
    one_hour_price_change = dy.Int64(nullable=True)
    one_week_price_change = dy.Int64(nullable=True)
    one_month_price_change = dy.Int64(nullable=True)
    one_year_price_change = dy.Int64(nullable=True)
    neg_risk_market_id = dy.String(nullable=True, max_length=255)
    series_color = dy.String(nullable=True, max_length=255)

    @dy.rule(group_by=["market_slug"])
    def _unique_market_slug(self) -> pl.Expr:
        return pl.len() == 1

    @dy.rule(group_by=["market_id"])
    def _unique_market_id(self) -> pl.Expr:
        return pl.len() == 1

    @dy.rule()
    def _clob_token_ids_format(self) -> pl.Expr:
        return pl.col("clob_token_ids").map_elements(
            lambda x: len(orjson.loads(x)) == 2,  # noqa: PLR2004
            return_dtype=pl.Boolean,
        )


def rename_to_snake_case(col_name: str) -> str:
    """Convert a camelCase column name to snake_case.

    Adds underscores between digits and numbers. Lo-Dash-style conversion.
    """
    return pydash.snake_case(col_name)


def main() -> None:
    """Load the full markets list dataset from API."""
    logger.info(f"Starting {Path(__file__).name}")

    df = pl.read_json(OUTPUT_EVENTS_JSON_FILE, infer_schema_length=None)
    logger.info(f"Read JSON: {df.shape}")

    df = (
        df.select(
            event_id=pl.col("id"),
            event_slug=pl.col("slug"),
            markets=pl.col("markets"),  # List-of-structs column.
        )
        .explode("markets")
        .unnest("markets")
        .drop("clobRewards")
    )

    df = df.rename(rename_to_snake_case).rename(
        {"id": "market_id", "slug": "market_slug", "new": "is_new"}
    )

    # Due to paginated fetching, we may have duplicate rows.
    # Deduplicate based on the primary key.
    # Order not too important, but the later fetch is likely slightly more up-to-date.
    df = df.unique(["market_id"], maintain_order=True, keep="last")

    # Filter out any rows with empty string `condition_id`, which indicates a data
    # issue. It seems that these markets are ones that were deleted or vanished.
    df = df.filter(pl.col("condition_id") != pl.lit("", dtype=pl.String))

    # Filter out old cases with more than 2 outcomes. Only binary markets are supported.
    df = df.filter(
        pl.col("outcomes").map_elements(
            lambda x: len(orjson.loads(x)) == 2,  # noqa: PLR2004
            return_dtype=pl.Boolean,
        )
        # Keep recent markets so they fail the validation later. Used in case they
        # start using 3+ outcome markets again.
        | (pl.col("start_date").str.extract(r"^(\d{4})").cast(pl.UInt16) > pl.lit(2023))
    )

    # Nullify any long image/icon URLs.
    df = df.with_columns(
        pl.when(pl.col(col_name).str.len_bytes() > pl.lit(500))
        .then(pl.lit(None))
        .otherwise(pl.col(col_name))
        .alias(col_name)
        for col_name in ["image", "icon"]
    )

    logger.debug(f"Columns in fetched data: {df.schema}")

    # Drop any extra columns.
    drop_columns = sorted(set(df.columns) - set(BronzeGammaMarketsSchema.columns()))
    if drop_columns:
        logger.warning(
            f"Dropping {len(drop_columns)} extra columns that are not in the schema: "
            f"{drop_columns}"
        )
        df = df.drop(drop_columns)

    assert set(df.columns) == set(BronzeGammaMarketsSchema.columns()), (
        f"Extra columns: {set(df.columns) - set(BronzeGammaMarketsSchema.columns())}, "
        f"missing columns: {set(BronzeGammaMarketsSchema.columns()) - set(df.columns)}"
    )
    BronzeGammaMarketsSchema.filter(df, cast=True)[1].write_parquet(
        OUTPUT_FOLDER / "schema_failures.parquet"
    )
    df = BronzeGammaMarketsSchema.validate(df, cast=True)

    # Check for any 100%-null columns (potentially remove from schema).
    null_cols = [col for col in df.columns if df[col].null_count() == df.height]
    if null_cols:
        logger.warning(
            f"The following columns are 100% null and may be candidates for removal "
            f"from the schema: {null_cols}"
        )

    df.write_parquet(OUTPUT_DATASET_PARQUET_FILE_BRONZE_GAMMA_MARKETS)
    df.write_csv(OUTPUT_FOLDER / "bronze_gamma_markets.csv")

    write_final_dolt_table(df, "bronze_gamma_markets")

    logger.success(f"Finished {Path(__file__).name}: {df.shape}")


if __name__ == "__main__":
    main()
