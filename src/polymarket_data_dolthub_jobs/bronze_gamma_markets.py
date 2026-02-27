"""Load the full markets list dataset from API and store it to DoltHub."""

from pathlib import Path
from typing import Any

import dataframely as dy
import orjson
import polars as pl
import pydash
from loguru import logger

from polymarket_data_dolthub_jobs.request_helpers import url_get_request

OUTPUT_FOLDER = Path("./out/") / Path(__file__).stem
OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)

OUTPUT_FOLDER_RAW_PAGES = OUTPUT_FOLDER / "raw_pages"
OUTPUT_FOLDER_RAW_PAGES.mkdir(parents=True, exist_ok=True)

OUTPUT_DATASET_PARQUET_FILE_BRONZE_GAMMA_MARKETS = (
    OUTPUT_FOLDER / "bronze_gamma_markets.parquet"
)


class BronzeGammaMarketsSchema(dy.Schema):
    """Schema for the Gamma markets list dataset (`bronze_gamma_markets` table)."""

    id = dy.String(primary_key=True)
    question = dy.String()
    condition_id = dy.String()
    slug = dy.String()
    resolution_source = dy.String(nullable=True)
    end_date = dy.String(nullable=True)
    liquidity = dy.String(nullable=True)
    start_date = dy.String(nullable=True)
    image = dy.String(nullable=True)
    icon = dy.String(nullable=True)
    description = dy.String(nullable=True)
    outcomes = dy.String(nullable=True)
    outcome_prices = dy.String(nullable=True)
    active = dy.Bool(nullable=True)
    closed = dy.Bool(nullable=True)
    market_maker_address = dy.String(nullable=True)
    created_at = dy.String(nullable=True)
    updated_at = dy.String(nullable=True)
    new = dy.Bool(nullable=True)
    featured = dy.Bool(nullable=True)
    submitted_by = dy.String(nullable=True)
    archived = dy.Bool(nullable=True)
    resolved_by = dy.String(nullable=True)
    restricted = dy.Bool(nullable=True)
    group_item_title = dy.String(nullable=True)
    question_id = dy.String(nullable=True)
    enable_order_book = dy.Bool(nullable=True)
    order_price_min_tick_size = dy.Float64(nullable=True)
    order_min_size = dy.Int64(nullable=True)
    liquidity_num = dy.Float64(nullable=True)
    end_date_iso = dy.String(nullable=True)
    start_date_iso = dy.String(nullable=True)
    has_reviewed_dates = dy.Bool(nullable=True)
    game_start_time = dy.String(nullable=True)
    seconds_delay = dy.Int64(nullable=True)
    clob_token_ids = dy.String(nullable=True)
    uma_bond = dy.String(nullable=True)
    uma_reward = dy.String(nullable=True)
    liquidity_clob = dy.Float64(nullable=True)
    custom_liveness = dy.Int64(nullable=True)
    accepting_orders = dy.Bool(nullable=True)
    neg_risk = dy.Bool(nullable=True)
    neg_risk_request_id = dy.String(nullable=True)
    ready = dy.Bool(nullable=True)
    funded = dy.Bool(nullable=True)
    accepting_orders_timestamp = dy.String(nullable=True)
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
    clear_book_on_start = dy.Bool(nullable=True)
    manual_activation = dy.Bool(nullable=True)
    neg_risk_other = dy.Bool(nullable=True)
    game_id = dy.String(nullable=True)
    sports_market_type = dy.String(nullable=True)
    uma_resolution_statuses = dy.String(nullable=True)
    pending_deployment = dy.Bool(nullable=True)
    deploying = dy.Bool(nullable=True)
    deploying_timestamp = dy.String(nullable=True)
    rfq_enabled = dy.Bool(nullable=True)
    event_start_time = dy.String(nullable=True)
    holding_rewards_enabled = dy.Bool(nullable=True)
    fees_enabled = dy.Bool(nullable=True)
    requires_translation = dy.Bool(nullable=True)
    fee_type = dy.String(nullable=True)
    line = dy.Float64(nullable=True)
    group_item_threshold = dy.String(nullable=True)
    maker_base_fee = dy.Int64(nullable=True)
    taker_base_fee = dy.Int64(nullable=True)
    show_gmp_series = dy.Bool(nullable=True)
    show_gmp_outcome = dy.Bool(nullable=True)
    maker_rebates_fee_share_bps = dy.Int64(nullable=True)
    volume = dy.String(nullable=True)
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

    @dy.rule(group_by=["slug"])
    def _unique_slug(self) -> pl.Expr:
        return pl.len() == 1


def fetch_all_data() -> pl.DataFrame:
    """Load the full markets list dataset from API and store it to DoltHub."""
    offset: int = 0
    page_size: int = 100

    df_pages: list[pl.DataFrame] = []

    while True:
        page_data: list[dict[str, Any]] = url_get_request(
            f"https://gamma-api.polymarket.com/markets?active=true&closed=false&limit={page_size}&offset={offset}"
        )
        assert isinstance(page_data, list)
        assert all(isinstance(market, dict) for market in page_data)

        (
            OUTPUT_FOLDER_RAW_PAGES / f"markets_page_{offset // page_size}.json"
        ).write_bytes(orjson.dumps(page_data, option=orjson.OPT_INDENT_2))

        # Minor transforms for certain nested fields.
        page_data_clean = [pydash.omit(row, ["events", "series"]) for row in page_data]

        df = pl.DataFrame(page_data_clean)

        df = df.rename(rename_to_snake_case)

        # Add missing columns.
        for col, dtype in BronzeGammaMarketsSchema.to_polars_schema().items():
            if col not in df.columns:
                df = df.with_columns(pl.lit(None, dtype=dtype).alias(col))

        df = BronzeGammaMarketsSchema.validate(df, cast=True)

        df_pages.append(df)
        offset += page_size

        if offset % 1000 == 0:
            logger.debug(
                f"Fetched page {offset / page_size:.0f} with {offset=}, {df.height=}."
            )

        if df.height < page_size:
            logger.info("Reached the end of the markets list.")
            break

    return pl.concat(df_pages, how="diagonal")


def rename_to_snake_case(col_name: str) -> str:
    """Convert a camelCase column name to snake_case.

    Adds underscores between digits and numbers. Lo-Dash-style conversion.
    """
    return pydash.snake_case(col_name)


def main() -> None:
    """Load the full markets list dataset from API and store it to DoltHub."""
    logger.info(f"Starting {Path(__file__).name}")

    df = fetch_all_data()
    logger.info(f"Fetched {df.height} rows of market data: {df.shape}")

    # Due to paginated fetching, we may have duplicate rows.
    # Deduplicate based on the primary key.
    # Order not too important, but the later fetch is likely slightly more up-to-date.
    df = df.unique(["id"], maintain_order=True, keep="last")

    assert set(df.columns) == set(BronzeGammaMarketsSchema.columns())
    df = BronzeGammaMarketsSchema.validate(df, cast=True)

    # Check for any 100%-null columns (potentially remove from schema).
    null_cols = [col for col in df.columns if df[col].null_count() == df.height]
    if null_cols:
        logger.warning(
            f"The following columns are 100% null and may be candidates for removal "
            f"from the schema: {null_cols}"
        )

    df.write_parquet(OUTPUT_DATASET_PARQUET_FILE_BRONZE_GAMMA_MARKETS)  # Main output.
    df.write_csv(OUTPUT_FOLDER / "markets_list.csv")

    logger.success(f"Finished {Path(__file__).name}: {df.shape}")


if __name__ == "__main__":
    main()
