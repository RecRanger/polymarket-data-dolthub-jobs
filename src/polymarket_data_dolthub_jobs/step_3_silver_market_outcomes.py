"""Create silver_market_outcomes table from the markets table."""

from pathlib import Path

import dataframely as dy
import orjson
import polars as pl
import pydash
from loguru import logger

from polymarket_data_dolthub_jobs.path_helpers import write_final_dolt_table
from polymarket_data_dolthub_jobs.step_2a_bronze_gamma_markets import (
    OUTPUT_DATASET_PARQUET_FILE_BRONZE_GAMMA_MARKETS,
)

OUTPUT_FOLDER = Path("./out/") / Path(__file__).stem
OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)

OUTPUT_DATASET_PARQUET_FILE_SILVER_MARKET_OUTCOMES = (
    OUTPUT_FOLDER / "silver_market_outcomes.parquet"
)


class SilverMarketOutcomesSchema(dy.Schema):
    """Schema for the `silver_market_outcomes` table."""

    outcome_id = dy.String(primary_key=True, min_length=1, max_length=255)
    outcome_slug = dy.String(min_length=1, max_length=355)
    outcome_index = dy.UInt8(is_in=[0, 1])
    market_id = dy.String(min_length=1, max_length=255)
    market_slug = dy.String(min_length=1, max_length=255)
    question = dy.String(min_length=1, max_length=255)
    outcome_name = dy.String(max_length=100)  # Must not have a min length.
    outcome_price = dy.Float64(nullable=True)
    clob_token_id = dy.String(min_length=1, max_length=255)

    @dy.rule(group_by=["outcome_id"])
    def _unique_outcome_id(self) -> pl.Expr:
        return pl.len() == 1

    # Outcome names can surprisingly be duplicated within a market.
    # Example: Ymer vs. Ymer - https://polymarket.com/sports/atp/atp-ymer-ymer4-2025-10-13
    # Not unique on - @dy.rule(group_by=["market_slug", "outcome_name"])
    # Not unique on - @dy.rule(group_by=["market_id", "outcome_name"])
    # Not unique on - @dy.rule(group_by=["outcome_slug"])

    @dy.rule(group_by=["clob_token_id"])
    def _unique_clob_token_id(self) -> pl.Expr:
        return pl.len() == 1


def main() -> None:
    """Construct the silver_market_outcomes table from the bronze_gamma_markets table.

    Basically explode the outcomes lists.
    """
    logger.info(f"Starting {Path(__file__).name}")

    df = pl.read_parquet(OUTPUT_DATASET_PARQUET_FILE_BRONZE_GAMMA_MARKETS)
    logger.info(f"Loaded markets dataset: {df.shape}")

    # Parse the nested JSON-as-string columns.
    df = df.with_columns(
        pl.col("outcomes").map_elements(orjson.loads, return_dtype=pl.List(pl.String)),
        pl.col("outcome_prices").map_elements(
            orjson.loads, return_dtype=pl.List(pl.Float64)
        ),
        pl.col("clob_token_ids").map_elements(
            orjson.loads, return_dtype=pl.List(pl.String)
        ),
    )

    # Validate long outcome lists.
    df_long_outcomes_issue = df.filter(
        (pl.col("outcomes").list.len() > pl.lit(2))
        | (pl.col("outcome_prices").list.len() > pl.lit(2))
        | (pl.col("clob_token_ids").list.len() > pl.lit(2))
    )
    if df_long_outcomes_issue.height > 0:
        logger.warning(
            f"Found {df_long_outcomes_issue.height} rows with more than 2 outcomes. "
            "Outcomes past the 2nd one will be truncated."
        )
    del df_long_outcomes_issue

    # Remove old markets with missing clob_token_ids (upstream data issue).
    df = df.filter(pl.col("clob_token_ids").list.len() == pl.lit(2))

    # Convert to structs to explode.
    df = (
        df.with_columns(
            pl.struct(["outcomes", "outcome_prices", "clob_token_ids"])
            .map_elements(
                lambda row: [
                    {
                        "outcome_index": i,
                        "outcome": (row["outcomes"][i] if row["outcomes"] else None),
                        "outcome_price": (
                            row["outcome_prices"][i] if row["outcome_prices"] else None
                        ),
                        "clob_token_id": (
                            row["clob_token_ids"][i] if row["clob_token_ids"] else None
                        ),
                    }
                    for i in range(2)
                ],
                return_dtype=pl.List(
                    pl.Struct(
                        {
                            "outcome_index": pl.Int64(),
                            "outcome": pl.String(),
                            "outcome_price": pl.Float64(),
                            "clob_token_id": pl.String(),
                        }
                    )
                ),
            )
            .alias("outcome_struct")
        )
        .explode("outcome_struct")
        .unnest("outcome_struct")
    )

    df = df.select(
        # Construct an internal PK ID for the specific outcome.
        # Note that slugs change, so we must use the market_id as the stable identifier.
        outcome_id=pl.concat_str(
            pl.col("market_id"),
            pl.col("outcome_index").cast(pl.String),
            separator="-",
        ),
        outcome_index=pl.col("outcome_index").cast(pl.UInt8),
        # Construct an internal unique ID for the specific outcome.
        outcome_slug=pl.concat_str(
            pl.col("market_slug"),
            pl.col("outcome").map_elements(pydash.kebab_case),
            separator="@",
        ),
        market_id=pl.col("market_id"),
        market_slug=pl.col("market_slug"),
        question=pl.col("question"),
        outcome_name=pl.col("outcome"),
        outcome_price=pl.col("outcome_price"),
        clob_token_id=pl.col("clob_token_id"),
    )

    assert set(df.columns) == set(SilverMarketOutcomesSchema.columns())
    SilverMarketOutcomesSchema.filter(df, cast=True)[1].write_parquet(
        OUTPUT_FOLDER / "schema_failures.parquet"
    )
    df = SilverMarketOutcomesSchema.validate(df, cast=True)

    logger.info(f"Transformed to silver_market_outcomes dataset: {df.shape}")

    df.write_parquet(OUTPUT_DATASET_PARQUET_FILE_SILVER_MARKET_OUTCOMES)
    df.write_csv(OUTPUT_FOLDER / "silver_market_outcomes.csv")

    write_final_dolt_table(df, "silver_market_outcomes")

    logger.success(f"Finished {Path(__file__).name}: {df.shape}")


if __name__ == "__main__":
    main()
