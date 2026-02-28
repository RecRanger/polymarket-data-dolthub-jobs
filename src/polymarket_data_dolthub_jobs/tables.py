"""Collect the schemas for all tables in one place."""

from polymarket_data_dolthub_jobs.step_2a_bronze_gamma_markets import (
    BronzeGammaMarketsSchema,
)
from polymarket_data_dolthub_jobs.step_3_silver_market_outcomes import (
    SilverMarketOutcomesSchema,
)

TABLES_TO_SCHEMAS = {
    "bronze_gamma_markets": BronzeGammaMarketsSchema,
    "silver_market_outcomes": SilverMarketOutcomesSchema,
}
