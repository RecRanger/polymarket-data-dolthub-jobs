"""Run all steps in the processing pipeline."""

from loguru import logger


def main() -> None:
    """Run all steps in the processing pipeline."""
    logger.info("Starting the data processing pipeline.")
    from .bronze_gamma_markets import main as bronze_gamma_markets_main  # noqa: PLC0415
    from .silver_market_outcomes import (  # noqa: PLC0415
        main as silver_market_outcomes_main,
    )

    bronze_gamma_markets_main()
    silver_market_outcomes_main()

    logger.info("Data processing pipeline completed successfully.")
