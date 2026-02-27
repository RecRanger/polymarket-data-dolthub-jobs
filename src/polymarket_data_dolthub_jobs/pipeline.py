"""Run all steps in the processing pipeline."""

from loguru import logger


def main() -> None:
    """Run all steps in the processing pipeline."""
    logger.info("Starting the data processing pipeline.")
    from polymarket_data_dolthub_jobs.bronze_gamma_markets import (  # noqa: PLC0415
        main as bronze_gamma_markets_main,
    )
    from polymarket_data_dolthub_jobs.silver_market_outcomes import (  # noqa: PLC0415
        main as silver_market_outcomes_main,
    )

    bronze_gamma_markets_main()
    silver_market_outcomes_main()

    logger.info("Data processing pipeline completed successfully.")


if __name__ == "__main__":
    main()
