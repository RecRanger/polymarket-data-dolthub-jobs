"""Run all steps in the processing pipeline."""

from loguru import logger


def main() -> None:
    """Run all steps in the processing pipeline."""
    logger.info("Starting the data processing pipeline.")
    from polymarket_data_dolthub_jobs.step_1_download_raw_gamma import (  # noqa: PLC0415
        main as step_1_download_raw_gamma_main,
    )
    from polymarket_data_dolthub_jobs.step_2a_bronze_gamma_markets import (  # noqa: PLC0415
        main as step_2a_bronze_gamma_markets_main,
    )
    from polymarket_data_dolthub_jobs.step_3_silver_market_outcomes import (  # noqa: PLC0415
        main as step_3_silver_market_outcomes_main,
    )

    step_1_download_raw_gamma_main()
    step_2a_bronze_gamma_markets_main()
    step_3_silver_market_outcomes_main()

    logger.info("Data processing pipeline completed successfully.")


if __name__ == "__main__":
    main()
