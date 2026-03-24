"""Run all steps in the processing pipeline."""

from typing import Literal

import tyro
from loguru import logger


def main(run_mode: Literal["full", "lightweight"]) -> None:
    """Run all steps in the processing pipeline."""
    logger.info("Starting the data processing pipeline.")
    from polymarket_data_dolthub_jobs.step_1_download_raw_gamma import (  # noqa: PLC0415
        main as step_1_download_raw_gamma_main,
    )
    from polymarket_data_dolthub_jobs.step_1b_download_raw_gamma_by_id import (  # noqa: PLC0415
        main as step_1b_download_raw_gamma_by_id_main,
    )
    from polymarket_data_dolthub_jobs.step_2a_bronze_gamma_markets import (  # noqa: PLC0415
        main as step_2a_bronze_gamma_markets_main,
    )
    from polymarket_data_dolthub_jobs.step_2b_bronze_gamma_events import (  # noqa: PLC0415
        main as step_2b_bronze_gamma_events_main,
    )
    from polymarket_data_dolthub_jobs.step_3_silver_market_outcomes import (  # noqa: PLC0415
        main as step_3_silver_market_outcomes_main,
    )

    step_1_run_mode_mapping: dict[
        Literal["full", "lightweight"], Literal["full", "active_only"]
    ] = {"full": "full", "lightweight": "active_only"}
    step_1_download_raw_gamma_main(run_mode=step_1_run_mode_mapping[run_mode])
    step_1b_download_raw_gamma_by_id_main(
        limit_to_fetch={"full": None, "lightweight": 500}[run_mode]
    )
    step_2a_bronze_gamma_markets_main()
    step_2b_bronze_gamma_events_main()
    step_3_silver_market_outcomes_main()

    logger.info("Data processing pipeline completed successfully.")


if __name__ == "__main__":
    tyro.cli(main)
