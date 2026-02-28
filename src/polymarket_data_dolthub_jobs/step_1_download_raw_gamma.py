"""Load the full Gamma events/markets datasets from API."""

from pathlib import Path
from typing import Any, Literal

import orjson
from loguru import logger

from polymarket_data_dolthub_jobs.request_helpers import url_get_request

OUTPUT_FOLDER = Path("./out/") / Path(__file__).stem
OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)

OUTPUT_FOLDER_RAW_PAGES = OUTPUT_FOLDER / "raw_pages"
OUTPUT_FOLDER_RAW_PAGES.mkdir(parents=True, exist_ok=True)

OUTPUT_MARKETS_JSON_FILE = OUTPUT_FOLDER / "markets_full.json"
OUTPUT_EVENTS_JSON_FILE = OUTPUT_FOLDER / "events_full.json"

GammaEndpointNameLiteral = Literal["events", "markets"]


def fetch_all_pages_from_endpoint(
    endpoint: GammaEndpointNameLiteral,
) -> list[dict[str, Any]]:
    """Load the full events/markets list dataset from API."""
    logger.info(f"Fetching all pages of {endpoint} data from API.")

    offset: int = 0
    page_size: int = 100

    data: list[dict[str, Any]] = []

    while True:
        page_data: list[dict[str, Any]] = url_get_request(
            f"https://gamma-api.polymarket.com/{endpoint}?active=true&closed=false&limit={page_size}&offset={offset}"
        )
        assert isinstance(page_data, list)
        assert all(isinstance(market, dict) for market in page_data)

        (
            OUTPUT_FOLDER_RAW_PAGES / f"{endpoint}_page_{offset // page_size}.json"
        ).write_bytes(orjson.dumps(page_data, option=orjson.OPT_INDENT_2))

        data.extend(page_data)
        offset += page_size

        if offset % 1000 == 0:
            logger.debug(
                f"Fetched page {offset / page_size:.0f} with "
                f"{offset=}, new_rows={len(page_data)}."
            )

        if len(page_data) < page_size:
            logger.info(f"Reached the end of the {endpoint} list.")
            break

    return data


def main() -> None:
    """Load the full events/markets list dataset from API (store locally)."""
    logger.info(f"Starting {Path(__file__).name}")

    endpoints_to_fetch: list[GammaEndpointNameLiteral] = [
        "events",
        # "markets",  # We don't currently use this dataset.
    ]

    for endpoint in endpoints_to_fetch:
        rows = fetch_all_pages_from_endpoint(endpoint)
        logger.info(f"Fetched {len(rows):,} rows of {endpoint} data.")

        output_file_path: Path = {
            "markets": OUTPUT_MARKETS_JSON_FILE,
            "events": OUTPUT_EVENTS_JSON_FILE,
        }[endpoint]
        output_file_path.write_bytes(orjson.dumps(rows, option=orjson.OPT_INDENT_2))

        logger.info(f"Saved {len(rows):,} rows of {endpoint} data to json.")

    logger.success(f"Finished {Path(__file__).name}.")


if __name__ == "__main__":
    main()
