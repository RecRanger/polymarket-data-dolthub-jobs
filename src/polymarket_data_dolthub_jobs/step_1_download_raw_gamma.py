"""Load the full Gamma events/markets datasets from API."""

import itertools
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
    *,
    active: bool = True,
    closed: bool = False,
    max_rows: int | None = None,
) -> list[dict[str, Any]]:
    """Load the full events/markets list dataset from API."""
    logger.info(f"Fetching all pages of {endpoint} data from API.")

    offset: int = 0
    page_size: int = 100

    data: list[dict[str, Any]] = []

    while True:
        page_data: list[dict[str, Any]] = url_get_request(
            f"https://gamma-api.polymarket.com/{endpoint}?active={str(active).lower()}&closed={str(closed).lower()}&limit={page_size}&offset={offset}"
        )
        assert isinstance(page_data, list)
        assert all(isinstance(market, dict) for market in page_data)

        active_or_inactive = "active" if active else "inactive"
        closed_or_open = "closed" if closed else "open"
        (
            OUTPUT_FOLDER_RAW_PAGES
            / f"{endpoint}_{active_or_inactive}_{closed_or_open}_page_{offset // page_size}.json"  # noqa: E501
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

        if (max_rows is not None) and (len(data) >= max_rows):
            logger.info(f"Reached the max_rows limit of {max_rows:,}. Stopping.")
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
        rows: list[dict[str, Any]] = []
        for active, closed in itertools.product([True, False], [True, False]):
            logger.info(
                f"Fetching {endpoint} data with active={active} closed={closed}."
            )
            rows_with_state = fetch_all_pages_from_endpoint(
                endpoint, active=active, closed=closed, max_rows=50_000
            )
            rows.extend(rows_with_state)
            logger.info(
                f"Fetched {len(rows_with_state):,} rows of "
                f"active={active} closed={closed} {endpoint} data."
            )

        logger.info(f"Fetched a total of {len(rows):,} rows of {endpoint} data.")

        output_file_path: Path = {
            "markets": OUTPUT_MARKETS_JSON_FILE,
            "events": OUTPUT_EVENTS_JSON_FILE,
        }[endpoint]
        output_file_path.write_bytes(orjson.dumps(rows, option=orjson.OPT_INDENT_2))

        logger.info(f"Saved {len(rows):,} rows of {endpoint} data to json.")

    logger.success(f"Finished {Path(__file__).name}.")


if __name__ == "__main__":
    main()
