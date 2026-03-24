"""One `id` at a time, load the Gamma info from the API.

The goal is to ensure the database correctly marks any events/markets that are
not active and/or closed as such.
"""

import subprocess
import tempfile
from pathlib import Path
from typing import Any, Literal

import orjson
import polars as pl
import tyro
from loguru import logger
from tqdm import tqdm

from polymarket_data_dolthub_jobs.path_helpers import FINAL_DOLT_TABLES_FOLDER
from polymarket_data_dolthub_jobs.request_helpers import url_get_request
from polymarket_data_dolthub_jobs.step_1_download_raw_gamma import (
    OUTPUT_EVENTS_JSON_FILE,
)

OUTPUT_FOLDER = Path("./out/") / Path(__file__).stem
OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)

OUTPUT_FOLDER_RAW_PAGES = OUTPUT_FOLDER / "raw_pages"
OUTPUT_FOLDER_RAW_PAGES.mkdir(parents=True, exist_ok=True)

OUTPUT_MARKETS_JSON_FILE_BY_ID = OUTPUT_FOLDER / "markets_full.json"
OUTPUT_EVENTS_JSON_FILE_BY_ID = OUTPUT_FOLDER / "events_full.json"

OUTPUT_ARCHIVED_EVENTS_SQL = OUTPUT_FOLDER / "set_archived_events.sql"

GammaEndpointNameLiteral = Literal["events", "markets"]


def _fetch_rows_to_requery() -> pl.DataFrame:
    with tempfile.TemporaryDirectory() as temp_dir:
        subprocess.check_output(
            ["dolt", "clone", "recranger/polymarket-data", "--depth", "1"],
            cwd=temp_dir,
        )
        logger.debug("Cloned polymarket-data.")

        subprocess.check_output(
            [
                "dolt",
                "table",
                "export",
                "bronze_gamma_events",
                parquet_path := Path(temp_dir) / "bronze_gamma_events.parquet",
            ],
            cwd=Path(temp_dir) / "polymarket-data",
        )
        logger.debug("Exported bronze_gamma_events table to parquet.")

        df = pl.read_parquet(parquet_path)

        logger.debug(f"Loaded bronze_gamma_events.parquet: {df.shape}")

    df = df.filter(
        # TODO: Potentially change AND to OR to ensure we get the latest updates.
        (pl.col("active").cast(pl.Boolean) == pl.lit(True, pl.Boolean))
        & (pl.col("closed").cast(pl.Boolean) == pl.lit(False, pl.Boolean))
        & (pl.col("archived").cast(pl.Boolean) == pl.lit(False, pl.Boolean))
    ).select(
        pl.col("event_id").cast(pl.UInt32),
    )

    return df  # noqa: RET504


def _fetch_endpoint_by_id(
    id_filter: list[int], endpoint: Literal["events", "markets"] = "events"
) -> list[dict[str, Any]]:
    id_filter_str = "&".join(f"id={i}" for i in sorted(id_filter))
    # API Docs: https://docs.polymarket.com/api-reference/events/list-events
    page_data: list[dict[str, Any]] = url_get_request(
        f"https://gamma-api.polymarket.com/{endpoint}?{id_filter_str}",
    )
    assert isinstance(page_data, list)
    assert all(isinstance(row, dict) for row in page_data)

    if len(page_data) != len(id_filter):
        disappeared = set(id_filter) - {int(row["id"]) for row in page_data}
        _ = (  # Previously logged this, but not too useful.
            f"Expected {len(id_filter)} rows, got {len(page_data)}. "
            f"Disappeared event_id values: {sorted(disappeared)}"
        )
        assert len(disappeared) == (len(id_filter) - len(page_data))

    (
        OUTPUT_FOLDER_RAW_PAGES
        / f"{endpoint}_{','.join(map(str, sorted(id_filter)))}.json"
    ).write_bytes(orjson.dumps(page_data, option=orjson.OPT_INDENT_2))

    return page_data


def main(limit_to_fetch: int | None = None) -> None:
    """Load the full events/markets list dataset from API (store locally).

    Args:
        limit_to_fetch: Maximum number of events/markets to fetch.

    """
    logger.info(f"Starting {Path(__file__).name}")
    endpoint = "events"

    df_to_fetch = _fetch_rows_to_requery()
    logger.info(
        f"Found {df_to_fetch.height:,} events which are marked active in the database."
    )

    if OUTPUT_EVENTS_JSON_FILE.exists():
        logger.debug("Loading already-fetched events from Step 1a.")
        df_already_fetched = pl.read_json(
            OUTPUT_EVENTS_JSON_FILE,
            schema={"id": pl.String},
        )
        df_already_fetched = df_already_fetched.cast({"id": pl.UInt32})
        logger.info(f"From Step 1a, already done {df_already_fetched.height:,} events.")

        df_to_fetch = df_to_fetch.filter(
            df_to_fetch["event_id"].is_in(df_already_fetched["id"].implode()).not_()
        )
    else:
        logger.warning("Step 1a not run. Will fetch all events, even if already done.")

    if (limit_to_fetch is not None) and (limit_to_fetch <= df_to_fetch.height):
        logger.info(
            f"Limiting fetch to {limit_to_fetch:,} events "
            f"from {df_to_fetch.height:,} total."
        )
        df_to_fetch = df_to_fetch.sample(limit_to_fetch)

    logger.info(f"Will fetch {df_to_fetch.height:,} events.")

    number_of_rows_per_fetch = 20  # Appears that 20 is the maximum.

    rows: list[dict[str, Any]] = []
    for df_slice in tqdm(
        df_to_fetch.iter_slices(number_of_rows_per_fetch),
        total=(df_to_fetch.height // number_of_rows_per_fetch) + 1,
    ):
        id_value: list[int] = df_slice["event_id"].to_list()
        rows.extend(_fetch_endpoint_by_id(id_value, endpoint=endpoint))

        if len(rows) % 1000 == 0:
            logger.debug(f"Fetched {len(rows):,} rows of events data.")

    logger.info(
        f"Requested {df_to_fetch.height:,}, "
        f"yielding {len(rows):,} rows of {endpoint} data."
    )

    # Figure out which events "disappeared".
    disappeared_list = sorted(
        set(df_to_fetch["event_id"].to_list()) - {int(row["id"]) for row in rows}
    )
    assert len(disappeared_list) == (df_to_fetch.height - len(rows))
    logger.info(f"Setting {len(disappeared_list):,} disappeared events as archived!")
    # Save the disappeared list.
    with OUTPUT_ARCHIVED_EVENTS_SQL.open("w") as fp:
        for disappeared_id in disappeared_list:
            fp.write(
                f"UPDATE bronze_gamma_events SET archived=1 "
                f"WHERE event_id = {disappeared_id};\n"
            )
    (FINAL_DOLT_TABLES_FOLDER / "set_archived_events.sql").write_text(
        OUTPUT_ARCHIVED_EVENTS_SQL.read_text()
    )

    # Save the main output of this step.
    output_file_path: Path = {
        "markets": OUTPUT_MARKETS_JSON_FILE_BY_ID,
        "events": OUTPUT_EVENTS_JSON_FILE_BY_ID,
    }[endpoint]
    output_file_path.write_bytes(orjson.dumps(rows, option=orjson.OPT_INDENT_2))

    logger.info(f"Saved {len(rows):,} rows of {endpoint} data to json.")

    logger.success(f"Finished {Path(__file__).name}.")


if __name__ == "__main__":
    tyro.cli(main)
