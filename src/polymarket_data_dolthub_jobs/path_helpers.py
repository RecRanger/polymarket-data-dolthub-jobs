"""Universal definition of all paths used in the project."""

from pathlib import Path

import polars as pl

OUTPUT_FOLDER = Path("./out/")
OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)

# This path is synced with the GitHub Action.
FINAL_DOLT_TABLES_FOLDER = OUTPUT_FOLDER / "dolt_tables"
FINAL_DOLT_TABLES_FOLDER.mkdir(parents=True, exist_ok=True)


def write_final_dolt_table(df: pl.DataFrame, table_name: str) -> None:
    """Write the final table CSV to the Dolt tables folder."""
    df.write_csv(FINAL_DOLT_TABLES_FOLDER / f"{table_name}.csv")
    df.write_parquet(FINAL_DOLT_TABLES_FOLDER / f"{table_name}.parquet")
