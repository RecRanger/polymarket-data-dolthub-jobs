"""Generate SQL schemas from Dataframely schemas."""

from pathlib import Path
from typing import Any

import polars as pl
import sqlalchemy as sa
from loguru import logger
from sqlalchemy.dialects.mysql.base import MySQLDialect

from polymarket_data_dolthub_jobs.tables import TABLES_TO_SCHEMAS

schema_output_folder = Path("./dolt_schemas")


def main() -> None:
    """Generate SQL schemas."""
    assert schema_output_folder.is_dir()

    for table_name, schema_class in TABLES_TO_SCHEMAS.items():
        logger.info(f"Generating SQL schema for {table_name}")
        sqlalchemy_columns: list[sa.Column[Any]] = schema_class.to_sqlalchemy_columns(  # pyright: ignore[reportUnknownVariableType,reportUnknownMemberType]
            dialect=MySQLDialect()
        )

        # Transform any enum columns.
        for i in range(len(sqlalchemy_columns)):
            column_name, polars_type = list(schema_class.to_polars_schema().items())[i]
            if isinstance(polars_type, pl.Enum):
                assert isinstance(sqlalchemy_columns[i].type, sa.String)
                logger.debug(f"Transforming enum column {column_name}")

                sqlalchemy_columns[i] = sa.Column(
                    sqlalchemy_columns[i].name,
                    sa.Enum(
                        *polars_type.categories.to_list(),
                        native_enum=True,
                        # Disable - create_constraint=True,
                    ),
                    primary_key=sqlalchemy_columns[i].primary_key,
                    nullable=sqlalchemy_columns[i].nullable,
                )

        # Create a MetaData instance.
        metadata = sa.MetaData()

        # Add automatic added and updated columns.
        sqlalchemy_columns.extend(
            [
                sa.Column(
                    "db_created_at",
                    sa.DateTime,
                    nullable=False,
                    # Set when the row is first inserted:
                    server_default=sa.func.now(),
                ),
                sa.Column(
                    "db_updated_at",
                    sa.DateTime,
                    nullable=False,
                    server_default=sa.text(
                        "CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
                    ),
                ),
            ]
        )

        # Dynamically create a Table from the list of columns.
        my_table = sa.Table(table_name, metadata, *sqlalchemy_columns)

        # Generate the CREATE TABLE statement.
        create_stmt = str(
            sa.schema.CreateTable(my_table).compile(
                compile_kwargs={"literal_binds": True},
                dialect=MySQLDialect(),
            )
        ).replace("\t", " " * 4)

        if "PRIMARY KEY" not in create_stmt:
            logger.warning(f"Table {table_name} has no PRIMARY KEY defined.")

        if "VARCHAR," in create_stmt:
            logger.warning(
                f"Table {table_name} has VARCHAR columns. Must add length limits."
            )

        # Hack: Replace the very-long "description" from VARCHAR to TEXT.
        create_stmt = create_stmt.replace(
            "description VARCHAR(50000)", "description TEXT"
        )

        (schema_output_folder / f"{table_name}.sql").write_text(create_stmt)

    logger.success("Done.")


if __name__ == "__main__":
    main()
