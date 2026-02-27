"""Generate SQL schemas from Dataframely schemas."""

from pathlib import Path
from typing import Any

import sqlalchemy
import sqlalchemy.dialects.mysql.base
from loguru import logger

from polymarket_data_dolthub_jobs.tables import TABLES_TO_SCHEMAS

schema_output_folder = Path("./dolt_schemas")


def main() -> None:
    """Generate SQL schemas."""
    assert schema_output_folder.is_dir()

    for table_name, schema_class in TABLES_TO_SCHEMAS.items():
        logger.info(f"Generating SQL schema for {table_name}")
        sqlalchemy_columns: list[Any] = schema_class.to_sqlalchemy_columns(  # pyright: ignore[reportUnknownVariableType,reportUnknownMemberType]
            dialect=sqlalchemy.dialects.mysql.base.MySQLDialect()
        )

        # Create a MetaData instance.
        metadata = sqlalchemy.MetaData()

        # Add automatic added and updated columns.
        sqlalchemy_columns.extend(
            [
                sqlalchemy.Column(
                    "db_created_at",
                    sqlalchemy.DateTime,
                    nullable=False,
                    # Set when the row is first inserted:
                    server_default=sqlalchemy.func.now(),
                ),
                sqlalchemy.Column(
                    "db_updated_at",
                    sqlalchemy.DateTime,
                    nullable=False,
                    server_default=sqlalchemy.text(
                        "CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
                    ),
                ),
            ]
        )

        # Dynamically create a Table from the list of columns.
        my_table = sqlalchemy.Table(table_name, metadata, *sqlalchemy_columns)

        # Generate the CREATE TABLE statement.
        create_stmt = str(
            sqlalchemy.schema.CreateTable(my_table).compile(
                compile_kwargs={"literal_binds": True}
            )
        ).replace("\t", " " * 4)

        if "PRIMARY KEY" not in create_stmt:
            logger.warning(f"Table {table_name} has no PRIMARY KEY defined.")

        if "VARCHAR," in create_stmt:
            logger.warning(
                f"Table {table_name} has VARCHAR columns. Must add length limits."
            )

        # Hack: Replace the very-long "desciption" from VARCHAR to TEXT.
        create_stmt = create_stmt.replace(
            "description VARCHAR(50000)", "description TEXT"
        )

        (schema_output_folder / f"{table_name}.sql").write_text(create_stmt)

    logger.success("Done.")


if __name__ == "__main__":
    main()
