from typing import Any, Dict, List

from models.order import order
from sqlalchemy import MetaData, Table, select, func
from sqlalchemy.engine import Engine

from utils.logger import logger


def compare_schema(
    db_table_name: str,
    cdc_columns: List,
    engine: Engine,
) -> List:

    db_table = Table(db_table_name, MetaData(bind=engine), autoload=True)
    db_cols = [c.name for c in db_table.columns]

    diff = list(set(cdc_columns) - set(db_cols))
    if "operation" in diff:
        diff.remove("operation")

    return diff


def insert_record(
    engine: Engine,
    table: Table,
    values: Dict[str, Any],
):
    query = table.insert().values(values)
    engine.execute(query)


def count_table_records(table: Table, engine: Engine) -> int:
    query = select([func.count()]).select_from(table)
    return engine.execute(query).one()[0]
