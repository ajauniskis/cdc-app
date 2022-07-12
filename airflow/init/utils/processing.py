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


def add_columns(table_name: str, columns: List[dict], engine: Engine) -> None:
    type_map = {
        "object": "varchar",
        "int64": "int4",
        "float64": "float8",
        "bool": "bool",
    }
    sql = f"""
    ALTER TABLE public."{table_name}"
    """

    for c in columns:
        sql = f"""
        {sql}
        ADD COLUMN {c['name']} {type_map[c['type']]}
        """

    logger.info(f"Adding columns: {[c['name'] for c in columns]} to table {table_name}")
    engine.execute(sql)


def insert_record(
    engine: Engine,
    table: Table,
    values: Dict[str, Any],
):
    # table = Table(table_name, MetaData(bind=engine), autoload=True)
    query = table.insert().values(values)
    engine.execute(query)


def update_record(engine: Engine, table_name: str, values: Dict[str, str]) -> None:
    table = Table(table_name, MetaData(bind=engine), autoload=True)
    query = (
        table.update()
        .where(table.c[f"{table_name}_id"] == values[f"{table_name}_id"])
        .values(values)
    )
    engine.execute(query)


def count_table_records(table: Table, engine: Engine) -> int:
    query = select([func.count()]).select_from(table)
    return engine.execute(query).one()[0]
