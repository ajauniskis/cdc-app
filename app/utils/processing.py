from typing import Any, Dict, List

from sqlalchemy import MetaData, Table
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
