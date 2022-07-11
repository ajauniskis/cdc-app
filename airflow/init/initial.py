import os

import pandas as pd

from models.order import order
from utils.database import engine, metadata
from utils.logger import logger
from utils.processing import insert_record
from utils.utils import string_to_datetime


def main() -> None:
    file_path = "./data/order/initial_data.csv"

    metadata.create_all(engine)

    df = pd.read_csv(file_path)

    string_cols_map = {
        "order_created_at": "%Y-%m-%d %H:%M:%S",
        "order_updated_at": "%Y-%m-%d %H:%M:%S",
    }
    df = string_to_datetime(string_cols_map, df)

    records = 0
    for _, record in df.iterrows():
        insert_record(
            engine,
            order,
            record.to_dict(),
        )
        records += 1

    logger.info(f"Order table populated with {records} records")

    os.rename(
        file_path,
        f"{os.path.dirname(file_path)}/processed/{os.path.basename(file_path)}",
    )
    logger.info("Processing complete. File moved to processed dir")

    engine.dispose()


if __name__ == "__main__":
    main()
