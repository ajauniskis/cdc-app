import os

import pandas as pd

from utils.database import engine
from utils.logger import logger
from utils.utils import string_to_datetime
from models.order import order
from utils.database import metadata


def main():
    file_path = "./data/Homework task @ Initial Data.csv"

    metadata.create_all(engine)

    df = pd.read_csv(file_path)

    string_cols_map = {
        "order_created_at": "%Y-%m-%d %H:%M:%S",
        "order_updated_at": "%Y-%m-%d %H:%M:%S",
    }
    df = string_to_datetime(string_cols_map, df)

    records = 0
    for _, record in df.iterrows():
        query = order.insert().values(
            order_id=record.order_id,
            tax_amount=record.tax_amount,
            tax_excluded_amount=record.tax_excluded_amount,
            discount_amount=record.discount_amount,
            service_fee_amount=record.service_fee_amount,
            sales_amount=record.sales_amount,
            is_order_paid=record.is_order_paid,
            order_created_at=record.order_created_at,
            order_updated_at=record.order_updated_at,
            order_source=record.order_source,
            order_option=record.order_option,
            order_status=record.order_status,
            is_valid_record=record.is_valid_record,
            employee_key=record.employee_key,
            first_name=record.first_name,
            last_name=record.last_name,
            is_order_deleted=record.is_order_deleted,
        )
        engine.execute(query)
        records += 1

    logger.info(f"Order table populated with {records} records")

    os.rename(
        file_path,
        f"{os.path.dirname(file_path)}/processed/{os.path.basename(file_path)}",
    )
    logger.info("Processing complete. File moved to processed dir")


if __name__ == "__main__":
    main()
