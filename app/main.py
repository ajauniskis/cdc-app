import pandas as pd

from models.order import order
from utils.database import engine
from utils.logger import logger
from utils.processing import add_columns, compare_schema, insert_record, update_record


def main(file_path: str, table_name: str) -> None:
    df = pd.read_csv(
        "/home/ajauniskis/GitHub/cdc-app/app/data/Homework task @ CDC 2021-06-19.csv"
    )

    diff = compare_schema(table_name, list(df.columns), engine)

    if len(diff) > 0:
        columns = []
        for c in diff:
            columns.append(
                {
                    "name": c,
                    "type": str(df.dtypes["loyalty_applied"]),
                }
            )
        add_columns(table_name, columns, engine)

    processed_records = {
        "INSERT": 0,
        "UPDATE": 0,
        "DELETE": 0,
    }

    for _, record in df.iterrows():
        if record.operation == "INSERT":
            record.drop(labels=["operation"], inplace=True)
            record = record.to_dict()
            record["is_order_deleted"] = False
            insert_record(
                engine,
                table_name,
                record,
            )
            processed_records["INSERT"] += 1

        elif record.operation == "UPDATE":
            record.drop(labels=["operation"], inplace=True)
            record = record.to_dict()
            record["is_order_deleted"] = False
            update_record(
                engine,
                table_name,
                record,
            )
            processed_records["UPDATE"] += 1

        elif record.operation == "DELETE":
            record.drop(labels=["operation"], inplace=True)
            record = record.to_dict()
            record["is_order_deleted"] = True
            update_record(
                engine,
                table_name,
                record,
            )
            processed_records["DELETE"] += 1

    logger.info(f"Finished processing daily files: {file_path}, {processed_records}")


if __name__ == "__main__":
    main("./data/Homework task @ CDC 2021-06-17.csv", "order")
