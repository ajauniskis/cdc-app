from os import listdir, rename
from os.path import basename, dirname, isfile, join
from typing import Any, Dict, List

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.baseoperator import BaseOperator
from sqlalchemy import MetaData, Table
from sqlalchemy.engine import Engine


class ProcessCdcOperator(BaseOperator):

    ui_color = "#FF9900"

    def __init__(
        self,
        *,
        conn_id: str,
        files_dir: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.files_dir = files_dir
        self.engine = PostgresHook(conn_id).get_sqlalchemy_engine()
        self.metadata = self._update_metadata()

    def _update_metadata(self) -> MetaData:
        return MetaData(bind=self.engine, schema="staging")

    def compare_schema(
        self,
        db_table_name: str,
        cdc_columns: List,
    ) -> List:

        db_table = Table(db_table_name, self.metadata, autoload=True)
        db_cols = [c.name for c in db_table.columns]

        diff = list(set(cdc_columns) - set(db_cols))
        if "operation" in diff:
            diff.remove("operation")

        return diff

    def add_columns(
        self,
        table_name: str,
        columns: List[dict],
    ) -> None:
        type_map = {
            "object": "varchar",
            "int64": "int4",
            "float64": "float8",
            "bool": "bool",
        }

        sql = f"""
        ALTER TABLE staging."{table_name}"
        """

        for c in columns:
            default = "NOT NULL DEFAULT false" if type_map[c["type"]] == "bool" else ""
            sql = f"""
            {sql}
            ADD COLUMN {c['name']} {type_map[c['type']]} {default}
            """

            print(f"Adding column: {c['name']} to table: {table_name}")
            self.engine.execute(sql)
            self.metadata = self._update_metadata()

            if type_map[c["type"]] == "bool":
                print(f"Updating all values for column: {c['name']} to False")
                self._update_all_records(table_name, {c["name"]: False})

    def insert_record(
        self,
        table_name: str,
        values: Dict[str, Any],
    ) -> None:
        table = Table(table_name, self.metadata, autoload=True)
        query = table.insert().values(values)
        self.engine.execute(query)

    def update_record(
        self,
        table_name: str,
        values: Dict[str, Any],
    ) -> None:
        table = Table(table_name, self.metadata, autoload=True)
        query = (
            table.update()
            .where(table.c[f"{table_name}_id"] == values[f"{table_name}_id"])
            .values(values)
        )
        self.engine.execute(query)

    def _update_all_records(
        self,
        table_name: str,
        values: Dict[str, Any],
    ) -> None:
        table = Table(table_name, self.metadata, autoload=True)
        query = table.update().values(values)
        self.engine.execute(query)

    def execute(self, context: dict) -> None:
        table_name = basename(self.files_dir)

        files = [f for f in listdir(self.files_dir) if isfile(join(self.files_dir, f))]
        files.sort()
        print(f"Found files to process: {files}")
        for file in files:
            file_path = f"{self.files_dir}/{file}"
            print(f"Processing file: {file}")
            df = pd.read_csv(f"{self.files_dir}/{file}")

            diff = self.compare_schema(
                db_table_name=table_name,
                cdc_columns=list(df.columns),
            )

            if len(diff) > 0:
                columns = []
                for c in diff:
                    columns.append(
                        {
                            "name": c,
                            "type": str(df.dtypes[c]),
                        }
                    )

                self.add_columns(table_name, columns)

            processed_records = {
                "INSERT": 0,
                "UPDATE": 0,
                "DELETE": 0,
            }

            df.drop_duplicates(keep="last", inplace=True)

            for _, record in df.iterrows():
                if record.operation == "INSERT":
                    record.drop(labels=["operation"], inplace=True)
                    record = record.to_dict()
                    record["is_order_deleted"] = False
                    self.insert_record(
                        table_name,
                        record,
                    )
                    processed_records["INSERT"] += 1

                elif record.operation == "UPDATE":
                    record.drop(labels=["operation"], inplace=True)
                    record = record.to_dict()
                    record["is_order_deleted"] = False
                    self.update_record(
                        table_name,
                        record,
                    )
                    processed_records["UPDATE"] += 1

                elif record.operation == "DELETE":
                    record.drop(labels=["operation"], inplace=True)
                    record = record.to_dict()
                    record["is_order_deleted"] = True
                    self.update_record(
                        table_name,
                        record,
                    )
                    processed_records["DELETE"] += 1

            print(f"Finished processing daily file: {file}, {processed_records}")
            # MOVE PROCESSED FILES
            # rename(
            #     file_path,
            #     f"{dirname(file_path)}/processed/{basename(file_path)}",
            # )
            # print("File moved to prcessed")
