from os import listdir
from os.path import abspath, basename, dirname

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from operators.process_cdc import ProcessCdcOperator

from models.airflow import default_args, environment_resolver

DATA_DIR_ROOT = f"{dirname(abspath(__file__))}/data"  # DATA DIR PATH

with DAG(
    basename(__file__).replace(".py", ""),
    description="Main pipeline for datalake.",
    schedule_interval=environment_resolver(
        "0 1 * * 1-5",
        None,
    ),
    default_args=default_args,
) as dag:

    with TaskGroup("staging_tables", tooltip="Staging Tables") as staging_tables:
        for dir in listdir(DATA_DIR_ROOT):

            with TaskGroup(dir, tooltip=dir) as locals()[dir]:

                process_cdc = ProcessCdcOperator(
                    task_id=f"process_{dir}",
                    conn_id="postgres",
                    files_dir=f"{DATA_DIR_ROOT}/{dir}",
                )

    further = DummyOperator(task_id="further_processing")

    staging_tables >> further  # pyright: ignore [reportUnusedExpression]
