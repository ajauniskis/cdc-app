from airflow.plugins_manager import AirflowPlugin

from operators.process_cdc import ProcessCdcOperator


class ProcessCdcOperatorPlugin(AirflowPlugin):
    name = "process_cdc"
    operators = [ProcessCdcOperator]
