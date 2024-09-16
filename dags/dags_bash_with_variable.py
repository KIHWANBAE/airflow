from airflow import DAG
import datetime
import pendulum

from airflow.operators.bash import BashOperator
from airflow.models import Variable

with DAG(
    dag_id="dags_bash_with_variable",
    schedule="10 9 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    bash_var_2 = BashOperator(
        task_id = "bash_var_2",
        bash_command="echo variable:{{var.value.sample_key}}"
    )