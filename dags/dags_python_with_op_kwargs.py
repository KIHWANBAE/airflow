from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
from common.common_func import regist

with DAG(
    dag_id="dags_python_with_op_kwargs",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2024, 9, 14, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    regist2_t1 = PythonOperator(
        task_id='regist2_t1',
        python_callable=regist,
        op_args=['kihwanb', 'man', 'kr', 'seoul'],
        op_kwargs={'email':'footix01@naver.com', 'phone':'010'}
    )

    regist2_t1