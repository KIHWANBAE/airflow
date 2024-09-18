from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
import pendulum
import datetime

from operators.real_estate_to_csv import RealEstateToCSVOperator

with DAG(
    dag_id='dags_real_estate_api',
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2024, 9, 18, tz='Asia/Seoul'),
    catchup=False                                
) as dag:
    '''서울시 실거래가 정보'''
    tb_real_estate_status = RealEstateToCSVOperator(
        task_id='tb_real_estate_status',
        dataset_nm='TbRealEstateStatus',
        path='/opt/airflow/files/RealEstateStatus/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}',
        file_name='TbRealEstateStatus.csv'
    )
    
    '''서울시 전월세가 정보'''
    tb_real_estate_status_m = RealEstateToCSVOperator(
        task_id='tb_real_estate_status_m',
        dataset_nm='TbRealEstateStatus_m',
        path='/opt/airflow/files/RealEstateStatus_m/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}',
        file_name='TbRealEstateStatus_m.csv'
    )

    tb_real_estate_status >> tb_real_estate_status_m

