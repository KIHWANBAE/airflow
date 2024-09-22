from airflow import DAG
from airflow.sensors.filesystem import FileSensor
import pendulum

with DAG(
    dag_id = 'dags_file_sensor',
    start_date=pendulum.datetime(2024,9,23, tz='Asia/Seoul'),
    schedule='0 7 * * *',
    catchup=False
) as dag:
    
    tbRealEstateStatusNew_sensor = FileSensor(
        task_id = 'tbRealEstateStatusNew_sensor',
        fs_conn_id= 'conn_file_opt_airflow_files',
        filepath= 'RealEstateStatus/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/TbRealEstateStatus.csv',
        recursive=False,
        poke_interval=60,
        timeout=60*60*24,
        mode='reschedule'
    )


