from airflow.hooks.base import BaseHook
import psycopg2
import pandas as pd

class CustomPostgresHook(BaseHook):

    def __init__(self, postgres_conn_id, **kwargs):
        self.postgres_conn_id = postgres_conn_id

    def get_conn(self):
        airflow_conn = BaseHook.get_connection(self.postgres_conn_id)
        self.host = airflow_conn.host
        self.user = airflow_conn.login
        self.password = airflow_conn.password
        self.dbname = airflow_conn.schema
        self.port = airflow_conn.port

        self.postgres_conn = psycopg2.connect(host=self.host, user=self.user, password=self.password,dbname=self.dbname, port=self.port)
        return self.postgres_conn

    def bulk_load(self, table_name, file_name, delimiter: str, is_header: bool, is_replace: bool):
        from sqlalchemy import create_engine

        self.log.info('적재 대상파일:' + file_name)
        self.log.info('테이블 :' + table_name)
        self.get_conn()
        header = 0 if is_header else None                       
        if_exists = 'replace' if is_replace else 'append'       

        # CSV 파일을 읽어옴
        try:
            file_df = pd.read_csv(file_name, header=header, delimiter=delimiter)
        except Exception as e:
            self.log.error(f"파일 로드 중 오류 발생: {e}")
            return

        # 데이터베이스의 테이블 컬럼 정보 가져오기
        uri = f'postgresql://{self.user}:{self.password}@{self.host}/{self.dbname}'
        engine = create_engine(uri)
        
        # 테이블의 컬럼 목록 가져오기
        table_columns = engine.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name='{table_name}'").fetchall()
        table_columns = [col[0] for col in table_columns]
        
        # 각 행의 컬럼 개수 확인 후, 예상된 컬럼 수와 맞지 않는 행은 제거
        correct_column_count = len(table_columns)
        file_df = file_df[file_df.apply(lambda row: len(row) == correct_column_count, axis=1)]

        self.log.info(f'유효한 행 개수: {len(file_df)}')

        # 불필요한 개행문자 제거
        for col in file_df.columns:
            try:
                file_df[col] = file_df[col].astype(str).str.replace('\r\n','')  # 개행문자 제거
                self.log.info(f'{table_name}.{col}: 개행문자 제거')
            except Exception as e:
                self.log.warning(f'{table_name}.{col}: 처리 중 오류 발생 - {e}')
                continue 
                    
        # 테이블에 적재
        try:
            file_df.to_sql(name=table_name,
                            con=engine,
                            schema='public',
                            if_exists=if_exists,
                            index=False
                        )
            self.log.info(f'{table_name} 테이블에 데이터 적재 완료')
        except Exception as e:
            self.log.error(f'{table_name} 테이블 적재 중 오류 발생: {e}')
