import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

conn_string = 'mysql+mysqlconnector://root:root@localhost:3306/mydb'
string_mysql = 'mysql+mysqlconnector://root:root@localhost:3306/mydb'
string_postgres = 'postgresql://airflow:airflow@localhost:5434/airflow'

def _insert_fact_monthly_covid():
    db_postgres = create_engine(string_postgres, pool_size=10, max_overflow=20)
    conn_postgres = db_postgres.connect()

    # Fact Monthly 
    resoverall = db_postgres.execute("""
        WITH CTE AS (
    	SELECT EXTRACT(YEAR FROM cast(tanggal as date)) AS "year", EXTRACT(month FROM cast(tanggal as date)) AS "month", 
    	kode_prov, case_id, SUM(total) AS "total" 
    	FROM fact_covid_province_daily
    	GROUP BY EXTRACT(YEAR FROM cast(tanggal as date)), EXTRACT(month FROM cast(tanggal as date)), kode_prov, case_id
        ) SELECT ROW_NUMBER() OVER (ORDER BY year ASC, month ASC) AS "id", year, month, kode_prov, case_id, total from CTE;
                                  """
    )
    df = pd.DataFrame(resoverall.fetchall())
    df.columns = resoverall.keys()
    df.to_sql('fact_covid_province_monthly', con=conn_postgres, if_exists='replace', schema='public', index=False)

    resoverall = db_postgres.execute("""
        WITH CTE AS (
        	SELECT EXTRACT(YEAR FROM cast(tanggal as date)) AS "year", EXTRACT(month FROM cast(tanggal as date)) AS "month", 
        	kode_kab, case_id, SUM(total) AS "total" 
        	FROM fact_covid_province_daily
        	GROUP BY EXTRACT(YEAR FROM cast(tanggal as date)), EXTRACT(month FROM cast(tanggal as date)), kode_kab, case_id
        ) SELECT ROW_NUMBER() OVER (ORDER BY year ASC, month ASC) AS "id", year, month, kode_kab, case_id, total from CTE;
                                  """
    )
    df = pd.DataFrame(resoverall.fetchall())
    df.columns = resoverall.keys()
    df.to_sql('fact_covid_district_monthly', con=conn_postgres, if_exists='replace', schema='public', index=False)

    conn_postgres.close()

DAG = DAG(
    dag_id='monthly_covid_dag',
    start_date=datetime(2024, 2, 3),
    schedule_interval='30 07 03 * *',
    catchup=False,
    #schedule='@monthly'
)

fact_monthly = PythonOperator(
    task_id='ingest_to_postgres',
    python_callable=_insert_fact_monthly_covid,
    dag=DAG
)

start = EmptyOperator(task_id='start', dag=DAG)
end = EmptyOperator(task_id='end', dag=DAG)

start >> fact_monthly >> end