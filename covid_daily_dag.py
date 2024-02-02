import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import requests
import pandas as pd
from sqlalchemy import create_engine

conn_string = 'mysql+mysqlconnector://root:root@localhost:3306/mydb'
string_mysql = 'mysql+mysqlconnector://root:root@localhost:3306/mydb'
string_postgres = 'postgresql://airflow:airflow@localhost:5434/airflow'

def _stg_covid_api():
    db = create_engine(conn_string, pool_size=10, max_overflow=20)
    conn = db.connect()

    # Create dataframe
    url = 'http://103.150.197.96:5005/api/v1/rekapitulasi_v2/jabar/harian?level=kab'
    response = requests.get(url)
    json_data = json.loads(response.text)
    df = pd.DataFrame(json_data['data']['content'])

    # Insert into dataframe
    df.to_sql('stg_covid_daily', con=conn, if_exists='replace', index=False)
    print('Succesfully Insert To Staging')

    sql_query = '''
    INSERT INTO raw_covid
    SELECT *, CONCAT(tanggal , '_', kode_prov, '_', kode_kab) AS 'UNIQUE_RECID' 
    FROM stg_covid_daily scd WHERE NOT EXISTS (SELECT 1 FROM raw_covid rc WHERE 
    	CONCAT(scd.tanggal , '_', scd.kode_prov, '_', scd.kode_kab) = rc.UNIQUE_RECID
    );
    '''
    # Execute and close connection
    conn.execute(sql_query)
    conn.close()

def _insert_dim_covid():
    db_mysql = create_engine(string_mysql, pool_size=10, max_overflow=20)
    db_postgres = create_engine(string_postgres, pool_size=10, max_overflow=20)
    conn_mysql = db_mysql.connect()
    conn_postgres = db_postgres.connect()

    # Insert Into Dim Provinsi
    resoverall = db_mysql.execute("""
        SELECT DISTINCT kode_prov, nama_prov FROM raw_covid;
                                  """
    )
    df = pd.DataFrame(resoverall.fetchall())
    df.columns = resoverall.keys()
    df.to_sql('dim_province', con=conn_postgres, if_exists='replace', schema='public', index=False)

    # Insert Into Dim District
    resoverall = db_mysql.execute("""
        SELECT DISTINCT kode_prov, kode_kab, nama_kab FROM raw_covid;
                                  """
    )
    df = pd.DataFrame(resoverall.fetchall())
    df.columns = resoverall.keys()
    df.to_sql('dim_district', con=conn_postgres, if_exists='replace', schema='public', index=False)

    # Insert Into DimCase
    resoverall = db_mysql.execute("""
        SELECT 1 AS 'case_id', 'closecontact' AS 'status_name', 'closecontact_dikarantina' AS 'status_detail'
        UNION
        SELECT 2 AS 'case_id', 'closecontact' AS 'status_name', 'closecontact_discarded' AS 'status_detail'
        UNION
        SELECT 3 AS 'case_id', 'closecontact' AS 'status_name', 'closecontact_meninggal' AS 'status_detail'
        UNION
        SELECT 4 AS 'case_id', 'confirmation' AS 'status_name', 'confirmation_meninggal' AS 'status_detail'
        UNION
        SELECT 5 AS 'case_id', 'confirmation' AS 'status_name', 'confirmation_sembuh' AS 'status_detail'
        UNION
        SELECT 6 AS 'case_id', 'probable' AS 'status_name', 'probable_diisolasi' AS 'status_detail'
        UNION
        SELECT 7 AS 'case_id', 'probable' AS 'status_name', 'probable_discarded' AS 'status_detail'
        UNION
        SELECT 8 AS 'case_id', 'probable' AS 'status_name', 'probable_meninggal' AS 'status_detail'
        UNION
        SELECT 9 AS 'case_id', 'suspect' AS 'status_name', 'suspect_diisolasi' AS 'status_detail'
        UNION
        SELECT 10 AS 'case_id', 'suspect' AS 'status_name', 'suspect_discarded' AS 'status_detail'
        UNION
        SELECT 11 AS 'case_id', 'suspect' AS 'status_name', 'suspect_meninggal' AS 'status_detail'
                                  """
    )
    df = pd.DataFrame(resoverall.fetchall())
    df.columns = resoverall.keys()
    df.to_sql('dim_case', con=conn_postgres, if_exists='replace', schema='public', index=False)

    conn_mysql.close()
    conn_postgres.close()

def _insert_fact_daily_covid():
    db_mysql = create_engine(string_mysql, pool_size=10, max_overflow=20)
    db_postgres = create_engine(string_postgres, pool_size=10, max_overflow=20)
    conn_mysql = db_mysql.connect()
    conn_postgres = db_postgres.connect()

    # Fact Daily
    resoverall = db_mysql.execute("""
        WITH CTE AS (
            SELECT kode_prov, kode_kab, tanggal, 'closecontact_dikarantina' AS 'status_detail', closecontact_dikarantina AS total FROM raw_covid rc
            UNION
            SELECT kode_prov, kode_kab, tanggal, 'closecontact_discarded' AS 'status_detail', closecontact_discarded AS total FROM raw_covid rc
            UNION
            SELECT kode_prov, kode_kab, tanggal, 'closecontact_meninggal' AS 'status_detail', closecontact_meninggal AS total FROM raw_covid rc
            UNION
            SELECT kode_prov, kode_kab, tanggal, 'confirmation_meninggal' AS 'status_detail', confirmation_meninggal AS total FROM raw_covid rc
            UNION
            SELECT kode_prov, kode_kab, tanggal, 'confirmation_sembuh' AS 'status_detail', confirmation_sembuh AS total FROM raw_covid rc
            UNION
            SELECT kode_prov, kode_kab, tanggal, 'probable_diisolasi' AS 'status_detail', probable_diisolasi AS total FROM raw_covid rc
            UNION
            SELECT kode_prov, kode_kab, tanggal, 'probable_discarded' AS 'status_detail', probable_discarded AS total FROM raw_covid rc
            UNION
            SELECT kode_prov, kode_kab, tanggal, 'probable_meninggal' AS 'status_detail', probable_meninggal AS total FROM raw_covid rc
            UNION
            SELECT kode_prov, kode_kab, tanggal, 'suspect_diisolasi' AS 'status_detail', suspect_diisolasi AS total FROM raw_covid rc
            UNION
            SELECT kode_prov, kode_kab, tanggal, 'suspect_discarded' AS 'status_detail', suspect_discarded AS total FROM raw_covid rc
            UNION
            SELECT kode_prov, kode_kab, tanggal, 'suspect_meninggal' AS 'status_detail', suspect_meninggal AS total FROM raw_covid rc
        ) SELECT ROW_NUMBER() OVER (ORDER BY tanggal ASC) AS 'id', kode_prov, kode_kab, tanggal, status_detail, total FROM CTE ;
        """
    )
    df = pd.DataFrame(resoverall.fetchall())
    df.columns = resoverall.keys()
    df.to_sql('fact_covid_province_daily', con=conn_postgres, if_exists='replace', schema='public', index=False)   

    resoverall = db_postgres.execute("""
        SELECT a.id, a.kode_prov, a.kode_kab, a.tanggal, b.case_id, a.total 
        FROM fact_covid_province_daily a
        LEFT JOIN dim_case b ON a.status_detail = b.status_detail;
        """
    )
    df = pd.DataFrame(resoverall.fetchall())
    df.columns = resoverall.keys()
    df.to_sql('fact_covid_province_daily', con=conn_postgres, if_exists='replace', schema='public', index=False)   

    conn_mysql.close()
    conn_postgres.close()

def _insert_fact_yearly_covid():
    db_postgres = create_engine(string_postgres, pool_size=10, max_overflow=20)
    conn_postgres = db_postgres.connect()

    # Fact Yearly 
    resoverall = db_postgres.execute("""
        WITH CTE AS (
    	    SELECT kode_prov, case_id, EXTRACT(YEAR FROM cast(tanggal as date)) AS "year", SUM(total) AS "total"
    	    FROM fact_covid_province_daily
    	    GROUP BY EXTRACT(YEAR FROM cast(tanggal as date)), kode_prov, case_id
        ) SELECT ROW_NUMBER() OVER (ORDER BY year ASC) AS "id", year, kode_prov, case_id, total from CTE;
                                  """
    )
    df = pd.DataFrame(resoverall.fetchall())
    df.columns = resoverall.keys()
    df.to_sql('fact_covid_province_yearly', con=conn_postgres, if_exists='replace', schema='public', index=False)

    resoverall = db_postgres.execute("""
        WITH CTE AS (
    	    SELECT kode_kab, case_id, EXTRACT(YEAR FROM cast(tanggal as date)) AS "year", SUM(total) AS "total"
    	    FROM fact_covid_province_daily
    	    GROUP BY EXTRACT(YEAR FROM cast(tanggal as date)), kode_kab, case_id
        ) SELECT ROW_NUMBER() OVER (ORDER BY year ASC) AS "id", year, kode_kab, case_id, total from CTE;
                                  """
    )
    df = pd.DataFrame(resoverall.fetchall())
    df.columns = resoverall.keys()
    df.to_sql('fact_covid_district_yearly', con=conn_postgres, if_exists='replace', schema='public', index=False)

    conn_postgres.close()

DAG = DAG(
    dag_id='daily_covid_dag',
    start_date=datetime(2024, 2, 3),
    schedule_interval='00 07 * * *',
    catchup=False,
    #start_date=datetime.datetime(2021, 10, 1),
    #schedule='@daily'
) 

staging = PythonOperator(
    task_id='check_file_exists',
    python_callable=_stg_covid_api,
    dag=DAG
)

dim = PythonOperator(
    task_id='get_data',
    python_callable=_insert_dim_covid,
    dag=DAG
)

fact_daily = PythonOperator(
    task_id='transform_data',
    python_callable=_insert_fact_daily_covid,
    dag=DAG
)

fact_yearly = PythonOperator(
    task_id='ingest_to_postgres',
    python_callable=_insert_fact_yearly_covid,
    dag=DAG
)

start = EmptyOperator(task_id='start', dag=DAG)
end = EmptyOperator(task_id='end', dag=DAG)

start >> staging >> dim >> fact_daily >> fact_yearly >> end