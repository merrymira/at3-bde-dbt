import os
import logging
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from airflow import AirflowException
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

########################################################
#
#   DAG Settings
#
#########################################################

dag_default_args = {
    'owner': 'at3_dbt',
    'start_date': datetime.now() - timedelta(days=2),
    'email': [],  
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'wait_for_downstream': False,
}

dag = DAG(
    dag_id='dag_at3',
    default_args=dag_default_args,
    schedule_interval=None,  # Set the schedule_interval as needed
    catchup=True,
    max_active_runs=1,
    concurrency=5
)

#########################################################
#
#   Load Environment Variables
#
#########################################################

AIRFLOW_DATA = "/home/airflow/gcs/data/"
LISTINGS = AIRFLOW_DATA+'listings/'

#########################################################
#
#   Custom Logics for Operator
#
#########################################################

def import_load_dim_census_g01_func(**kwargs):

    # Set up pg connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    # Generate DataFrame by combining files
    df = pd.read_csv(AIRFLOW_DATA+'/2016Census_G01_NSW_LGA.csv')

    if not df.empty:
        col_names = df.columns.to_list()
        values = df[col_names].to_dict('split')
        values = values['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO raw.Census_G01({})
                    VALUES %s
                    """.format(','.join(col_names))

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()
    else:
        None

    return None  

def import_load_dim_census_g02_func(**kwargs):

    # Set up pg connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    # Generate DataFrame by combining files
    df = pd.read_csv(AIRFLOW_DATA+'2016Census_G02_NSW_LGA.csv')

    if not df.empty:
        col_names = df.columns.to_list()
        values = df[col_names].to_dict('split')
        values = values['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO raw.Census_G02({})
                    VALUES %s
                    """.format(','.join(col_names))

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()
    else:
        None

    return None  

def import_load_dim_nsw_lga_code_func(**kwargs):

    # Set up pg connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    # Generate DataFrame by combining files
    df = pd.read_csv(AIRFLOW_DATA+'NSW_LGA_CODE.csv')

    if not df.empty:
        col_names = df.columns.to_list()
        values = df[col_names].to_dict('split')
        values = values['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO raw.nsw_lga_code({})
                    VALUES %s
                    """.format(','.join(col_names))

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()
    else:
        None

    return None  

def import_load_dim_nsw_lga_suburb_func(**kwargs):

    # Set up pg connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    # Generate DataFrame by combining files
    df = pd.read_csv(AIRFLOW_DATA+'NSW_LGA_SUBURB.csv')

    if not df.empty:
        col_names = df.columns.to_list()
        values = df[col_names].to_dict('split')
        values = values['data']
       
        logging.info(values)

        insert_sql = """
                    INSERT INTO raw.nsw_lga_suburb({})
                    VALUES %s
                    """.format(','.join(col_names))


        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()
    else:
        None

    return None  

def import_load_listings_func(**kwargs):

    # Set up pg connection
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    #get all files with filename including the string '.csv'
    filelist = [k for k in os.listdir(LISTINGS) if '.csv' in k]

    # Generate DataFrame by combining files
    df = pd.concat([pd.read_csv(LISTINGS + f) for f in filelist], ignore_index=True)

    if not df.empty:
        col_names = df.columns.to_list()
        values = df[col_names].to_dict('split')
        values = values['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO raw.listings({})
                    VALUES %s
                    """.format(','.join(col_names))

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(df))
        conn_ps.commit()
    else:
        None

    return None 


#########################################################
#
#   DAG Operator Setup
#
#########################################################

import_load_dim_census_g01_task = PythonOperator(
    task_id='import_load_dim_census_g01',
    python_callable=import_load_dim_census_g01_func,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

import_load_dim_census_g02_task = PythonOperator(
    task_id='import_load_dim_census_g02',
    python_callable=import_load_dim_census_g02_func,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

import_load_dim_nsw_lga_code_task = PythonOperator(
    task_id='import_load_dim_nsw_lga_code',
    python_callable=import_load_dim_nsw_lga_code_func,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

import_load_dim_nsw_lga_suburb_task = PythonOperator(
    task_id='import_load_dim_nsw_lga_suburb_func',
    python_callable=import_load_dim_nsw_lga_suburb_func,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

import_load_listings_task = PythonOperator(
    task_id='import_load_listings',
    python_callable=import_load_listings_func,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

# Set up task dependencies if needed
[import_load_dim_census_g01_task, import_load_dim_census_g02_task, import_load_dim_nsw_lga_code_task, import_load_dim_nsw_lga_suburb_task, import_load_listings_task] 
