"""
Updates each Tableau datasource's columns/connection/etc, according to the config files.
"""

import datetime
import logging
import os
import shutil
import ast
from tableau_utilities import Datasource, TableauServer
from tableau_utilities.tableau_file.tableau_file_objects import Folder, Column, MetadataRecord

import dags.tableau_datasource_update.configs.configuration as cfg
from airflow import DAG, models
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
# This is our custom SnowflakeHook - Your code will need to be adapted
from plugins.snowflake_connection.snowflake_operator_manual_update import SnowflakeHook

AIRFLOW_ENV = models.Variable.get('AIRFLOW_ENVIRONMENT', '').upper()
EXCLUDED_DATASOURCES = ast.literal_eval(models.Variable.get('EXCLUDED_DATASOURCES', '[]'))
SKIP_REFRESH = ast.literal_eval(models.Variable.get('NO_REFRESH_DATASOURCES', '[]'))
UPDATE_ACTIONS = [
    'delete_metadata',
    'modify_metadata',
    'add_metadata',
    'add_column',
    'modify_column',
    'add_folder',
    'delete_folder',
    'update_connection'
]






default_dag_args = {
    'start_date': datetime.datetime(2020, 8, 1)
}

dag = DAG(
    dag_id='update_tableau_datasources',
    schedule_interval='@hourly',
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=360),
    max_active_runs=1,
    default_args=default_dag_args
)

with open('dags/tableau_datasource_update/tableau_datasource_update.md') as doc_md:
    dag.doc_md = doc_md.read()

add_tasks = TableauDatasourceTasks(
    dag=dag,
    task_id='gather_datasource_update_tasks',
    snowflake_conn_id='snowflake_tableau_datasource',
    tableau_conn_id='tableau_update_datasources',
    github_conn_id='github_dbt_repo'
)

update = TableauDatasourceUpdate(
    dag=dag,
    task_id='update_datasources',
    snowflake_conn_id='snowflake_tableau_datasource',
    tableau_conn_id='tableau_update_datasources',
    tasks_task_id='gather_datasource_update_tasks'
)

refresh = PythonOperator(
    dag=dag,
    task_id='refresh_datasources',
    python_callable=refresh_datasources,
    op_kwargs={
        'tableau_conn_id': 'tableau_update_datasources',
        'tasks': "{{task_instance.xcom_pull(task_ids='gather_datasource_update_tasks')}}"
    }
)

add_tasks >> update >> refresh
