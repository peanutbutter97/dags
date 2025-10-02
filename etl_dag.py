import sys
sys.path.append('/app')

from airflow.decorators import dag
from datetime import datetime
from typing import Dict, List
from airflow_demo.etl_task import get_batch_params, extract_batch, load_batch, rollback_on_failure, prepare_staging_table, finalize_table_swap
from airflow_demo.etl_func import get_dest_table_name

@dag(
    dag_id='postgres_arrow_etl_dag',
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    description='PostgreSQL to Arrow ETL pipeline using Airflow with staging table',
)
def postgres_arrow_etl_dag():
    db_host = "mq-airflow-etl-test.cwanclutkkrz.ap-southeast-1.rds.amazonaws.com"
    source_table_name: str = "public.dummy_tbl"
    batch_size: int = 400
    bucket_name: str = "mq-de-airflow-demo-etl"
    where: str = ""
    dest_table_name = get_dest_table_name(source_table_name)
    conn_params: Dict[str, str] = {
        'dbname': "postgres",
        'user': "postgres",
        'password': "postgres",
        'host': db_host,
    }

    # 1. Calculate total batches
    batch_params_task = get_batch_params(
        conn_params=conn_params,
        source_table_name=source_table_name,
        batch_size=batch_size,
        where=where
    )

    # 2. Prepare staging table
    staging_table_name_task = prepare_staging_table(conn_params, dest_table_name)

    # 3. Dynamically create extract tasks per batch
    extract_batch_task = extract_batch.partial(
        bucket_name=bucket_name,
        dest_table_name=dest_table_name,
    ).expand_kwargs(batch_params_task)

    # 4. Map load tasks per batch after extract
    load_batch_task = load_batch.partial(
        dest_table_name_staging=staging_table_name_task
    ).expand_kwargs(extract_batch_task)

    # 5. Finalize table swap after all loads complete
    finalize_table_swap_task = finalize_table_swap(conn_params, dest_table_name, staging_table_name_task)

    # Set dependencies
    batch_params_task >> staging_table_name_task >> extract_batch_task >> load_batch_task >> finalize_table_swap_task
    # load_batch_task >> rollback_task

# Instantiate the DAG
dag_instance = postgres_arrow_etl_dag()