import sys
sys.path.append('/app')

from airflow.decorators import dag
from datetime import datetime
from typing import Dict, List
from airflow_demo.etl_task import get_batch_params, extract_batch, load_batch, rollback_on_failure, prepare_staging_table, post_load
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
    batch_size: int = 100_000
    bucket_name: str = "mq-de-airflow-demo-etl"
    where: str = ""
    dest_table_name = get_dest_table_name(source_table_name)
    column_names: List[str] = []
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
    prepare_staging_table_task = prepare_staging_table(conn_params, dest_table_name)

    # 3. Dynamically create extract tasks per batch
    extract_batch_task = extract_batch.partial(
        conn_params=conn_params,
        source_table_name=source_table_name,
        where=where,
        batch_size=batch_size,
        bucket_name=bucket_name,
        column_names=column_names,
    ).expand_kwargs(batch_params_task)

    # 4. Map load tasks per batch after extract
    load_batch_task = load_batch.partial(
        conn_params=conn_params,
        source_table_name=source_table_name,
        dest_table_name_staging=prepare_staging_table_task,
        column_names=column_names,
    ).expand_kwargs(extract_batch_task)

    # 5. Finalize table swap after all loads complete
    post_load_task = post_load(conn_params, bucket_name, source_table_name, dest_table_name, prepare_staging_table_task)

    # 6. Rollback task
    rollback_task = rollback_on_failure(conn_params, dest_table_name, prepare_staging_table_task)

    # Set dependencies
    batch_params_task >> prepare_staging_table_task >> extract_batch_task >> load_batch_task >> post_load_task
    load_batch_task >> rollback_task

# Instantiate the DAG
dag_instance = postgres_arrow_etl_dag()