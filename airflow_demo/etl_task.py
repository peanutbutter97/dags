from airflow_demo.etl_func import transform_batch, write_batch, convert_data_to_arrow, create_pa_schema, add_metadata_to_pa_tbl, read_arrow_s3
from airflow_demo.db_func import init_db_conn, fetch_batch, close_db_connection, count_tbl_row, insert_batch_to_table
from airflow_demo.aws_func import upload_to_public_s3
from airflow.decorators import task
from typing import Dict
import logging
import math
from typing import List, Optional, Tuple
import pyarrow.ipc as ipc
from airflow.utils.trigger_rule import TriggerRule
from psycopg2 import sql
import os

@task(max_active_tis_per_dag=1)
def get_batch_params(conn_params: Dict, bucket_name: str, source_table_name: str, dest_table_name: str, batch_size: int, where: str = "", **context) -> list[dict]:
    """Return list of batch parameters for dynamic task mapping."""
    conn = None
    cur = None
    logging.info(f"[get_batch_params] DAG Run ID: {context['dag_run'].run_id}")
    logging.info(f"[get_batch_params] dest_table_name: {dest_table_name}")
    try:
        conn = init_db_conn(**conn_params)
        total_rows = count_tbl_row(conn, source_table_name, where=where)
        total_batches = math.ceil(total_rows / batch_size)
        logging.info(f"[get_batch_params] Total rows={total_rows}, batches={total_batches}")

        # Return list of dictionaries (one per batch)
        return [
            {
                'conn_params': conn_params,
                'bucket_name': bucket_name,
                'source_table_name': source_table_name,
                'dest_table_name': dest_table_name,
                'batch_num': i,
                'batch_size': batch_size,
                'where': where,
            }
            for i in range(total_batches)
        ]
    finally:
        close_db_connection(conn, cur)

@task(max_active_tis_per_dag=2)
def extract_batch(
    conn_params: Dict,
    bucket_name: str,
    source_table_name: str,
    dest_table_name: str,
    batch_num: int,
    batch_size: int,
    where: str = "",
    **context
) -> bool:
    """ETL for a single batch."""
    conn = None
    cur = None
    batch_params: Dict = {
        'conn_params': conn_params,
        'source_table_name': source_table_name,
        'dest_table_name': dest_table_name,
        'batch_num': batch_num,
    }
    logging.info(f"[get_batch_params] DAG Run ID: {context['dag_run'].run_id}")
    try:
        # --- Extract phase ---
        logging.info(f"[extract_batch] Connecting to DB at {conn_params['host']}")
        conn = init_db_conn(**conn_params)
        cur, rows = fetch_batch(conn, source_table_name, batch_num=batch_num, batch_size=batch_size, where=where)
        data = [list(row) for row in rows] if rows else []
        logging.info(f"[extract_batch] Batch {batch_num}: fetched {len(data)} rows")
        if not data:
            logging.error(f"No data fetched for batch {batch_num}")
            return batch_params

        # --- Convert to PyArrow ---
        pa_schema = create_pa_schema(cur.description)
        pa_arrays = convert_data_to_arrow(data, pa_schema)

        # --- Transform ---
        transformed_arrays = transform_batch(pa_arrays, transform_fn=None)

        # --- Write ---
        s3_path = write_batch(
            pa_arrays=transformed_arrays,
            pa_schema=pa_schema,
            bucket_name=bucket_name,
            table_name=source_table_name,
            batch_num=batch_num,
        )
        batch_params["s3_path"] = s3_path
        logging.info(f"[extract_batch] Batch {batch_num} written to {s3_path}")
    except Exception as e:
        logging.error(f"[extract_batch] Error in batch {batch_num}: {str(e)}")
        return batch_params
    finally:
        close_db_connection(conn, cur)
    return batch_params

@task(max_active_tis_per_dag=2)
def load_batch(
    conn_params: Dict,
    source_table_name: str,
    dest_table_name: str,
    batch_num: int,
    s3_path: str,
    column_names: Optional[List[str]] = None,
    **context
) -> Dict:
    """
    Load a batch of data from an Arrow file into PostgreSQL table
    using insert_batch_to_table.
    """
    conn = None
    
    logging.info(f"[get_batch_params] DAG Run ID: {context['dag_run'].run_id}")
    try:
        # 1. Read Arrow file
        logging.info(f"[load_batch] Reading Arrow file {s3_path}")
        pa_table = read_arrow_s3(s3_path)
        
        if pa_table.num_rows == 0:
            logging.warning(f"[load_batch] No data in Arrow file {s3_path}")
            return False

        pa_table = add_metadata_to_pa_tbl(pa_table, context['dag_run'].run_id)
        
        # 2. Convert to list of tuples
        data = [
            tuple(pa_table.column(i)[row_index].as_py() for i in range(pa_table.num_columns))
            for row_index in range(pa_table.num_rows)
        ]

        # 3. Connect to Postgres
        logging.info(f"[load_batch] Connecting to DB at {conn_params['host']}")
        conn = init_db_conn(**conn_params)

        # 4. Insert batch
        rows_inserted = insert_batch_to_table(conn, dest_table_name, data, column_names)
        logging.info(f"[load_batch] Successfully inserted {rows_inserted} rows into {dest_table_name}")

    except Exception as e:
        logging.error(f"[load_batch] Error inserting batch {batch_num} into {dest_table_name}: {str(e)}")
        raise
    finally:
        close_db_connection(conn, None)
    return {}

@task(trigger_rule=TriggerRule.ONE_FAILED)
def rollback_on_failure(conn_params: Dict, target_table_name: str, **context) -> bool:
    """This task runs only if at least one load task fails"""
    conn = None
    cur = None
    dag_run_id: str = context['dag_run'].run_id
    try:
        # Connect to Postgres
        logging.info(f"[rollback_task] Connecting to DB at {conn_params['host']}")
        conn = init_db_conn(**conn_params)
        cur = conn.cursor()
        
        # Build DELETE query safely
        delete_query = sql.SQL("DELETE FROM {} WHERE dag_run_id = %s").format(
            sql.Identifier(*target_table_name.split('.'))
        )
        logging.info(f"[rollback_task] Executing DELETE for dag_run_id={dag_run_id}")
        cur.execute(delete_query, (dag_run_id,))
        deleted_rows = cur.rowcount
        conn.commit()
        
        logging.info(f"[rollback_task] Deleted {deleted_rows} rows from {target_table_name}")
    except Exception as e:
        logging.error(f"[rollback_task] Error during rollback: {str(e)}")
        if conn:
            conn.rollback()
        raise
    finally:
        close_db_connection(conn, cur)
    return True