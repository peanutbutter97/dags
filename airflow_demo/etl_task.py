from airflow_demo.etl_func import transform_batch, write_batch, convert_data_to_arrow, create_pa_schema, add_metadata_to_pa_tbl, read_arrow_s3, cleanup_s3_arrow_files
from airflow_demo.db_func import init_db_conn, fetch_batch, close_db_connection, count_tbl_row, insert_batch_to_table, table_exists, drop_table_if_exists, create_staging_table_like, atomic_table_swap
from airflow.decorators import task
from typing import Dict
import logging
import math
from typing import List, Optional, Tuple
from airflow.utils.trigger_rule import TriggerRule
from psycopg2 import sql

@task(max_active_tis_per_dag=1)
def get_batch_params(conn_params: Dict, source_table_name: str, batch_size: int, where: str = "", **context) -> list[dict]:
    """Return list of batch parameters for dynamic task mapping."""
    conn = None
    cur = None
    logging.info(f"[get_batch_params] DAG Run ID: {context['dag_run'].run_id}")
    try:
        conn = init_db_conn(**conn_params)
        total_rows = count_tbl_row(conn, source_table_name, where=where)
        total_batches = math.ceil(total_rows / batch_size)
        logging.info(f"[get_batch_params] Total rows={total_rows}, batches={total_batches}")

        # Return list of dictionaries (one per batch)
        return [{ 'batch_num': i } for i in range(total_batches)]
    finally:
        close_db_connection(conn, cur)

@task(max_active_tis_per_dag=2)
def extract_batch(
    conn_params: Dict,
    bucket_name: str,
    source_table_name: str,
    batch_num: int,
    batch_size: int,
    where: str = "",
    **context
) -> bool:
    """ETL for a single batch."""
    conn = None
    cur = None
    batch_params: Dict = {
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

@task(max_active_tis_per_dag=1)
def prepare_staging_table(conn_params: Dict, dest_table_name: str, **context) -> str:
    """Prepare staging table with same DDL as destination table."""
    conn = None
    
    # Generate staging table name
    staging_table_name = f"{dest_table_name}_staging"
    
    try:
        logging.info(f"[prepare_staging_table] Preparing staging table {staging_table_name}")
        conn = init_db_conn(**conn_params)
        
        # Drop staging table if it exists
        if table_exists(conn, staging_table_name):
            drop_table_if_exists(conn, staging_table_name)
            logging.info(f"[prepare_staging_table] Dropped existing staging table {staging_table_name}")
        
        # Create staging table using LIKE syntax
        if table_exists(conn, dest_table_name):
            create_staging_table_like(conn, dest_table_name, staging_table_name)
            logging.info(f"[prepare_staging_table] Created staging table {staging_table_name} using LIKE syntax")
        else:
            logging.warning(f"[prepare_staging_table] Destination table {dest_table_name} does not exist")
            raise ValueError(f"Destination table {dest_table_name} must exist to create staging table")
        
        return staging_table_name
        
    except Exception as e:
        logging.error(f"[prepare_staging_table] Error preparing staging table: {str(e)}")
        raise
    finally:
        close_db_connection(conn, None)

@task(max_active_tis_per_dag=2)
def load_batch(
    conn_params: Dict,
    batch_num: int,
    s3_path: str,
    dest_table_name_staging: str,
    column_names: Optional[List[str]] = None,
    **context
) -> Dict:
    """
    Load a batch of data from an Arrow file into PostgreSQL staging table
    using insert_batch_to_table.
    """
    conn = None
    
    logging.info(f"[load_batch] DAG Run ID: {context['dag_run'].run_id}")
    logging.info(f"[load_batch] Using staging table: {dest_table_name_staging}")
    try:
        # 1. Read Arrow file
        logging.info(f"[load_batch] Reading Arrow file {s3_path}")
        pa_table = read_arrow_s3(s3_path)
        
        if pa_table.num_rows == 0:
            logging.warning(f"[load_batch] No data in Arrow file {s3_path}")
            return {}

        pa_table = add_metadata_to_pa_tbl(pa_table, context['dag_run'].run_id)
        
        # 2. Convert to list of tuples
        data = [
            tuple(pa_table.column(i)[row_index].as_py() for i in range(pa_table.num_columns))
            for row_index in range(pa_table.num_rows)
        ]

        # 3. Connect to Postgres
        logging.info(f"[load_batch] Connecting to DB at {conn_params['host']}")
        conn = init_db_conn(**conn_params)

        # 4. Insert batch into staging table
        rows_inserted = insert_batch_to_table(conn, dest_table_name_staging, data, column_names)
        logging.info(f"[load_batch] Successfully inserted {rows_inserted} rows into staging table {dest_table_name_staging}")
    except Exception as e:
        logging.error(f"[load_batch] Error inserting batch {batch_num} into staging table {dest_table_name_staging}: {str(e)}")
        raise
    finally:
        close_db_connection(conn, None)
    return {}

@task(max_active_tis_per_dag=1)
def post_load(conn_params: Dict, bucket_name: str, source_table_name, dest_table_name: str, dest_table_name_staging: str, **context) -> bool:
    """Perform atomic table swap: staging -> dest, drop old."""
    conn = None
    is_success: bool = False
    
    try:
        logging.info(f"[post_load] Starting atomic table swap")
        conn = init_db_conn(**conn_params)
        
        # Perform atomic table swap
        is_success = atomic_table_swap(conn, dest_table_name, dest_table_name_staging)
        logging.info(f"[post_load] Successfully completed table swap")
    except Exception as e:
        logging.error(f"[post_load] Error during table swap: {str(e)}")
        raise
    finally:
        close_db_connection(conn, None)

    if is_success:
        # Cleanup S3 Arrow files
        clean_table_name = source_table_name.replace('.', '__').lstrip('_')
        cleanup_s3_arrow_files(bucket=bucket_name, prefix=clean_table_name)
        return True
    return False

@task(trigger_rule=TriggerRule.ONE_FAILED)
def rollback_on_failure(conn_params: Dict, dest_table_name: str, dest_table_name_staging: str, **context) -> bool:
    """
    Rollback logic:
    - Drop staging table if exists.
    - If dest_table_name_old exists:
        - Drop dest_table_name if exists.
        - Rename dest_table_name_old to dest_table_name.
    """
    conn = None
    cur = None
    old_table_name = f"{dest_table_name}_old"
    try:
        logging.info(f"[rollback_task] Connecting to DB at {conn_params['host']}")
        conn = init_db_conn(**conn_params)
        cur = conn.cursor()

        # Drop staging table if exists
        logging.info(f"[rollback_task] Dropping staging table if exists: {dest_table_name_staging}")
        cur.execute(
            sql.SQL("DROP TABLE IF EXISTS {}").format(
                sql.Identifier(*dest_table_name_staging.split('.'))
            )
        )

        # Check if old table exists
        logging.info(f"[rollback_task] Checking if old table exists: {old_table_name}")
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            )
            """,
            (old_table_name.split('.')[0] if '.' in old_table_name else 'public', old_table_name.split('.')[-1])
        )
        old_exists = cur.fetchone()[0]

        if old_exists:
            # Drop dest_table_name if exists
            logging.info(f"[rollback_task] Dropping dest table if exists: {dest_table_name}")
            cur.execute(
                sql.SQL("DROP TABLE IF EXISTS {}").format(
                    sql.Identifier(*dest_table_name.split('.'))
                )
            )
            # Rename old table back to dest_table_name
            logging.info(f"[rollback_task] Renaming {old_table_name} to {dest_table_name}")
            cur.execute(
                sql.SQL("ALTER TABLE {} RENAME TO {}").format(
                    sql.Identifier(*old_table_name.split('.')),
                    sql.Identifier(dest_table_name.split('.')[-1])
                )
            )

        conn.commit()
        logging.info("[rollback_task] Rollback completed successfully")
    except Exception as e:
        logging.error(f"[rollback_task] Error during rollback: {str(e)}")
        if conn:
            conn.rollback()
        raise
    finally:
        close_db_connection(conn, cur)
    return True