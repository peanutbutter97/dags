import pyarrow as pa
from pyarrow import fs as pa_fs
from typing import Callable, List, Optional, Tuple
import logging
import datetime as dt
import pendulum

def transform_batch(
        batch: List[Tuple],
        transform_fn: Optional[Callable] = None,
        **kwargs
    ) -> List[Tuple]:
    if transform_fn is not None:
        batch = transform_fn(batch, **kwargs)
    return batch

def write_batch(
    pa_arrays: list[pa.Array],
    pa_schema: pa.Schema,
    bucket_name: str,
    table_name: str,
    batch_num: int = 0,
) -> str:
    """
    Write a batch of PyArrow arrays to an Arrow file using RecordBatchFileWriter.

    Args:
        pa_arrays (list[pa.Array]): List of PyArrow arrays, one per column.
        pa_schema (pa.Schema): PyArrow schema for the batch.
        bucket_name (str): S3 bucket name.
        table_name (str): Table name for file naming.
        batch_num (int): Batch number for file naming.

    Returns:
        str: Path to the written Arrow file.
    """
    s3_path: str = ""
    pa_writer = None
    s3_fs = None
    try:
        if not pa_arrays or not pa_schema:
            logging.warning(f"[write_batch] No data or schema provided for batch {batch_num}")
            return None

        pa_writer, s3_fs, s3_path = init_writer(
            bucket_name=bucket_name,
            table_name=table_name,
            pa_schema=pa_schema,
            batch_num=batch_num
        )
        if pa_writer is None or s3_path is None:
            raise RuntimeError(f"Failed to initialize writer for batch {batch_num}")

        pa_record_batch = pa.RecordBatch.from_arrays(pa_arrays, schema=pa_schema)
        pa_writer.write_batch(pa_record_batch)
        logging.info(f"[write_batch] Successfully wrote batch {batch_num} to {s3_path}")
    except Exception as e:
        logging.error(f"[write_batch] Error writing batch {batch_num}: {str(e)}")
        raise
    finally:
        pa_writer.close()
        s3_fs.close()
    return s3_path

def init_writer(
    bucket_name: str,
    table_name: str,
    pa_schema: pa.Schema,
    batch_num: int = 0,
) -> Tuple[pa.ipc.RecordBatchFileWriter, pa_fs.S3FileSystem, str]:
    """
    Initialize a PyArrow RecordBatchFileWriter and generate the file path.

    Args:
        output_path (str): Directory to save the Arrow file.
        table_name (str): Table name for file naming.
        pa_schema (pa.Schema): PyArrow schema.
        batch_num (int): Batch number for filename.

    Returns:
        (RecordBatchFileWriter, batch_file_path) or (None, None) on failure.
    """
    try:
        # Generate unique filename
        date_str = dt.datetime.now().strftime('%Y%m%d')
        clean_table_name = table_name.replace('.', '_').lstrip('_')
        s3_obj_key = f"{clean_table_name}/{clean_table_name}_{date_str}_{batch_num:03d}.arrow"

        # Initialize writer and get s3 object key
        s3_path = f"s3://{bucket_name}/{s3_obj_key}"
        s3_fs = pa_fs.S3FileSystem(region="ap-southeast-1").open_output_stream(s3_path)
        
        pa_writer = pa.ipc.RecordBatchFileWriter(
            s3_fs,
            pa_schema,
            options=pa.ipc.ipcWriteOptions(compression='lz4')
        )
        return pa_writer, s3_fs, s3_path

    except Exception as e:
        logging.error(f"[init_writer] Error initializing writer for batch {batch_num}: {e}")
        return None, None
    
def create_pa_schema(cursor_desc: Tuple) -> Optional[pa.Schema]:
        # Infer schema from cursor description or data types
        pa_fields: List[pa.Field] = []
        # PostgreSQL type OID constants
        PG_INT4 = 23
        PG_INT2 = 21
        PG_INT8 = 20
        PG_FLOAT4 = 700
        PG_FLOAT8 = 701
        PG_DECIMAL = 1700
        PG_BOOL = 16
        PG_DATE = 1082
        PG_TIME = 1083
        PG_TIMESTAMP = 1114
        PG_TIMESTAMPTZ = 1184

        if not cursor_desc:
            logging.error("Cursor description is empty or None")
            return
        
        for desc in cursor_desc:
            try:
                column_name = desc.name  # Column name
                pg_type = desc.type_code      # PostgreSQL type OID
                
                # Map PostgreSQL types to PyArrow types
                if pg_type == PG_INT2:  # smallint
                    arrow_type = pa.int16()
                elif pg_type == PG_INT4:  # integer
                    arrow_type = pa.int32()
                elif pg_type == PG_INT8:  # bigint
                    arrow_type = pa.int64()
                elif pg_type == PG_FLOAT4:  # float
                    arrow_type = pa.float32()
                elif pg_type == PG_FLOAT8:  # double
                    arrow_type = pa.float64()
                elif pg_type == PG_DECIMAL:  # numeric / decimal
                    if desc.precision is not None and desc.scale is not None:
                        arrow_type = pa.decimal128(desc.precision, desc.scale)
                    else:
                        arrow_type = pa.decimal128(38, 9)  # fallback
                elif pg_type == PG_BOOL: # boolean
                    arrow_type = pa.bool_()
                elif pg_type == PG_DATE:  # date
                    arrow_type = pa.date32()
                elif pg_type == PG_TIME:  # time
                    arrow_type = pa.time64('us')
                elif pg_type == PG_TIMESTAMP:  # timestamp without timezone (assume UTC)
                    arrow_type = pa.timestamp('us', tz='UTC')
                elif pg_type == PG_TIMESTAMPTZ:  # timestamp with timezone
                    arrow_type = pa.timestamp('us', tz='UTC')
                else:  # Default to string for other types
                    arrow_type = pa.string()
                pa_fields.append(pa.field(column_name, arrow_type))
            except Exception as e:
                logging.error(
                    f"[create_pa_schema] Error processing column '{column_name}' "
                    f"(type_code={pg_type}): {str(e)}"
                )
                raise
        pa_schema = pa.schema(pa_fields)
        logging.info(f"[create_pa_schema] Created schema with {len(pa_fields)} fields")
        return pa_schema

def convert_data_to_arrow(batch: List[Tuple], pa_schema: pa.Schema) -> Optional[pa.RecordBatch]:
    """Convert psycopg2 result tuples to PyArrow format with schema inference."""
    logging.info(f"[convert_data_to_arrow] Converting {len(batch)} rows to PyArrow format")
    
    # Check for empty batch
    if not batch:
        logging.warning("[convert_data_to_arrow] Empty batch provided")
        return
        
    num_columns = len(batch[0]) if batch else 0
    
    # Convert data to PyArrow arrays
    pa_array = []
    for i in range(num_columns):
        try:
            column_data = [row[i] if i < len(row) else None for row in batch]
            
            # Convert data based on schema type
            field_type = pa_schema.field(i).type
            if isinstance(field_type, pa.TimestampType):
                # Handle datetime objects for timestamp types using pendulum
                converted_data = []
                for dt_val in column_data:
                    if dt_val is None:
                        converted_data.append(None)
                    elif isinstance(dt_val, dt.datetime):
                        # Convert to UTC using pendulum for better timezone handling
                        if dt_val.tzinfo is None:
                            # Assume UTC for naive datetime
                            pendulum_dt = pendulum.instance(dt_val, tz='UTC')
                        else:
                            pendulum_dt = pendulum.instance(dt_val).in_timezone('UTC')
                        converted_data.append(pendulum_dt)
                    else:
                        converted_data.append(None)
                column_data = converted_data
            elif field_type == pa.date32():
                # Handle date objects using pendulum
                converted_data = []
                for dt_val in column_data:
                    if dt_val is None:
                        converted_data.append(None)
                    elif isinstance(dt_val, dt.datetime):
                        pendulum_dt = pendulum.instance(dt_val)
                        converted_data.append(pendulum_dt.date())
                    elif isinstance(dt_val, dt.date):
                        converted_data.append(dt_val)
                    else:
                        converted_data.append(None)
                column_data = converted_data
            elif isinstance(field_type, pa.Time64Type):
                # Handle time objects using pendulum
                converted_data = []
                for dt_val in column_data:
                    if dt_val is None:
                        converted_data.append(None)
                    elif isinstance(dt_val, dt.datetime):
                        pendulum_dt = pendulum.instance(dt_val)
                        converted_data.append(pendulum_dt.time())
                    elif isinstance(dt_val, dt.time):
                        converted_data.append(dt_val)
                    else:
                        converted_data.append(None)
                column_data = converted_data
        except Exception as e:
            logging.error(f"Error converting column '{i}' to {field_type}: {str(e)}")
            raise
        pa_array.append(pa.array(column_data, type=field_type))

    logging.info(f"[convert_data_to_arrow] Successfully created PyArrow arrays with {len(pa_array)} columns")
    return pa_array


def read_arrow_s3(s3_path: str, region: str = "ap-southeast-1") -> pa.Table:
    """
    Read an Arrow (.arrow) file from S3 and return a PyArrow Table.

    :param bucket_name: str, S3 bucket name
    :param object_key: str, key of the .arrow file in the bucket
    :param region: str, AWS region
    :return: pyarrow.Table
    """
    s3_fs = None
    pa_reader = None
    try:
        logging.info(f"[read_arrow_s3] Opening Arrow file from S3: s3://{s3_path}")
        
        s3_fs = pa_fs.S3FileSystem(region=region).open_input_file(s3_path)
        pa_reader = pa.ipc.RecordBatchFileReader(s3_fs)
        pa_table = pa_reader.read_all()
        
        logging.info(f"[read_arrow_s3] Successfully read {pa_table.num_rows} rows and {pa_table.num_columns} columns")
        return pa_table

    except Exception as e:
        logging.error(f"[read_arrow_s3] Error reading Arrow file: {e}")
        return pa.Table.from_arrays([], [])
    finally:
        if pa_reader:
            pa_reader.close()
        if s3_fs:
            s3_fs.close()
    
def add_metadata_to_pa_tbl(pa_table: pa.Table, dag_run_id: str) -> pa.Table:
    dag_run_id_col = pa.array([dag_run_id] * pa_table.num_rows, type=pa.string())
    pa_table = pa_table.append_column('dag_run_id', dag_run_id_col)
    return pa_table

def get_dest_table_name(source_table_name: str, prefix: str = "etl") -> str:
    prefix: str = "s"
    tbl_part = source_table_name.rsplit('.', 1)
    dest_table_name: str = ""

    if len(tbl_part) == 1:
        # No schema specified, just table name
        dest_table_name = f"{prefix}_{tbl_part[0]}"
    else:
        # Schema.table format
        schema, table = tbl_part
        if not schema or not table:
            raise ValueError("Invalid table name format: both schema and table parts must be non-empty")
        dest_table_name = f"{schema}.{prefix}_{table}"
    return dest_table_name