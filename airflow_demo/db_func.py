import psycopg2
from typing import List, Tuple, Optional
from psycopg2 import sql
import logging

def init_db_conn(
        dbname: str,
        user: str,
        password: str,
        host: str,
        port: int = 5432
    ) -> psycopg2.extensions.connection:
    """Initialize database connection."""
    try:
        logging.info(f"[init_db_conn] Connecting to database {dbname} at {host}:{port}")
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        logging.info("[init_db_conn] Database connection established successfully")
        return conn
    except Exception as e:
        logging.error(f"[init_db_conn] Failed to connect to database: {str(e)}")

def fetch_batch(
    conn,
    table_name: str,
    batch_num: int,
    batch_size: int = 100_000,
    where: str = ""
) -> Tuple[psycopg2.extensions.cursor, List[Tuple]]:
    """
    Fetch a specific batch of rows from a PostgreSQL table using LIMIT and OFFSET.

    Args:
        conn: Active psycopg2 connection.
        table_name (str): Table name (can include schema, e.g., 'public.my_table').
        batch_num (int): Batch number (0-based).
        batch_size (int): Number of rows per batch.
        where (str): Optional WHERE clause (without 'WHERE').

    Returns:
        List[Tuple]: List of rows for this batch.
    """
    cur = None
    try:
        cur = conn.cursor()
        offset: int = batch_num * batch_size
        
        # Build base query
        query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(*table_name.split('.')))
        
        # Add WHERE clause if provided
        if where:
            query = sql.SQL("{} WHERE {}").format(query, sql.SQL(where))
            logging.info(f"[fetch_batch] Adding WHERE clause: {where}")
        
        # Add LIMIT and OFFSET for batch
        query = sql.SQL("{} LIMIT {} OFFSET {}").format(query, sql.Literal(batch_size), sql.Literal(offset))
        logging.info(f"[fetch_batch] Executing query for batch {batch_num}: {query.as_string(conn)}")

        cur.execute(query)
        rows = cur.fetchall()
        logging.info(f"[fetch_batch] Batch {batch_num} fetched {len(rows)} rows")
        return cur, rows

    except Exception as e:
        logging.error(f"[fetch_batch] Error fetching batch {batch_num}: {str(e)}")
        return None

def close_db_connection(conn, cur):
    """Close database connection and cursor."""
    try:
        logging.info("[close_db_connection] Closing database connections")
        if cur is not None:
            cur.close()
            logging.info("[close_db_connection] Cursor closed successfully")
        if conn is not None:
            conn.close()
            logging.info("[close_db_connection] Database connection closed successfully")
    except Exception as e:
        logging.error(f"[close_db_connection] Error closing database connections: {str(e)}")

def insert_batch_to_table(
    conn,
    table_name: str,
    data: List[Tuple],
    column_names: Optional[List[str]] = None
) -> int:
    """Insert batch data to PostgreSQL table."""
    # Guard clause for empty data
    if not data:
        logging.warning(f"[insert_batch_to_table] No data provided for table {table_name}")
        return 0
        
    logging.info(f"[insert_batch_to_table] Inserting {len(data)} rows to table {table_name}")
    cur = conn.cursor()
    try:
        # Build INSERT statement
        if column_names:
            columns_sql = sql.SQL(', ').join(map(sql.Identifier, column_names))
            placeholders_sql = sql.SQL(', ').join(sql.Placeholder() * len(column_names))
            query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                sql.Identifier(*table_name.split('.')),
                columns_sql,
                placeholders_sql
            )
            logging.info(f"[insert_batch_to_table] Using specified columns: {column_names}")
        else:
            # Assume all columns if not specified
            num_col = len(data[0])
            placeholders_sql = sql.SQL(', ').join(sql.Placeholder() * num_col)
            query = sql.SQL("INSERT INTO {} VALUES ({})").format(
                sql.Identifier(*table_name.split('.')),
                placeholders_sql
            )
            logging.info("[insert_batch_to_table] Using all columns (no column names specified)")
        
        logging.info(f"[insert_batch_to_table] Executing SQL: {query}")
        
        # Execute batch insert
        cur.executemany(query, data)
        conn.commit()
        
        rows_inserted = cur.rowcount
        logging.info(f"[insert_batch_to_table] Successfully inserted {rows_inserted} rows to {table_name}")
        return rows_inserted
    except Exception as e:
        logging.error(f"[insert_batch_to_table] Error during insert, rolling back transaction: {str(e)}")
        conn.rollback()
        raise
    
def count_tbl_row(
        conn,
        table_name: str,
        where: str = ""
    ) -> int:
    """
    Count total rows in a PostgreSQL table, optionally with a filter.

    Args:
        conn (psycopg2.connection): Active database connection.
        table_name (str): Table name (can include schema, e.g., 'public.my_table').
        where (str, optional): Optional WHERE clause (without 'WHERE').

    Returns:
        int: Total row count.
    """
    cur = None
    if conn is None:
        logging.error("[count_tbl_row] No active database connection provided")
        return 0
    try:
        cur = conn.cursor()
        # Base query
        query = sql.SQL("SELECT COUNT(*) FROM {}").format(
            sql.Identifier(*table_name.split('.'))
        )

        # Add WHERE clause if provided
        if where:
            query = sql.SQL("{} WHERE {}").format(query, sql.SQL(where))
            logging.info(f"[count_tbl_row] Applying filter: {where}")

        cur.execute(query)
        total_rows = cur.fetchone()[0]
        logging.info(f"[count_tbl_row] Table {table_name} has {total_rows} rows (where='{where}')")
        return total_rows

    except Exception as e:
        logging.error(f"[count_tbl_row] Error counting rows in {table_name}: {str(e)}")

def table_exists(conn, table_name: str) -> bool:
    """Check if a table exists."""
    cur = None
    try:
        cur = conn.cursor()
        schema_name, tbl_name = table_name.split('.') if '.' in table_name else ('public', table_name)
        
        check_query = """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = %s AND table_name = %s
        );
        """
        cur.execute(check_query, (schema_name, tbl_name))
        exists = cur.fetchone()[0]
        logging.info(f"[table_exists] Table {table_name} exists: {exists}")
        return exists
        
    except Exception as e:
        logging.error(f"[table_exists] Error checking if table {table_name} exists: {str(e)}")
        raise
    finally:
        if cur:
            cur.close()

def drop_table_if_exists(conn, table_name: str) -> bool:
    """Drop table if it exists."""
    cur = None
    try:
        cur = conn.cursor()
        drop_query = sql.SQL("DROP TABLE IF EXISTS {}").format(
            sql.Identifier(*table_name.split('.'))
        )
        cur.execute(drop_query)
        conn.commit()
        logging.info(f"[drop_table_if_exists] Dropped table {table_name} if it existed")
        return True
        
    except Exception as e:
        logging.error(f"[drop_table_if_exists] Error dropping table {table_name}: {str(e)}")
        conn.rollback()
        raise
    finally:
        if cur:
            cur.close()

def atomic_table_swap(conn, dest_table_name: str, staging_table_name: str) -> bool:
    """Perform atomic table swap: dest -> old, staging -> dest, drop old."""
    cur = None
    try:
        cur = conn.cursor()
        
        # Generate old table name
        old_table_name = f"{dest_table_name}_old"
        
        # Start transaction for atomic operation
        cur.execute("BEGIN;")
        
        # Step 1: Rename current table to _old (if exists)
        if table_exists(conn, dest_table_name):
            rename_query = sql.SQL("ALTER TABLE {} RENAME TO {}").format(
                sql.Identifier(*dest_table_name.split('.')),
                sql.Identifier(old_table_name.split('.')[-1])
            )
            cur.execute(rename_query)
            logging.info(f"[atomic_table_swap] Renamed {dest_table_name} to {old_table_name}")
        
        # Step 2: Rename staging table to destination
        rename_query = sql.SQL("ALTER TABLE {} RENAME TO {}").format(
            sql.Identifier(*staging_table_name.split('.')),
            sql.Identifier(dest_table_name.split('.')[-1])
        )
        cur.execute(rename_query)
        logging.info(f"[atomic_table_swap] Renamed {staging_table_name} to {dest_table_name}")
        
        # Step 3: Drop old table (if exists)
        if table_exists(conn, old_table_name):
            drop_query = sql.SQL("DROP TABLE {}").format(
                sql.Identifier(*old_table_name.split('.'))
            )
            cur.execute(drop_query)
            logging.info(f"[atomic_table_swap] Dropped old table {old_table_name}")
        
        # Commit transaction
        cur.execute("COMMIT;")
        logging.info(f"[atomic_table_swap] Successfully completed atomic table swap")
        return True
        
    except Exception as e:
        logging.error(f"[atomic_table_swap] Error during table swap: {str(e)}")
        if cur:
            cur.execute("ROLLBACK;")
        raise
    finally:
        if cur:
            cur.close()

def create_staging_table_like(conn, dest_table_name: str, staging_table_name: str) -> bool:
    """Create staging table using CREATE TABLE ... LIKE syntax."""
    cur = None
    try:
        cur = conn.cursor()
        
        # Create staging table with same structure as destination table
        create_query = sql.SQL("CREATE TABLE {} (LIKE {} INCLUDING ALL)").format(
            sql.Identifier(*staging_table_name.split('.')),
            sql.Identifier(*dest_table_name.split('.'))
        )
        cur.execute(create_query)
        conn.commit()
        logging.info(f"[create_staging_table_like] Created staging table {staging_table_name} like {dest_table_name}")
        return True
        
    except Exception as e:
        logging.error(f"[create_staging_table_like] Error creating staging table {staging_table_name}: {str(e)}")
        conn.rollback()
        raise
    finally:
        if cur:
            cur.close()