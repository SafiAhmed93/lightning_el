import argparse
import decimal
import uuid
import os
import queue
import threading
import time
import importlib

import pandas as pd
import pyarrow as pa
from sqlalchemy import create_engine

# --- Helpers ---

def get_adbc_connection(driver_name: str, conn_str: str):
    """Dynamically load and return an ADBC connection based on the driver name."""
    try:
        module = importlib.import_module(f"{driver_name}.dbapi")
        return module.connect(conn_str)
    except ImportError:
        raise ValueError(f"ADBC driver '{driver_name}' not installed. Try running: pip install {driver_name}")

def normalize_chunk(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize Pandas DataFrames to prevent Arrow conversion errors."""
    if df.empty: return df
    for column_name in df.columns:
        first_valid_index = df[column_name].first_valid_index()
        if first_valid_index is None: continue
        sample_value = df.at[first_valid_index, column_name]
        if isinstance(sample_value, decimal.Decimal):
            df[column_name] = df[column_name].astype("float64")
        elif isinstance(sample_value, uuid.UUID):
            df[column_name] = df[column_name].astype("string")
    return df

# --- Extractors ---

def extract_sqlalchemy(conn_str, db, schema, table, chunk_size, query=None):
    """Yield chunks of data as Arrow tables using SQLAlchemy and Pandas."""
    engine = create_engine(conn_str)
    if not query:
        table_ref = f"{schema}.{table}" if schema else table
        query = f"SELECT * FROM {table_ref}"
    
    with engine.connect() as conn:
        if db:
            try: conn.execute(f"USE {db}")
            except: pass
            
        for chunk_df in pd.read_sql_query(query, conn, chunksize=chunk_size):
            normalized = normalize_chunk(chunk_df)
            yield pa.Table.from_pandas(normalized, preserve_index=False)


def extract_turbodbc(conn_str, db, schema, table, chunk_size, query=None):
    """Yield chunks of data natively as Arrow tables using Turbodbc."""
    import turbodbc
    options = turbodbc.make_options(prefer_unicode=True, use_async_io=True, read_buffer_size=turbodbc.Rows(chunk_size))
    
    # Attempt to sniff if local connection requires trust
    trust_cert = "yes" if "localhost" in conn_str or "127.0.0.1" in conn_str else "no"
    
    with turbodbc.connect(connection_string=conn_str, turbodbc_options=options) as conn:
        with conn.cursor() as cursor:
            if db:
                try: cursor.execute(f"USE {db}")
                except: pass
                
            if not query:
                table_ref = f"{schema}.{table}" if schema else table
                
                # Metadata detection for SQL Server to cast GUIDs to VARCHAR
                is_mssql = "SQL Server" in conn_str or "mssql" in conn_str.lower()
                query = f"SELECT * FROM {table_ref}"
                
                if is_mssql and schema:
                    try:
                        cursor.execute(f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}' ORDER BY ORDINAL_POSITION")
                        columns = cursor.fetchall()
                        if columns:
                            select_parts = []
                            for col_name, data_type in columns:
                                if data_type.lower() == 'uniqueidentifier':
                                    select_parts.append(f"CAST([{col_name}] AS VARCHAR(36)) AS [{col_name}]")
                                else:
                                    select_parts.append(f"[{col_name}]")
                            query = f"SELECT {', '.join(select_parts)} FROM {table_ref}"
                    except Exception as e:
                        print(f"Warning: Failed to fetch metadata for GUID casting: {e}")

            cursor.execute(query)
            for batch in cursor.fetcharrowbatches():
                yield batch


def extract_adbc(conn_str, driver, db, schema, table, chunk_size, query=None):
    """Yield chunks of data natively as Arrow tables using ADBC."""
    with get_adbc_connection(driver, conn_str) as conn:
        with conn.cursor() as cursor:
            # ADBC expects standard table names, schema mapping depends on the driver.
            if not query:
                table_ref = f"{schema}.{table}" if schema else table
                query = f"SELECT * FROM {table_ref}"
            cursor.execute(query)
            reader = cursor.fetch_record_batch()
            for batch in reader:
                # Group single RecordBatch into an Arrow Table
                yield pa.Table.from_batches([batch])

# --- Ingestors ---

class SqlAlchemyIngestor:
    """Ingest data using SQLAlchemy and Pandas `to_sql` method."""
    def __init__(self, conn_str, db, schema, table, truncate=False):
        self.engine = create_engine(conn_str)
        self.schema = schema
        self.table = table
        self.truncate = truncate

    def setup(self, arrow_table=None):
        if self.truncate:
            with self.engine.begin() as conn:
                from sqlalchemy import text
                table_ref = f"{self.schema}.{self.table}" if self.schema else self.table
                try:
                    conn.execute(text(f"TRUNCATE TABLE {table_ref}"))
                    print(f"Truncated table: {table_ref}")
                except Exception as e:
                    print(f"Failed to truncate table {table_ref} (it might not exist): {e}")

    def ingest(self, chunk_num, arrow_table):
        df = arrow_table.to_pandas()
        # If we truncated, we just append. Otherwise, first chunk replaces.
        if_exists = "append" if (chunk_num > 1 or self.truncate) else "replace"
        with self.engine.begin() as conn:
            df.to_sql(self.table, conn, schema=self.schema, if_exists=if_exists, index=False, method="multi")


class TurbodbcIngestor:
    """Ingest data rapidly using Turbodbc C++ optimized buffers."""
    def __init__(self, conn_str, db, schema, table, truncate=False):
        import turbodbc
        self.conn = turbodbc.connect(connection_string=conn_str, autocommit=True)
        self.db = db
        self.schema = schema
        self.table = table
        self.truncate = truncate
        self.query_prepared = False
        self.insert_query = None

    def setup(self, arrow_table=None):
        print("Note: For Turbodbc ingestion, the destination table must already exist.")
        if self.truncate:
            table_ref = f"{self.schema}.{self.table}" if self.schema else self.table
            cursor = self.conn.cursor()
            try:
                if self.db:
                    try: cursor.execute(f"USE {self.db}")
                    except: pass
                cursor.execute(f"TRUNCATE TABLE {table_ref}")
                print(f"Truncated table: {table_ref}")
            except Exception as e:
                print(f"Failed to truncate table {table_ref}: {e}")

    def ingest(self, chunk_num, arrow_table):
        cursor = self.conn.cursor()
        if self.db:
            try: cursor.execute(f"USE {self.db}")
            except: pass
            
        df = arrow_table.to_pandas()
        
        if not self.query_prepared:
            cols = ", ".join([f"[{c}]" for c in df.columns])
            placeholders = ", ".join(["?"] * len(df.columns))
            table_ref = f"{self.schema}.{self.table}" if self.schema else self.table
            self.insert_query = f"INSERT INTO {table_ref} ({cols}) VALUES ({placeholders})"
            self.query_prepared = True
        
        # Convert columns to Numpy arrays for C++ executemanycolumns
        numpy_cols = [df[col].to_numpy() for col in df.columns]
        cursor.executemanycolumns(self.insert_query, numpy_cols)


class AdbcIngestor:
    """Ingest data via ADBC native streaming (fastest for Postgres)."""
    def __init__(self, conn_str, driver, db, schema, table, create_unlogged=False, truncate=False):
        self.conn = get_adbc_connection(driver, conn_str)
        self.driver = driver
        self.schema = schema
        self.table = table
        self.create_unlogged = create_unlogged
        self.truncate = truncate

    def _get_pg_type(self, arrow_type):
        """Map Arrow types to Postgres types for CREATE TABLE."""
        import pyarrow as pa
        if pa.types.is_int64(arrow_type): return "BIGINT"
        if pa.types.is_int32(arrow_type): return "INTEGER"
        if pa.types.is_floating(arrow_type): return "DOUBLE PRECISION"
        if pa.types.is_string(arrow_type): return "TEXT"
        if pa.types.is_boolean(arrow_type): return "BOOLEAN"
        if pa.types.is_timestamp(arrow_type): return "TIMESTAMP"
        if pa.types.is_date32(arrow_type) or pa.types.is_date64(arrow_type): return "DATE"
        if pa.types.is_decimal(arrow_type): return f"DECIMAL({arrow_type.precision}, {arrow_type.scale})"
        return "TEXT" # Fallback

    def setup(self, arrow_table=None):
        """Pre-create UNLOGGED table if requested and arrow_table is provided."""
        table_ref = f"\"{self.schema}\".\"{self.table}\"" if self.schema else f"\"{self.table}\""
        
        if self.create_unlogged and arrow_table is not None:
            if "postgresql" in self.driver.lower():
                with self.conn.cursor() as cursor:
                    cols = []
                    for field in arrow_table.schema:
                        pg_type = self._get_pg_type(field.type)
                        cols.append(f"\"{field.name}\" {pg_type}")
                    
                    cursor.execute(f"DROP TABLE IF EXISTS {table_ref}")
                    create_sql = f"CREATE UNLOGGED TABLE {table_ref} ({', '.join(cols)})"
                    print(f"Creating UNLOGGED table: {table_ref}")
                    cursor.execute(create_sql)
                    self.conn.commit()
        elif self.truncate:
            with self.conn.cursor() as cursor:
                try:
                    cursor.execute(f"TRUNCATE TABLE {table_ref}")
                    print(f"Truncated table: {table_ref}")
                    self.conn.commit()
                except Exception as e:
                    print(f"Failed to truncate table {table_ref}: {e}")

    def ingest(self, chunk_num, arrow_table):
        if chunk_num == 1 and self.create_unlogged:
             self.setup(arrow_table)

        cursor = self.conn.cursor()
        # If we created the table or truncated it, we must use 'append' mode.
        mode = "append" if (chunk_num > 1 or self.create_unlogged or self.truncate) else "create_append"
        kwargs = {"mode": mode}
        if self.schema:
            kwargs["db_schema_name"] = self.schema
            
        cursor.adbc_ingest(self.table, arrow_table, **kwargs)
        self.conn.commit()


# --- Orchestration ---

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generic High-Performance Database Extract & Load Tool")

    # Source arguments
    parser.add_argument("--source-engine", choices=["sqlalchemy", "turbodbc", "adbc"], required=True, help="Engine used to read data.")
    parser.add_argument("--source-conn-str", required=True, help="Connection string or ODBC DSN for the source database.")
    parser.add_argument("--source-adbc-driver", default="adbc_driver_postgresql", help="ADBC driver module name (if using adbc).")
    parser.add_argument("--source-db-name", help="Source Database name (optional).")
    parser.add_argument("--source-schema-name", help="Source Schema name (optional).")
    parser.add_argument("--source-table-name", help="Source Table name (required if --source-query is not provided).")
    parser.add_argument("--source-query", help="Direct SQL query for extraction. Overrides --source-table-name.")

    # Destination arguments
    parser.add_argument("--dest-engine", choices=["sqlalchemy", "turbodbc", "adbc"], required=True, help="Engine used to write data.")
    parser.add_argument("--dest-conn-str", required=True, help="Connection string or ODBC DSN for the destination database.")
    parser.add_argument("--dest-adbc-driver", default="adbc_driver_postgresql", help="ADBC driver module name (if using adbc).")
    parser.add_argument("--dest-db-name", help="Destination Database name (optional).")
    parser.add_argument("--dest-schema-name", help="Destination Schema name (optional).")
    parser.add_argument("--dest-table-name", required=True, help="Destination Table name.")
    parser.add_argument("--create-unlogged", action="store_true", help="For Postgres destinations, create target table as UNLOGGED.")
    parser.add_argument("--truncate-dest-table", action="store_true", help="Truncate the destination table before ingestion.")

    # Pipeline arguments
    parser.add_argument("--chunk-size", type=int, default=100000, help="Number of rows per memory batch.")
    parser.add_argument("--parallel", action="store_true", help="Use background threads for simultaneous extraction and ingestion.")
    parser.add_argument("--queue-size", type=int, default=3, help="Max number of chunks to buffer in RAM (only used with --parallel).")

    return parser.parse_args()


def run_serial(extractor, ingestor):
    print("Running in SERIAL mode...")
    start = time.perf_counter()
    total_rows = 0
    fetch_time = 0.0
    ingest_time = 0.0
    
    chunk_num = 1
    while True:
        t0 = time.perf_counter()
        try:
            arrow_table = next(extractor)
        except StopIteration:
            break
        fetch_time += time.perf_counter() - t0
        
        row_count = arrow_table.num_rows
        t1 = time.perf_counter()
        ingestor.ingest(chunk_num, arrow_table)
        ingest_time += time.perf_counter() - t1
        
        total_rows += row_count
        print(f"Chunk {chunk_num}: ingested {row_count} rows (total={total_rows})")
        chunk_num += 1
        
    print("-" * 50)
    print(f"Pipeline Complete: {total_rows} rows extracted and loaded.")
    print(f"Fetch Time:   {fetch_time:.2f}s")
    print(f"Ingest Time:  {ingest_time:.2f}s")
    print(f"Total Time:   {time.perf_counter() - start:.2f}s")
    print("-" * 50)


def run_parallel(extractor, ingestor, queue_size):
    print(f"Running in PARALLEL mode (Queue Size: {queue_size})...")
    start = time.perf_counter()
    total_rows = 0
    fetch_time = 0.0
    ingest_time = 0.0
    
    chunk_queue = queue.Queue(maxsize=queue_size)
    error_queue = queue.Queue(maxsize=1)
    
    def producer():
        nonlocal fetch_time
        try:
            chunk_num = 1
            while True:
                t0 = time.perf_counter()
                try:
                    arrow_table = next(extractor)
                except StopIteration:
                    break
                fetch_time += time.perf_counter() - t0
                
                chunk_queue.put((chunk_num, arrow_table))
                chunk_num += 1
        except Exception as e:
            error_queue.put(e)
        finally:
            chunk_queue.put(None) # EOF marker
            
    t = threading.Thread(target=producer, daemon=True, name="ExtractorThread")
    t.start()
    
    while True:
        item = chunk_queue.get()
        if item is None:
            break
            
        chunk_num, arrow_table = item
        row_count = arrow_table.num_rows
        
        t1 = time.perf_counter()
        ingestor.ingest(chunk_num, arrow_table)
        ingest_time += time.perf_counter() - t1
        
        total_rows += row_count
        print(f"Chunk {chunk_num}: ingested {row_count} rows (total={total_rows})")
        
    t.join()
    if not error_queue.empty():
        raise error_queue.get()
        
    print("-" * 50)
    print(f"Pipeline Complete: {total_rows} rows extracted and loaded.")
    print(f"Fetch Time:   {fetch_time:.2f}s")
    print(f"Ingest Time:  {ingest_time:.2f}s")
    print(f"Total Time:   {time.perf_counter() - start:.2f}s")
    print("-" * 50)


def main():
    args = parse_args()

    if not args.source_query and not args.source_table_name:
        print("Error: Either --source-table-name or --source-query must be provided.")
        return

    # Initialize Extractor
    print(f"Initializing Source: {args.source_engine.upper()}")
    extractor = None
    if args.source_engine == "sqlalchemy":
        extractor = extract_sqlalchemy(args.source_conn_str, args.source_db_name, args.source_schema_name, args.source_table_name, args.chunk_size, query=args.source_query)
    elif args.source_engine == "turbodbc":
        extractor = extract_turbodbc(args.source_conn_str, args.source_db_name, args.source_schema_name, args.source_table_name, args.chunk_size, query=args.source_query)
    elif args.source_engine == "adbc":
        extractor = extract_adbc(args.source_conn_str, args.source_adbc_driver, args.source_db_name, args.source_schema_name, args.source_table_name, args.chunk_size, query=args.source_query)

    # Initialize Ingestor
    print(f"Initializing Destination: {args.dest_engine.upper()}")
    ingestor = None
    if args.dest_engine == "sqlalchemy":
        ingestor = SqlAlchemyIngestor(args.dest_conn_str, args.dest_db_name, args.dest_schema_name, args.dest_table_name, truncate=args.truncate_dest_table)
    elif args.dest_engine == "turbodbc":
        ingestor = TurbodbcIngestor(args.dest_conn_str, args.dest_db_name, args.dest_schema_name, args.dest_table_name, truncate=args.truncate_dest_table)
    elif args.dest_engine == "adbc":
        ingestor = AdbcIngestor(args.dest_conn_str, args.dest_adbc_driver, args.dest_db_name, args.dest_schema_name, args.dest_table_name, create_unlogged=args.create_unlogged, truncate=args.truncate_dest_table)

    if not extractor or not ingestor:
        print("Error: Failed to initialize extractor or ingestor.")
        return

    ingestor.setup()
    
    # Execute Pipeline
    if args.parallel:
        run_parallel(extractor, ingestor, args.queue_size)
    else:
        run_serial(extractor, ingestor)


if __name__ == "__main__":
    main()
