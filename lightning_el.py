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

def extract_sqlalchemy(conn_str, db, schema, table, chunk_size):
    """Yield chunks of data as Arrow tables using SQLAlchemy and Pandas."""
    engine = create_engine(conn_str)
    table_ref = f"{schema}.{table}" if schema else table
    query = f"SELECT * FROM {table_ref}"
    
    with engine.connect() as conn:
        if db:
            try: conn.execute(f"USE {db}")
            except: pass
            
        for chunk_df in pd.read_sql_query(query, conn, chunksize=chunk_size):
            normalized = normalize_chunk(chunk_df)
            yield pa.Table.from_pandas(normalized, preserve_index=False)


def extract_turbodbc(conn_str, db, schema, table, chunk_size):
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


def extract_adbc(conn_str, driver, db, schema, table, chunk_size):
    """Yield chunks of data natively as Arrow tables using ADBC."""
    with get_adbc_connection(driver, conn_str) as conn:
        with conn.cursor() as cursor:
            # ADBC expects standard table names, schema mapping depends on the driver.
            table_ref = f"{schema}.{table}" if schema else table
            cursor.execute(f"SELECT * FROM {table_ref}")
            reader = cursor.fetch_record_batch()
            for batch in reader:
                # Group single RecordBatch into an Arrow Table
                yield pa.Table.from_batches([batch])

# --- Ingestors ---

class SqlAlchemyIngestor:
    """Ingest data using SQLAlchemy and Pandas `to_sql` method."""
    def __init__(self, conn_str, db, schema, table):
        self.engine = create_engine(conn_str)
        self.schema = schema
        self.table = table

    def setup(self):
        pass # Pandas to_sql handles table creation automatically

    def ingest(self, chunk_num, arrow_table):
        df = arrow_table.to_pandas()
        if_exists = "replace" if chunk_num == 1 else "append"
        with self.engine.begin() as conn:
            df.to_sql(self.table, conn, schema=self.schema, if_exists=if_exists, index=False, method="multi")


class TurbodbcIngestor:
    """Ingest data rapidly using Turbodbc C++ optimized buffers."""
    def __init__(self, conn_str, db, schema, table):
        import turbodbc
        self.conn = turbodbc.connect(connection_string=conn_str, autocommit=True)
        self.db = db
        self.schema = schema
        self.table = table
        self.query_prepared = False
        self.insert_query = None

    def setup(self):
        print("Note: For Turbodbc ingestion, the destination table must already exist.")

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
    def __init__(self, conn_str, driver, db, schema, table):
        self.conn = get_adbc_connection(driver, conn_str)
        self.schema = schema
        self.table = table

    def setup(self):
        pass # ADBC 'create_append' mode handles this automatically

    def ingest(self, chunk_num, arrow_table):
        cursor = self.conn.cursor()
        mode = "create_append" if chunk_num == 1 else "append"
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
    parser.add_argument("--source-table-name", required=True, help="Source Table name.")

    # Destination arguments
    parser.add_argument("--dest-engine", choices=["sqlalchemy", "turbodbc", "adbc"], required=True, help="Engine used to write data.")
    parser.add_argument("--dest-conn-str", required=True, help="Connection string or ODBC DSN for the destination database.")
    parser.add_argument("--dest-adbc-driver", default="adbc_driver_postgresql", help="ADBC driver module name (if using adbc).")
    parser.add_argument("--dest-db-name", help="Destination Database name (optional).")
    parser.add_argument("--dest-schema-name", help="Destination Schema name (optional).")
    parser.add_argument("--dest-table-name", required=True, help="Destination Table name.")

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
    
    # Initialize Extractor
    print(f"Initializing Source: {args.source_engine.upper()}")
    if args.source_engine == "sqlalchemy":
        extractor = extract_sqlalchemy(args.source_conn_str, args.source_db_name, args.source_schema_name, args.source_table_name, args.chunk_size)
    elif args.source_engine == "turbodbc":
        extractor = extract_turbodbc(args.source_conn_str, args.source_db_name, args.source_schema_name, args.source_table_name, args.chunk_size)
    elif args.source_engine == "adbc":
        extractor = extract_adbc(args.source_conn_str, args.source_adbc_driver, args.source_db_name, args.source_schema_name, args.source_table_name, args.chunk_size)

    # Initialize Ingestor
    print(f"Initializing Destination: {args.dest_engine.upper()}")
    if args.dest_engine == "sqlalchemy":
        ingestor = SqlAlchemyIngestor(args.dest_conn_str, args.dest_db_name, args.dest_schema_name, args.dest_table_name)
    elif args.dest_engine == "turbodbc":
        ingestor = TurbodbcIngestor(args.dest_conn_str, args.dest_db_name, args.dest_schema_name, args.dest_table_name)
    elif args.dest_engine == "adbc":
        ingestor = AdbcIngestor(args.dest_conn_str, args.dest_adbc_driver, args.dest_db_name, args.dest_schema_name, args.dest_table_name)

    ingestor.setup()
    
    # Execute Pipeline
    if args.parallel:
        run_parallel(extractor, ingestor, args.queue_size)
    else:
        run_serial(extractor, ingestor)


if __name__ == "__main__":
    main()
