import os
import subprocess
import adbc_driver_postgresql.dbapi
import sqlite3

pg_uri = "postgresql://admin_user:$tr0ngPa$sworD@34.132.192.101/dev"
bq_conn_str = '{"adbc.bigquery.sql.project_id": "osaat-classroom-prod", "adbc.bigquery.sql.dataset_id": "offline_classroom"}'

def get_pending_tables():
    conn = sqlite3.connect('transfer_tracking.db')
    c = conn.cursor()
    c.execute('''
        SELECT source_table, dest_table, id 
        FROM transfer_status 
        WHERE transfer_status = 0
        ORDER BY id
    ''')
    tables = c.fetchall()
    conn.close()
    return tables

def update_status(id, status):
    conn = sqlite3.connect('transfer_tracking.db')
    c = conn.cursor()
    c.execute('''
        UPDATE transfer_status 
        SET transfer_status = ?, last_run = CURRENT_TIMESTAMP
        WHERE id = ?
    ''', (status, id))
    conn.commit()
    conn.close()

def truncate_status():
    """Truncates the transfer_status table upon successful completion of all jobs."""
    conn = sqlite3.connect('transfer_tracking.db')
    c = conn.cursor()
    c.execute('DELETE FROM transfer_status')
    # reset the autoincrement counter
    c.execute('DELETE FROM sqlite_sequence WHERE name="transfer_status"')
    conn.commit()
    conn.close()

def main():
    pending_tables = get_pending_tables()
    
    if not pending_tables:
        print("No pending tables to process.")
        return

    print(f"Found {len(pending_tables)} pending tables.")

    # Process each table independently so failures don't stop the rest
    for source_table, dest_table, record_id in pending_tables:
        print(f"\\n{'='*60}")
        print(f"Processing table: {source_table}")
        print(f"{'='*60}")
        
        # 1. Extract to stg1
        print(f"\\n--- Extracting {source_table} from BigQuery to Postgres stg1 ---")
        
        if source_table == "user_master":
            cmd = [
                "python3", "lightning_el.py",
                "--source-engine", "adbc",
                "--source-conn-str", bq_conn_str,
                "--source-adbc-driver", "adbc_driver_bigquery",
                "--source-query", f"SELECT id, cluster, school, ARRAY_TO_STRING(email, ',') AS email FROM {source_table}",
                "--dest-engine", "adbc",
                "--dest-conn-str", pg_uri,
                "--dest-schema-name", "stg1",
                "--dest-table-name", source_table,
                "--create-unlogged",
                "--parallel"
            ]
        else:
            cmd = [
                "python3", "lightning_el.py",
                "--source-engine", "adbc",
                "--source-conn-str", bq_conn_str,
                "--source-adbc-driver", "adbc_driver_bigquery",
                "--source-table-name", source_table,
                "--dest-engine", "adbc",
                "--dest-conn-str", pg_uri,
                "--dest-schema-name", "stg1",
                "--dest-table-name", source_table,
                "--create-unlogged",
                "--parallel"
            ]
            
        res = subprocess.run(cmd)
        if res.returncode != 0:
            print(f"Error extracting table {source_table}. Skipping to next table.")
            continue
            
        # 2. Transfer from stg1 to public, truncating stg1 after
        print(f"\\n--- Transferring {source_table} from stg1 to public.{dest_table} ---")
        transfer_success = False
        
        try:
            with adbc_driver_postgresql.dbapi.connect(pg_uri) as conn:
                with conn.cursor() as cur:
                    # Disable FK checks to allow independent TRUNCATE/INSERT
                    cur.execute("SET session_replication_role = 'replica'")
                    
                    # Get stg1 columns
                    cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_schema = 'stg1' AND table_name = '{source_table}'")
                    stg1_cols = cur.fetch_arrow_table().to_pydict().get('column_name', [])
                    
                    # Get public columns
                    cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '{dest_table}'")
                    pub_cols = cur.fetch_arrow_table().to_pydict().get('column_name', [])
                    
                    if not pub_cols:
                        print(f"Warning: Table public.{dest_table} not found or has no columns!")
                        # Re-enable FK checks
                        cur.execute("SET session_replication_role = 'origin'")
                        conn.commit()
                        continue
                        
                    # Map case-insensitive
                    mapped_stg1 = []
                    mapped_pub = []
                    for pub_col in pub_cols:
                        stg1_match = next((c for c in stg1_cols if c.lower() == pub_col.lower()), None)
                        if stg1_match:
                            mapped_pub.append(f'"{pub_col}"')
                            mapped_stg1.append(f'"{stg1_match}"')
                            
                    if not mapped_pub:
                        print(f"Warning: No matching columns found for {dest_table}")
                        # Re-enable FK checks
                        cur.execute("SET session_replication_role = 'origin'")
                        conn.commit()
                        continue
                        
                    # Truncate public
                    cur.execute(f"TRUNCATE TABLE public.\"{dest_table}\" CASCADE")
                    
                    # Insert
                    pub_cols_str = ", ".join(mapped_pub)
                    stg1_cols_str = ", ".join(mapped_stg1)
                    insert_sql = f"INSERT INTO public.\"{dest_table}\" ({pub_cols_str}) SELECT {stg1_cols_str} FROM stg1.\"{source_table}\""
                    cur.execute(insert_sql)
                    
                    # Get row count public
                    cur.execute(f"SELECT COUNT(*) as count FROM public.\"{dest_table}\"")
                    row_count = cur.fetch_arrow_table().to_pydict()['count'][0]
                    print(f"Inserted {row_count} rows into public.{dest_table}")
                    
                    # Truncate stg1
                    cur.execute(f"TRUNCATE TABLE stg1.\"{source_table}\"")
                    print(f"Truncated stg1.{source_table}")

                    # Re-enable FK checks
                    cur.execute("SET session_replication_role = 'origin'")
                    conn.commit()
                    
                    transfer_success = True
                    
        except Exception as e:
            print(f"Error during transfer of {source_table}: {e}")
            try:
                # Try to clean up and re-enable FK checks on error
                with adbc_driver_postgresql.dbapi.connect(pg_uri) as conn_err:
                    with conn_err.cursor() as cur_err:
                        cur_err.execute("SET session_replication_role = 'origin'")
                        conn_err.commit()
            except:
                pass
                
        # Update tracking database
        if transfer_success:
            update_status(record_id, 1)
            print(f"Successfully processed {source_table}. Status updated to 1.")
        else:
            print(f"Failed to process {source_table}. Status remains 0.")

    print("\\nAll tables processed.")
    
    # Check if any tables failed by seeing if there are pending tables left
    remaining_tables = get_pending_tables()
    if not remaining_tables:
        print("\\nAll transfers completed successfully! Truncating transfer tracking table.")
        truncate_status()
    else:
        print(f"\\nWarning: {len(remaining_tables)} tables failed to transfer. Transfer tracking table will not be truncated.")

if __name__ == "__main__":
    main()
