# LightningEL

**LightningEL** is a high-performance "Extract-Load" tool designed to move data rapidly between databases using **Apache Arrow** as the wire format. By bypassing costly Python object conversions and utilizing multithreaded architecture, it allows network extraction and insertion I/O to overlap, maximizing throughput.

---

## 🚀 Key Features

* **Apache Arrow Backbone:** Uses PyArrow (`pa.Table` and `pa.RecordBatch`) to maintain a highly efficient, CPU-friendly memory format during the entire transfer process.
* **Multithreaded Execution:** Capable of running extraction and ingestion in parallel (`--parallel` flag). Data chunks are piped through a thread-safe queue so ingestion begins while extraction is still ongoing.
* **Broad Engine Support:**
  * **ADBC (Arrow Database Connectivity):** The fastest method for Arrow-native databases (e.g., PostgreSQL, BigQuery).
  * **Turbodbc:** High-performance ODBC driver that natively outputs PyArrow batches.
  * **SQLAlchemy:** Universal fallback for databases lacking native Arrow drivers.
* **Query Extraction:** Extract entire tables or pass custom SQL queries (`--source-query`) to transform/filter data mid-flight.
* **Postgres Optimizations:** Support for bypassing Write-Ahead Logging (WAL) on target tables (`--create-unlogged`) to drastically speed up initial data loading.

---

## 🛠 Usage

To run a single data transfer, use the `lightning_el.py` script.

### Basic Example (SQLAlchemy to SQLAlchemy)

```bash
python3 lightning_el.py \
  --source-engine sqlalchemy \
  --source-conn-str "sqlite:///source.db" \
  --source-table-name "my_table" \
  --dest-engine sqlalchemy \
  --dest-conn-str "sqlite:///dest.db" \
  --dest-table-name "my_table_copy" \
  --parallel
```

### High-Performance Example (BigQuery to PostgreSQL via ADBC)

```bash
python3 lightning_el.py \
  --source-engine adbc \
  --source-conn-str '{"adbc.bigquery.sql.project_id": "my-project", "adbc.bigquery.sql.dataset_id": "my_dataset"}' \
  --source-adbc-driver "adbc_driver_bigquery" \
  --source-table-name "user_master" \
  --dest-engine adbc \
  --dest-conn-str "postgresql://user:pass@host/db" \
  --dest-schema-name "public" \
  --dest-table-name "user_master" \
  --create-unlogged \
  --parallel
```

### Custom Query Extraction

```bash
python3 lightning_el.py \
  --source-engine sqlalchemy \
  --source-conn-str "sqlite:///source.db" \
  --source-query "SELECT id, name FROM users WHERE active = 1" \
  --dest-engine adbc \
  --dest-conn-str "postgresql://user:pass@host/db" \
  --dest-table-name "active_users" \
  --truncate-dest-table \
  --parallel
```

---

## ⚙️ CLI Flags & Arguments

### Source Configuration
* `--source-engine {sqlalchemy,turbodbc,adbc}`: **(Required)** Engine used to read data.
* `--source-conn-str`: **(Required)** Connection string, ODBC DSN, or JSON string (for BigQuery) for the source database.
* `--source-adbc-driver`: ADBC driver module name (e.g., `adbc_driver_bigquery`). Required if using `adbc` source.
* `--source-db-name`: Source database name (optional).
* `--source-schema-name`: Source schema name (optional).
* `--source-table-name`: Source table name. (Required unless `--source-query` is provided).
* `--source-query`: Direct SQL query for extraction. Overrides `--source-table-name`.

### Destination Configuration
* `--dest-engine {sqlalchemy,turbodbc,adbc}`: **(Required)** Engine used to write data.
* `--dest-conn-str`: **(Required)** Connection string or ODBC DSN for the destination database.
* `--dest-adbc-driver`: ADBC driver module name (e.g., `adbc_driver_postgresql`). Required if using `adbc` destination.
* `--dest-db-name`: Destination database name (optional).
* `--dest-schema-name`: Destination schema name (optional).
* `--dest-table-name`: **(Required)** Destination table name.
* `--create-unlogged`: For Postgres destinations, creates the target table as `UNLOGGED` to bypass WAL writing, drastically improving insert speeds.
* `--truncate-dest-table`: Truncates the destination table before ingestion begins.

### Performance Tuning
* `--chunk-size`: Number of rows per memory batch (Default: `250000`). *Note: If using `turbodbc`, optimal chunk size is `125000` rows.*
* `--parallel`: Enables background threads for overlapping extraction and ingestion network I/O. Highly recommended.
* `--queue-size`: Max number of chunks to buffer in RAM when using `--parallel` (Default: `2`). Increase if your extraction is significantly faster than your ingestion and you have abundant RAM.

---

## 🐳 Docker Deployment

For environments where installing C++ dependencies (like `turbodbc` and `pyarrow`) natively is difficult (such as ARM64 Macs), LightningEL includes a highly-optimized Docker container. 

The container uses a `mambaforge` base to pull pre-compiled binaries via conda-forge and is specifically built for `linux/amd64`. It comes pre-installed with the following ODBC drivers out-of-the-box:
* **Microsoft SQL Server** (`ODBC Driver 18 for SQL Server`)
* **SAP HANA** (`HDBODBC`)
* **PostgreSQL ADBC Driver** (for Arrow-native loading)

### 1. Build the Container
```bash
docker build -t lightning_el:latest .
```

### 2. Run via Docker
When connecting to databases running on your host machine from within the container, use the `--add-host host.docker.internal:host-gateway` flag.

**Example: SAP HANA to PostgreSQL**
```bash
docker run --rm --add-host host.docker.internal:host-gateway lightning_el:latest python3 lightning_el.py \
  --source-engine turbodbc \
  --source-conn-str "Driver={HDBODBC};ServerNode=host.docker.internal:39015;UID=SYSTEM;PWD=manager;" \
  --source-schema-name "MY_SCHEMA" \
  --source-table-name "MY_TABLE" \
  --dest-engine adbc \
  --dest-conn-str "postgresql://postgres:password@host.docker.internal:5432/postgres" \
  --dest-adbc-driver "adbc_driver_postgresql" \
  --dest-schema-name "public" \
  --dest-table-name "MY_TABLE" \
  --create-unlogged \
  --parallel
```

---

## 🤖 Batch Automation

The repository includes a batch automation script, `transfer_all.py`, designed to manage bulk table transfers. 

* It relies on a local SQLite database (`transfer_tracking.db`) to track the state of pending and completed jobs.
* Execute it simply by running: `python3 transfer_all.py`
* The tracking table (`transfer_status`) is automatically truncated upon a successful batch completion, readying the system for the next run.
