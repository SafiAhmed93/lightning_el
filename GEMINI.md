# Project Overview

This is a generic, high-performance Python database extraction and loading (ETL) tool. It provides a modular pipeline for transferring data between different database systems by extracting data into Apache Arrow batches and loading them into a destination database.

The project supports mixing and matching different engines for extraction and ingestion to optimize performance based on the specific source and destination systems:
*   **SQLAlchemy / Pandas:** A standard, highly compatible path that reads data as Pandas DataFrames and converts them to Arrow batches.
*   **Turbodbc:** A high-performance path utilizing C++ optimized buffers to read and write Arrow tables natively.
*   **ADBC (Arrow Database Connectivity):** The fastest path for supported databases (like PostgreSQL), providing native Arrow stream ingestion and extraction.

**Key Technologies:**
*   **Python:** Core programming language.
*   **Apache Arrow (PyArrow):** In-memory columnar data representation used as the universal intermediate format.
*   **ADBC / Turbodbc / SQLAlchemy:** Database drivers and connectivity engines.
*   **Pandas:** Used for data normalization and compatibility layers when native Arrow streams are not available.

# Building and Running

This is a standalone Python script, so there is no formal build step.

### Prerequisites

You must install the necessary Python dependencies. Depending on the engines and drivers you intend to use, install the relevant packages:

```bash
# TODO: Verify or create a dependency management file (requirements.txt)
pip install pandas pyarrow sqlalchemy
pip install turbodbc  # If using the turbodbc engine
pip install adbc-driver-postgresql  # Example ADBC driver
```

### Execution

Run the tool using the Python interpreter. The script requires specifying both the source and destination configurations.

```bash
python lightning_el.py [OPTIONS]
```

**Key Command-Line Arguments:**

*   **Source Configuration:**
    *   `--source-engine`: Engine to read data (`sqlalchemy`, `turbodbc`, `adbc`).
    *   `--source-conn-str`: Connection string or ODBC DSN.
    *   `--source-table-name`: Table to extract from.
    *   `--source-adbc-driver`, `--source-db-name`, `--source-schema-name`: Optional source specifics.
*   **Destination Configuration:**
    *   `--dest-engine`: Engine to write data (`sqlalchemy`, `turbodbc`, `adbc`).
    *   `--dest-conn-str`: Connection string or ODBC DSN.
    *   `--dest-table-name`: Target table name.
    *   `--dest-adbc-driver`, `--dest-db-name`, `--dest-schema-name`: Optional destination specifics.
*   **Pipeline Configuration:**
    *   `--chunk-size`: Number of rows per memory batch (default: `100000`).
    *   `--parallel`: Flag to use background threads for simultaneous extraction and ingestion.
    *   `--queue-size`: Max number of chunks to buffer in RAM when using `--parallel` (default: `3`).

**Example: SQL Server (Turbodbc) to PostgreSQL (ADBC) in Parallel**

```bash
python lightning_el.py \
    --source-engine turbodbc \
    --source-conn-str "DRIVER={ODBC Driver 18 for SQL Server};SERVER=tcp:my_server,1433;DATABASE=my_db;UID=user;PWD=pass;Encrypt=yes;TrustServerCertificate=yes;" \
    --source-schema-name dbo \
    --source-table-name MyLargeTable \
    --dest-engine adbc \
    --dest-adbc-driver adbc_driver_postgresql \
    --dest-conn-str "postgresql://user:pass@localhost:5432/my_dest_db" \
    --dest-table-name MyLargeTable_Loaded \
    --parallel
```

# Development Conventions

Based on the `lightning_el.py` script, the project adheres to the following conventions:

*   **Modular Architecture:** The script separates the data flow into interchangeable **Extractors** (generator functions yielding PyArrow tables) and **Ingestors** (classes implementing a standardized `.ingest(chunk_num, arrow_table)` method).
*   **Type Hinting & Normalization:** Employs type hint annotations and provides data normalization routines (e.g., casting `decimal.Decimal` to `float64` and `uuid.UUID` to `string`) to prevent Arrow type conversion errors during pipeline execution.
*   **Concurrency:** Leverages the standard library's `threading` and `queue` modules to implement a robust producer-consumer pattern for the `--parallel` execution mode.
*   **Dynamic Loading:** ADBC drivers are imported dynamically via `importlib` based on user input to avoid hard dependencies on drivers that may not be installed or needed by the user.