# Agent Guidelines for LightningEL

Welcome to the LightningEL repository. This comprehensive guide is designed for AI agents (e.g., Cursor, Copilot, MCP agents) to ensure consistency, safety, and high quality across the codebase.

*(Note: There are no `.cursorrules` or `.github/copilot-instructions.md` present in this repository. This file serves as the definitive rulebook.)*

---

## 1. Project Structure & Architecture

LightningEL is a high-performance "Extract-Load" pipeline engineered to move data rapidly between databases using Apache Arrow as the wire format.
- **`lightning_el.py`**: The core ETL utility supporting `sqlalchemy`, `turbodbc`, and `adbc`. Uses multithreading to overlap network I/O.
- **`transfer_all.py`**: Batch automation script that manages bulk table transfers. Relies on a local `transfer_tracking.db` SQLite database to track state.
- **`ddgs_mcp.py`**: A Model Context Protocol (MCP) server leveraging DuckDuckGo for search tools.

### Architecture Context
- **Extractors:** Must yield `pyarrow.Table` or `pyarrow.RecordBatch`.
- **Ingestors:** Consume Arrow data. They must implement a `setup()` method and an `ingest(chunk_num, arrow_table)` method.
- **Parallelism:** Implemented via `threading.Thread` and `queue.Queue`.

---

## 2. Build, Lint, and Test Commands

### Application Execution
- **Core Application Help:**
  ```bash
  python3 lightning_el.py --help
  ```
- **Example Single Transfer:**
  ```bash
  python3 lightning_el.py \
    --source-engine sqlalchemy --source-conn-str "sqlite:///source.db" --source-table-name "my_table" \
    --dest-engine sqlalchemy --dest-conn-str "sqlite:///dest.db" --dest-table-name "my_table_copy" \
    --parallel
  ```
- **Batch Automation:**
  ```bash
  python3 transfer_all.py
  ```

### Testing (pytest)
We use `pytest` with SQLite mocks for unit testing.
- **Run all tests:**
  ```bash
  python3 -m pytest tests/
  ```
- **Run a specific test file:**
  ```bash
  python3 -m pytest tests/test_lightning_el.py -v
  ```
- **Run a single test method/function (CRITICAL FOR AGENTS):**
  ```bash
  python3 -m pytest tests/test_lightning_el.py::test_specific_function_name -v
  ```
- **Run with standard output (for debugging prints):**
  ```bash
  python3 -m pytest tests/ -s -v
  ```

### Linting & Formatting
If you introduce or modify code, adhere to PEP 8.
- **Linting:** Prefer `ruff` for linting.
  ```bash
  ruff check .
  ```
- **Formatting:** Use `ruff format` or `black`.
  ```bash
  ruff format .
  ```
- **Type Checking:** Use `mypy` to validate type hints.
  ```bash
  mypy lightning_el.py
  ```

---

## 3. Code Style & Conventions

### Imports
- **Grouping:** Group imports strictly: standard library first, third-party packages second, local modules third. Separate groups with a blank line.
- **Format:** Use absolute imports where possible. Avoid `import *`.

### Formatting
- **Indentation:** 4 spaces per indentation level. No tabs.
- **Line Length:** Soft limit of 120 characters to accommodate long SQL strings and DB URIs.
- **Strings:** Use double quotes (`"`) for strings, unless the string itself contains double quotes (then use `'`). Use triple quotes (`"""`) for docstrings.

### Types & Naming
- **Type Hints:** ALWAYS use fully specified type hints for function signatures and class properties (e.g., `def fetch_data(table: str, limit: int) -> pa.Table:`).
- **Variables & Functions:** Use `snake_case`.
- **Classes:** Use `PascalCase`.
- **Constants:** Use `SCREAMING_SNAKE_CASE` (e.g., `DEFAULT_CHUNK_SIZE = 125000`).

### Data Handling
- **Pandas:** Acceptable for intermediate processing or type casting if pyarrow lacks native operations.
- **PyArrow:** The definitive wire format. Prioritize native PyArrow functions to prevent CPU-bound serialization bottlenecks.

### Error Handling
- **Specificity:** Catch specific exceptions (e.g., `sqlalchemy.exc.OperationalError`, `sqlite3.DatabaseError`). DO NOT use a broad `except Exception:` unless for top-level fatal crashes.
- **Logging & Context:** Provide highly descriptive error messages. Include table names, connection strings (masking passwords), and schema specifics.
- **Threading Context:** Child threads must catch exceptions and put them into an `error_queue` to propagate them gracefully back to the main orchestrator thread.

---

## 4. Database & Ingestion Patterns

- **Engine Support:** Must gracefully handle `sqlalchemy`, `turbodbc`, and `adbc`.
- **Optional Schemas:** Database schemas are always optional parameters; handle `None` gracefully.
- **MSSQL Quirks:** When dealing with SQL Server, explicitly cast `uniqueidentifier` (GUID) types to `VARCHAR` during extraction, as they can break Arrow IPC serialization.
- **Turbodbc Chunk Sizes:** The `turbodbc` driver internally halves the requested `--chunk-size` (e.g., requesting 250,000 yields 125,000 row batches). **Optimal chunk size is 125,000 rows.** Do not request more than 250,000, as excessive sizes crash C++ memory allocation.
- **PostgreSQL ADBC:** This is the fastest method for Postgres. Use `create_append` or `append`. For extreme ingestion speeds, create destination tables as `UNLOGGED` to bypass WAL writing, then standardize them post-ingestion if necessary.
- **BigQuery:** BigQuery ADBC driver kwargs are passed as a JSON string via `adbc_driver_bigquery.dbapi`.

---

## 5. Agent Operational Guidelines

When operating autonomously in this codebase, adhere strictly to these rules:

1. **Bash Timeouts:** When using the Bash tool to execute `lightning_el.py` or `transfer_all.py` on large datasets, explicitly set the `timeout` parameter to at least 10 minutes (`600000` ms) to prevent the operation from terminating prematurely.
2. **Read Before Edit:** Never assume the schema or structure of a file. Use the `read` or `grep` tool before modifying any `transfer_tracking.db` or python script.
3. **No Interactive Commands:** All executed scripts must be non-interactive. Supply `--yes` or equivalent flags if prompts exist.
4. **SQLite Interactions:** When checking transfer status or modifying jobs, execute inline `sqlite3` commands directly on `transfer_tracking.db` (e.g. `sqlite3 transfer_tracking.db "SELECT * FROM transfer_status"`). **IMPORTANT**: Upon the successful completion of an entire EL batch session (all tables transferred successfully), the agent must truncate the `transfer_status` table to reset state for future runs.
5. **Documentation:** Keep README.md and AGENTS.md updated if CLI arguments in `lightning_el.py` change.
