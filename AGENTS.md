# Agent Guidelines for LightningEL

Welcome to the LightningEL repository. This guide is designed for agentic coding tools to ensure consistency and quality across the codebase.

## 1. Build, Lint, and Test Commands

Currently, this project is a standalone Python utility. There is no formal test suite or linting configuration (like `pytest` or `ruff`) yet.

- **Running the Application:**
  ```bash
  python3 lightning_el.py --help
  ```
- **Execution Example:**
  ```bash
  python3 lightning_el.py \
    --source-engine sqlalchemy --source-conn-str "sqlite:///source.db" --source-table-name "my_table" \
    --dest-engine sqlalchemy --dest-conn-str "sqlite:///dest.db" --dest-table-name "my_table_copy" \
    --parallel
  ```
- **Linting:** Use standard PEP 8 guidelines. If you introduce a linter, prefer `ruff`.
- **Testing:** We use `pytest` for unit testing. Tests are located in the `tests/` directory and use SQLite for mocking database connections. Run tests with `python3 -m pytest tests/`.

## 2. Code Style & Conventions

### Imports
- Group imports: standard library, third-party packages, and then local modules.
- Use absolute imports where possible.
- Prefer specific imports over `import *`.

### Formatting
- Use 4 spaces for indentation.
- Limit line length to 120 characters where reasonable.
- Use double quotes for strings unless the string contains double quotes.

### Types & Naming
- **Type Hints:** ALWAYS use type hints for function signatures (e.g., `def func(a: int) -> str:`).
- **Naming:**
  - Functions and variables: `snake_case`
  - Classes: `PascalCase`
  - Constants: `SCREAMING_SNAKE_CASE`
- **Data Handling:** Use `pandas` for intermediate processing and `pyarrow` for high-performance data transfer.

### Error Handling
- Use specific exception handling rather than broad `except Exception:`.
- Provide meaningful error messages that help debug connection or schema issues.
- In `threading` contexts, use an `error_queue` to propagate exceptions from child threads to the main thread.

### Database Patterns
- Support multiple engines: `sqlalchemy`, `turbodbc`, and `adbc`.
- Always handle schema and database names as optional parameters.
- When working with SQL Server (MSSQL), be mindful of `uniqueidentifier` (GUID) types which may need explicit casting to `VARCHAR` for Arrow compatibility.
- **Turbodbc Setup (macOS):** Requires `unixodbc`, `apache-arrow`, and `boost`. Install via Homebrew and use `--no-build-isolation` with `pip` or `uv`.
- **Performance:** Turbodbc + Arrow can reduce extraction time by ~80% compared to SQLAlchemy/Pandas by bypassing Python object conversion.
- **ADBC Ingestion:** Fastest for Postgres. Use `create_append` or `append` modes. For maximum speed, pre-create destination tables as `UNLOGGED` to bypass WAL.
- **Turbodbc Chunk Sizes:** Note that `turbodbc` internally halves the requested `--chunk-size` (e.g., requesting 250,000 yields 125,000 row batches). The optimal chunk size appears to be **125,000 rows** (requesting 250,000). Excessively large chunks (like 250,000 rows, requesting 500,000) heavily degrade extraction performance due to C++ memory allocation and Arrow conversion overhead.

## 3. Architecture Context

LightningEL is a high-performance "Extract-Load" tool designed to move data between databases using Apache Arrow as the wire format.

- **Extractors:** Yield `pyarrow.Table` or `pyarrow.RecordBatch`.
- **Ingestors:** Consume Arrow data. They should implement a `setup()` and `ingest(chunk_num, arrow_table)` method.
- **Parallelism:** Implemented using `threading.Thread` and `queue.Queue` to overlap network I/O for extraction and ingestion.

## 4. Existing Rules
- No `.cursorrules` or `.github/copilot-instructions.md` found in this repository.
- Follow the patterns established in `lightning_el.py`.
- **Bash Tool Timeouts:** When using the Bash tool to execute `lightning_el.py` on large datasets, explicitly set the `timeout` parameter to 10 minutes (600,000 milliseconds) to prevent the command from timing out prematurely.
