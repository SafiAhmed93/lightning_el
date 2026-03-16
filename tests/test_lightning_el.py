import sqlite3
import pandas as pd
import pyarrow as pa
import pytest
from sqlalchemy import create_engine
import sys
import os

# Add root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from lightning_el import extract_sqlalchemy, SqlAlchemyIngestor

@pytest.fixture
def source_db(tmp_path):
    db_path = tmp_path / "source.db"
    conn = sqlite3.connect(str(db_path))
    df = pd.DataFrame({"id": [1, 2, 3], "name": ["alice", "bob", "charlie"]})
    df.to_sql("users", conn, index=False)
    conn.close()
    return f"sqlite:///{db_path}"

@pytest.fixture
def dest_db(tmp_path):
    db_path = tmp_path / "dest.db"
    return f"sqlite:///{db_path}"

def test_extract_sqlalchemy(source_db):
    # extractor is a generator
    extractor = extract_sqlalchemy(source_db, None, None, "users", 10)
    table = next(extractor)
    assert table.num_rows == 3
    assert table.column_names == ["id", "name"]

def test_extract_sqlalchemy_query(source_db):
    query = "SELECT id FROM users WHERE id > 1"
    extractor = extract_sqlalchemy(source_db, None, None, None, 10, query=query)
    table = next(extractor)
    assert table.num_rows == 2
    assert table.column_names == ["id"]

def test_sqlalchemy_ingestor(dest_db):
    table = pa.Table.from_pandas(pd.DataFrame({"a": [1], "b": ["x"]}))
    ingestor = SqlAlchemyIngestor(dest_db, None, None, "test_table")
    ingestor.setup()
    ingestor.ingest(1, table)
    
    # Verify
    engine = create_engine(dest_db)
    with engine.connect() as conn:
        df = pd.read_sql_query("SELECT * FROM test_table", conn)
        assert len(df) == 1
        assert list(df.columns) == ["a", "b"]
