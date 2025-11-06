from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import event
from sqlalchemy.engine import Engine
import sqlite3
from typing import Generator

# Set PRAGMA for SQLite connections to enable WAL mode for better concurrency
# This listener will apply to all engines created.
@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    if isinstance(dbapi_connection, sqlite3.Connection):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL;")
        #cursor.execute("PRAGMA busy_timeout = 5000;") # Set busy timeout to 5 seconds
        cursor.close()

def create_db_engine(url: str) -> Engine:
    """Helper function to create an async engine with appropriate connect_args."""
    if url.startswith("sqlite+aiosqlite"):
        connect_args = {"timeout": 15}
    elif url.startswith("sqlite"):
        connect_args = {}
    else:
        connect_args = {} # Add other DB-specific args here if needed
    
    return create_async_engine(
        url,
        echo=False,
        future=True,
        connect_args=connect_args
    )
