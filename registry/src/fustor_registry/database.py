from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import event
from sqlalchemy.engine import Engine
import sqlite3
from typing import Generator
from .config import register_config # CORRECTED IMPORT

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
    if url.startswith("sqlite"):
        connect_args = {"timeout": 15}
    else:
        connect_args = {} # Add other DB-specific args here if needed
    
    return create_async_engine(
        url,
        echo=False,
        future=True,
        connect_args=connect_args
    )

# --- Create engine for State database only ---
register_engine = create_db_engine(register_config.FUSTOR_REGISTER_DB_URL) # CORRECTED USAGE

# --- Create a single Session factory for State database only ---
AsyncSessionLocal: async_sessionmaker[AsyncSession] = async_sessionmaker(
    bind=register_engine,
    class_=AsyncSession,
    expire_on_commit=False
)

async def get_db() -> Generator[AsyncSession, None, None]:
    """Dependency injector for a database session."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
