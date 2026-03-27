"""Database connection management for Stats Bluenoser.

Provides a connection pool, transaction context manager, and query helpers.
Reads DATABASE_URL from environment. Uses psycopg2 with standard
PostgreSQL connection strings — no vendor-specific dependencies.
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from typing import Any, Iterator

import psycopg2
import psycopg2.extras
import psycopg2.pool

logger = logging.getLogger(__name__)

# Module-level connection pool (initialized on first use)
_pool: psycopg2.pool.ThreadedConnectionPool | None = None


def _get_database_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError(
            "DATABASE_URL not set. Copy .env.example to .env and configure it."
        )
    return url


def get_pool() -> psycopg2.pool.ThreadedConnectionPool:
    """Get or create the module-level connection pool."""
    global _pool
    if _pool is None or _pool.closed:
        url = _get_database_url()
        _pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=5,
            dsn=url,
        )
        logger.info("Database connection pool created")
    return _pool


def close_pool():
    """Close the connection pool. Call on shutdown."""
    global _pool
    if _pool is not None and not _pool.closed:
        _pool.closeall()
        _pool = None
        logger.info("Database connection pool closed")


@contextmanager
def get_connection():
    """Get a connection from the pool. Auto-returns on exit."""
    pool = get_pool()
    conn = pool.getconn()
    try:
        yield conn
    finally:
        pool.putconn(conn)


@contextmanager
def transaction():
    """Context manager for a database transaction.

    Commits on successful exit, rolls back on exception.

    Usage:
        with transaction() as cur:
            cur.execute("INSERT INTO ...", (value,))
            cur.execute("UPDATE ...", (value,))
        # auto-committed here
    """
    with get_connection() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            yield cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()


def execute(sql: str, params: tuple | dict | None = None) -> list[dict[str, Any]]:
    """Execute a query and return all rows as dicts.

    For SELECT queries. Uses parameterized queries to prevent SQL injection.

    Args:
        sql: SQL query with %s or %(name)s placeholders.
        params: Query parameters (tuple for %s, dict for %(name)s).

    Returns:
        List of dicts (column_name -> value).
    """
    with transaction() as cur:
        cur.execute(sql, params)
        if cur.description:
            return cur.fetchall()
        return []


def execute_one(sql: str, params: tuple | dict | None = None) -> dict[str, Any] | None:
    """Execute a query and return the first row, or None."""
    with transaction() as cur:
        cur.execute(sql, params)
        if cur.description:
            return cur.fetchone()
        return None


def execute_many(sql: str, params_list: list[tuple | dict]) -> int:
    """Execute a query for each parameter set. Returns total rowcount.

    Uses executemany for batch operations (inserts, updates).

    Args:
        sql: SQL with placeholders.
        params_list: List of parameter tuples/dicts.

    Returns:
        Total number of affected rows.
    """
    with transaction() as cur:
        cur.executemany(sql, params_list)
        return cur.rowcount


def execute_values(
    sql: str,
    values: list[tuple],
    template: str | None = None,
    page_size: int = 1000,
) -> list[dict[str, Any]]:
    """Batch insert using psycopg2.extras.execute_values for performance.

    Significantly faster than executemany for bulk inserts.

    Args:
        sql: SQL with a VALUES placeholder, e.g.:
             "INSERT INTO t (a, b) VALUES %s RETURNING id"
        values: List of tuples, one per row.
        template: Optional value template, e.g. "(%s, %s)".
        page_size: Rows per batch.

    Returns:
        List of dicts if query has RETURNING clause, else [].
    """
    with transaction() as cur:
        psycopg2.extras.execute_values(
            cur, sql, values, template=template, page_size=page_size
        )
        if cur.description:
            return cur.fetchall()
        return []
