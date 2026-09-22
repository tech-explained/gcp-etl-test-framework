"""ValidationContext: database + GCS access abstraction shared by all validators.

Validators never touch Airflow hooks or driver-specific APIs directly; they go
through this context so the same test-case code runs on Cloud Composer
(Postgres via psycopg2) and locally (sqlite) for fast iteration.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence


class ValidationContext:
    def __init__(
        self,
        conn,
        dialect: str = "postgres",
        gcs_client=None,
        pipeline: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ):
        self.conn = conn
        self.dialect = dialect
        self.gcs_client = gcs_client
        self.pipeline = pipeline or {}
        self.params = params or {}

    # ------------------------------------------------------------------ SQL
    def _placeholders(self, sql: str) -> str:
        # sqlite uses "?", postgres (psycopg2) uses "%s"
        return sql.replace("%s", "?") if self.dialect == "sqlite" else sql

    def fetchall(self, sql: str, args: Optional[Sequence] = None) -> List[Dict[str, Any]]:
        cur = self.conn.cursor()
        try:
            cur.execute(self._placeholders(sql), list(args or []))
            cols = [d[0] for d in cur.description] if cur.description else []
            return [dict(zip(cols, row)) for row in cur.fetchall()]
        finally:
            cur.close()

    def fetchone(self, sql: str, args: Optional[Sequence] = None) -> Optional[Dict[str, Any]]:
        rows = self.fetchall(sql, args)
        return rows[0] if rows else None

    def execute(self, sql: str, args: Optional[Sequence] = None) -> None:
        cur = self.conn.cursor()
        try:
            cur.execute(self._placeholders(sql), list(args or []))
        finally:
            cur.close()

    def commit(self) -> None:
        try:
            self.conn.commit()
        except Exception:
            pass

    # -------------------------------------------------------------- naming
    def table_ref(self, schema: str, table: str) -> str:
        if self.dialect == "sqlite":
            return f'"{schema}_{table}"'
        return f'"{schema}"."{table}"'

    def columns(self, schema: str, table: str) -> List[Dict[str, str]]:
        """Return [{'name': ..., 'type': ...}] for a table, dialect-aware."""
        if self.dialect == "sqlite":
            rows = self.fetchall(f"PRAGMA table_info({self.table_ref(schema, table)})")
            return [{"name": r["name"], "type": (r["type"] or "").upper()} for r in rows]
        rows = self.fetchall(
            """
            SELECT column_name AS name, data_type AS type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            [schema, table],
        )
        return [{"name": r["name"], "type": (r["type"] or "").upper()} for r in rows]

    def count(self, schema: str, table: str, where: str = "", args: Optional[Sequence] = None) -> int:
        sql = f"SELECT COUNT(*) AS c FROM {self.table_ref(schema, table)}"
        if where:
            sql += f" WHERE {where}"
        row = self.fetchone(sql, args)
        return int(row["c"]) if row else 0
