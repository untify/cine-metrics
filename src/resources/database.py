import asyncio
import time
from contextlib import contextmanager

import duckdb
from dagster import ConfigurableResource


class MotherDuckResource(ConfigurableResource):
    connection_string: str
    token: str
    max_retries: int = 3
    retry_delay: int = 1

    def get_connection(self):
        for attempt in range(self.max_retries):
            try:
                return duckdb.connect(self.connection_string)
            except Exception:
                if attempt == self.max_retries - 1:
                    raise
                time.sleep(self.retry_delay)

    @contextmanager
    def connection(self):
        conn = self.get_connection()
        try:
            yield conn
        finally:
            conn.close()

    def query(self, sql, params=None):
        with self.connection() as conn:
            return conn.execute(sql, params).fetchall()

    def execute(self, sql, params=None):
        with self.connection() as conn:
            conn.execute(sql, params)

    async def run_async(self, func, *args, **kwargs):
        return await asyncio.to_thread(func, *args, **kwargs)
