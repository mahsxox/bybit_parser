import logging
import re
from typing import Iterable, List, Tuple

import asyncpg

log = logging.getLogger(__name__)

_SAFE_NAME = re.compile(r"^[a-z0-9_]+$")


def table_name(symbol: str, interval: str) -> str:
    s = symbol.strip().lower().replace("/", "_").replace("-", "_")
    i = str(interval).strip().lower()
    name = f"klines_{s}_{i}"
    if not _SAFE_NAME.match(name):
        raise ValueError(f"Недопустимое имя таблицы: {name!r}")
    if len(name) > 63:
        raise ValueError(f"Имя таблицы длиннее 63 символов: {name!r}")
    return name


def _create_table_sql(name: str) -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {name} (
        category   VARCHAR(16)      NOT NULL,
        timestamp  BIGINT           NOT NULL,
        open       DOUBLE PRECISION NOT NULL,
        high       DOUBLE PRECISION NOT NULL,
        low        DOUBLE PRECISION NOT NULL,
        close      DOUBLE PRECISION NOT NULL,
        volume     DOUBLE PRECISION NOT NULL,
        turnover   DOUBLE PRECISION NOT NULL,
        PRIMARY KEY (category, timestamp)
    );
    """


def _upsert_sql(name: str, mode: str) -> str:
    if mode == "update":
        conflict = (
            "DO UPDATE SET "
            "open = EXCLUDED.open, "
            "high = EXCLUDED.high, "
            "low = EXCLUDED.low, "
            "close = EXCLUDED.close, "
            "volume = EXCLUDED.volume, "
            "turnover = EXCLUDED.turnover"
        )
    else:
        conflict = "DO NOTHING"
    return f"""
    INSERT INTO {name}
        (category, timestamp, open, high, low, close, volume, turnover)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    ON CONFLICT (category, timestamp) {conflict};
    """


class Database:
    def __init__(self, host: str, port: int, name: str, user: str, password: str):
        self.dsn = f"postgresql://{user}:{password}@{host}:{port}/{name}"
        self._pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        self._pool = await asyncpg.create_pool(
            self.dsn, min_size=1, max_size=10, ssl=False
        )
        log.info("PostgreSQL подключён")

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()

    async def table_exists(self, name: str) -> bool:
        async with self._pool.acquire() as conn:
            return await conn.fetchval("SELECT to_regclass($1)", name) is not None

    async def ensure_table(self, symbol: str, interval: str) -> str:
        name = table_name(symbol, interval)
        async with self._pool.acquire() as conn:
            await conn.execute(_create_table_sql(name))
        return name

    async def get_last_timestamp(
        self, symbol: str, interval: str, category: str
    ) -> int | None:
        name = table_name(symbol, interval)
        if not await self.table_exists(name):
            return None
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT MAX(timestamp) AS ts FROM {name} WHERE category=$1",
                category,
            )
        return row["ts"] if row and row["ts"] is not None else None

    async def get_first_timestamp(
        self, symbol: str, interval: str, category: str
    ) -> int | None:
        name = table_name(symbol, interval)
        if not await self.table_exists(name):
            return None
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT MIN(timestamp) AS ts FROM {name} WHERE category=$1",
                category,
            )
        return row["ts"] if row and row["ts"] is not None else None

    async def count_rows(self, symbol: str, interval: str, category: str) -> int:
        name = table_name(symbol, interval)
        if not await self.table_exists(name):
            return 0
        async with self._pool.acquire() as conn:
            return await conn.fetchval(
                f"SELECT COUNT(*) FROM {name} WHERE category=$1", category
            )

    async def upsert_klines(
        self,
        symbol: str,
        interval: str,
        category: str,
        candles: Iterable[Tuple],
        mode: str = "skip",
    ) -> int:
        name = table_name(symbol, interval)
        rows = [
            (category, c[0], c[1], c[2], c[3], c[4], c[5], c[6]) for c in candles
        ]
        if not rows:
            return 0
        sql = _upsert_sql(name, mode)
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.executemany(sql, rows)
        return len(rows)

    async def list_klines_tables(self) -> List[str]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT tablename
                FROM pg_tables
                WHERE schemaname='public' AND tablename LIKE 'klines\\_%'
                ORDER BY tablename
                """
            )
        return [r["tablename"] for r in rows]