import asyncio
import logging
import os
from datetime import datetime, timedelta

import pandas as pd

from bybit_client import BybitClient
from config import Config, INTERVAL_MS
from database import Database, table_name

log = logging.getLogger(__name__)


class Collector:
    def __init__(self, cfg: Config, client: BybitClient, db: Database):
        self.cfg = cfg
        self.client = client
        self.db = db

    async def collect_one(self, symbol: str, interval: str) -> int:
        symbol = symbol.upper()
        category = self.cfg.category
        interval_ms = INTERVAL_MS[interval]

        tname = await self.db.ensure_table(symbol, interval)

        end_ms = int(datetime.now().timestamp() * 1000)
        target_start = int(
            (datetime.now() - timedelta(days=self.cfg.days)).timestamp() * 1000
        )

        last_ts = await self.db.get_last_timestamp(symbol, interval, category)
        first_ts = await self.db.get_first_timestamp(symbol, interval, category)

        if last_ts is None:
            ranges = [(target_start, end_ms)]
            mode = "полная"
        else:
            ranges = []
            if last_ts + interval_ms < end_ms:
                ranges.append((last_ts + interval_ms, end_ms))
            if first_ts is not None and first_ts > target_start:
                ranges.append((target_start, first_ts - interval_ms))
            mode = "инкремент"

        if not ranges:
            log.info(
                "[%s %s] актуален (таблица %s, %d строк)",
                symbol, interval, tname,
                await self.db.count_rows(symbol, interval, category),
            )
            return 0

        log.info(
            "[%s %s] %s → таблица %s, диапазонов %d",
            symbol, interval, mode, tname, len(ranges),
        )

        total = 0
        for start_ms, stop_ms in ranges:
            log.info(
                "[%s %s] %s → %s",
                symbol, interval,
                datetime.fromtimestamp(start_ms / 1000).strftime("%Y-%m-%d %H:%M"),
                datetime.fromtimestamp(stop_ms / 1000).strftime("%Y-%m-%d %H:%M"),
            )
            candles = await self.client.fetch_klines(
                symbol, interval, start_ms, stop_ms, category
            )
            n = await self.db.upsert_klines(
                symbol, interval, category, candles, mode="skip"
            )
            total += n

        if self.cfg.export_csv:
            await self._export_csv(symbol, interval, category)

        log.info(
            "[%s %s] +%d новых (всего %d в %s)",
            symbol, interval, total,
            await self.db.count_rows(symbol, interval, category),
            tname,
        )
        return total

    async def collect_all(self) -> None:
        pairs = [(s, i) for s in self.cfg.symbols for i in self.cfg.intervals]
        tasks = [self.collect_one(s, i) for s, i in pairs]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        total = 0
        for (sym, iv), res in zip(pairs, results):
            if isinstance(res, Exception):
                log.error("[%s %s] ОШИБКА: %s", sym, iv, res)
            else:
                total += res
        log.info("Итерация завершена, добавлено новых свечей: %d", total)

    async def run(self, watch: bool = False, poll_sec: int | None = None) -> None:
        if not watch:
            t0 = asyncio.get_event_loop().time()
            await self.collect_all()
            log.info("Готово за %.1f сек", asyncio.get_event_loop().time() - t0)
            return

        shortest = min(INTERVAL_MS[i] for i in self.cfg.intervals) // 1000
        if poll_sec is None:
            poll_sec = max(30, min(600, shortest // 2))

        log.info(
            "Watch: symbols=%s intervals=%s опрос каждые %d сек. Ctrl+C — выход.",
            self.cfg.symbols, self.cfg.intervals, poll_sec,
        )
        while True:
            try:
                await self.collect_all()
            except Exception as e:
                log.exception("Ошибка цикла: %s", e)
            try:
                await asyncio.sleep(poll_sec)
            except asyncio.CancelledError:
                return

    async def _export_csv(self, symbol: str, interval: str, category: str) -> None:
        os.makedirs(self.cfg.csv_dir, exist_ok=True)
        tname = table_name(symbol, interval)
        async with self.db._pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT timestamp, open, high, low, close, volume, turnover "
                f"FROM {tname} WHERE category=$1 ORDER BY timestamp",
                category,
            )
        if not rows:
            return
        df = pd.DataFrame(
            [tuple(r) for r in rows],
            columns=["timestamp", "open", "high", "low", "close", "volume", "turnover"],
        )
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        path = os.path.join(
            self.cfg.csv_dir, f"{symbol}_{interval}_{category}.csv"
        )
        df.to_csv(path, index=False)
        log.info("[%s %s] CSV: %s (%d строк)", symbol, interval, path, len(df))