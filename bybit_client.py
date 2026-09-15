import asyncio
import logging
from typing import List, Tuple

import aiohttp

from config import INTERVAL_MS

log = logging.getLogger(__name__)

KLINE_LIMIT = 1000


class BybitClient:

    def __init__(
        self,
        base_url: str,
        concurrency: int = 8,
        max_retries: int = 4,
        retry_delay: float = 1.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.semaphore = asyncio.Semaphore(concurrency)
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self._session: aiohttp.ClientSession | None = None

    async def __aenter__(self) -> "BybitClient":
        connector = aiohttp.TCPConnector(limit=100, ttl_dns_cache=300)
        self._session = aiohttp.ClientSession(
            connector=connector,
            timeout=aiohttp.ClientTimeout(total=30),
        )
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._session:
            await self._session.close()

    async def _request(self, url: str, params: dict) -> list:
        async with self.semaphore:
            last_exc: Exception | None = None
            for attempt in range(1, self.max_retries + 1):
                try:
                    async with self._session.get(url, params=params) as resp:
                        if resp.status == 429:
                            wait = self.retry_delay * (2 ** (attempt - 1))
                            log.warning("429 Too Many Requests, sleep %.1fs", wait)
                            await asyncio.sleep(wait)
                            continue
                        resp.raise_for_status()
                        payload = await resp.json()
                        if payload.get("retCode") != 0:
                            raise RuntimeError(
                                f"Bybit retCode={payload.get('retCode')} "
                                f"msg={payload.get('retMsg')}"
                            )
                        return payload["result"]["list"]
                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    last_exc = e
                    wait = self.retry_delay * (2 ** (attempt - 1))
                    log.warning(
                        "Запрос упал (attempt %d/%d): %s. Спим %.1fs",
                        attempt, self.max_retries, e, wait,
                    )
                    await asyncio.sleep(wait)
            raise RuntimeError(f"Все попытки исчерпаны: {last_exc}")

    async def _fetch_chunk(
        self,
        symbol: str,
        interval: str,
        category: str,
        start_ms: int,
        end_ms: int,
    ) -> list:
        url = f"{self.base_url}/v5/market/kline"
        params = {
            "category": category,
            "symbol": symbol,
            "interval": interval,
            "start": start_ms,
            "end": end_ms,
            "limit": KLINE_LIMIT,
        }
        return await self._request(url, params)

    async def fetch_klines(
        self,
        symbol: str,
        interval: str,
        start_ms: int,
        end_ms: int,
        category: str = "spot",
    ) -> List[Tuple]:
        interval_ms = INTERVAL_MS[interval]
        chunks: List[Tuple[int, int]] = []
        cur = start_ms
        while cur <= end_ms:
            chunk_end = min(cur + (KLINE_LIMIT - 1) * interval_ms, end_ms)
            chunks.append((cur, chunk_end))
            cur = chunk_end + interval_ms

        expected = int((end_ms - start_ms) // interval_ms) + 1
        log.info(
            "[%s %s] %d чанков, ожидается ~%d свечей",
            symbol, interval, len(chunks), expected,
        )

        tasks = [
            self._fetch_chunk(symbol, interval, category, s, e)
            for s, e in chunks
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        dedup: dict[int, Tuple] = {}
        for res in results:
            if isinstance(res, Exception):
                log.error("[%s] чанк упал: %s", symbol, res)
                continue
            for c in res:
                ts = int(c[0])
                dedup[ts] = (
                    ts,
                    float(c[1]), float(c[2]), float(c[3]), float(c[4]),
                    float(c[5]), float(c[6]),
                )

        candles = sorted(dedup.values(), key=lambda x: x[0])
        log.info("[%s %s] получено %d уникальных свечей", symbol, interval, len(candles))

        if len(candles) < expected * 0.98:
            log.warning(
                "[%s %s] похоже на пропуск: получено %d из ~%d",
                symbol, interval, len(candles), expected,
            )
        return candles