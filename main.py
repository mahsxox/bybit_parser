import argparse
import asyncio
import logging
import sys
from typing import List

from bybit_client import BybitClient
from collector import Collector
from config import Config, INTERVAL_MS
from database import Database

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
)
log = logging.getLogger("main")


def _dedup(items: List[str]) -> List[str]:
    seen = set()
    return [x for x in items if not (x in seen or seen.add(x))]


def ask_pairs() -> List[str]:
    print("\nВведите торговые пары через пробел или запятую.")
    print("Пример: BTCUSDT ETHUSDT SOLUSDT")
    while True:
        raw = input("Пары: ").strip()
        if not raw:
            print("Нужно ввести хотя бы одну пару.")
            continue
        syms = _dedup([p.upper() for p in raw.replace(",", " ").split() if p])
        if syms:
            return syms


def ask_intervals() -> List[str]:
    valid = ", ".join(sorted(INTERVAL_MS, key=lambda k: (len(k), k)))
    print("\nВведите таймфреймы через пробел или запятую.")
    print(f"Допустимо: {valid}")
    print("Пример: 60 240 D")
    while True:
        raw = input("Таймфреймы: ").strip()
        if not raw:
            print("Нужен хотя бы один таймфрейм.")
            continue
        ivs = [
            i.upper() if i.isalpha() else i
            for i in raw.replace(",", " ").split()
        ]
        bad = [i for i in ivs if i not in INTERVAL_MS]
        if bad:
            print(f"Неизвестные таймфреймы: {bad}. Попробуйте снова.")
            continue
        return _dedup(ivs)


def ask_days(default: int = 730) -> int:
    raw = input(f"\nГлубина истории в днях [{default}]: ").strip()
    if not raw:
        return default
    try:
        d = int(raw)
        if d <= 0:
            raise ValueError
        return d
    except ValueError:
        print(f"Некорректное значение, беру {default}.")
        return default


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Bybit → PostgreSQL: свечи по выбранным парам и таймфреймам"
    )
    p.add_argument("-s", "--symbols", nargs="+",
                   help="Пары: BTCUSDT ETHUSDT ... (иначе спросим интерактивно)")
    p.add_argument("-i", "--intervals", nargs="+",
                   help="Таймфреймы: 60 240 D ... (иначе спросим интерактивно)")
    p.add_argument("-c", "--category", choices=["spot", "linear", "inverse"])
    p.add_argument("-d", "--days", type=int, help="Глубина истории, дней")
    p.add_argument("--concurrency", type=int, help="Одновременных HTTP-запросов")
    p.add_argument("--csv", action="store_true",
                   help="Дополнительно сохранять CSV-копию каждой таблицы")
    p.add_argument("--watch", action="store_true",
                   help="Работать непрерывно, докачивая новые свечи (Ctrl+C — выход)")
    p.add_argument("--poll-sec", type=int,
                   help="Период опроса в watch-режиме, сек")
    return p.parse_args()


async def async_main() -> None:
    args = parse_args()
    cfg = Config()

    interactive = not args.symbols and not args.intervals
    if args.symbols:
        cfg.symbols = _dedup([s.upper() for s in args.symbols])
    else:
        cfg.symbols = ask_pairs()
    if args.intervals:
        cfg.intervals = _dedup([
            i.upper() if i.isalpha() else i for i in args.intervals
        ])
    else:
        cfg.intervals = ask_intervals()

    if args.category:
        cfg.category = args.category
    if args.days:
        cfg.days = args.days
    elif interactive:
        cfg.days = ask_days(cfg.days)
    if args.concurrency:
        cfg.concurrency = args.concurrency
    if args.csv:
        cfg.export_csv = True

    cfg.validate()
    log.info(
        "Конфиг: symbols=%s intervals=%s category=%s days=%d concurrency=%d watch=%s",
        cfg.symbols, cfg.intervals, cfg.category, cfg.days,
        cfg.concurrency, args.watch,
    )

    db = Database(cfg.db_host, cfg.db_port, cfg.db_name, cfg.db_user, cfg.db_password)
    await db.connect()
    try:
        async with BybitClient(
            base_url=cfg.base_url,
            concurrency=cfg.concurrency,
            max_retries=cfg.max_retries,
            retry_delay=cfg.retry_delay,
        ) as client:
            collector = Collector(cfg, client, db)
            await collector.run(watch=args.watch, poll_sec=args.poll_sec)

            tables = await db.list_klines_tables()
            if tables:
                log.info("Таблицы: %s", ", ".join(tables))
    finally:
        await db.close()


def main() -> None:
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        log.warning("Прервано пользователем")


if __name__ == "__main__":
    main()