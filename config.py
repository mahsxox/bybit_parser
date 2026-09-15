import os
from dataclasses import dataclass, field
from typing import List

from dotenv import load_dotenv

load_dotenv()

INTERVAL_MS = {
    "1": 60_000,
    "3": 3 * 60_000,
    "5": 5 * 60_000,
    "15": 15 * 60_000,
    "30": 30 * 60_000,
    "60": 60 * 60_000,
    "120": 2 * 60 * 60_000,
    "240": 4 * 60 * 60_000,
    "360": 6 * 60 * 60_000,
    "720": 12 * 60 * 60_000,
    "D": 24 * 60 * 60_000,
    "W": 7 * 24 * 60 * 60_000,
    "M": 30 * 24 * 60 * 60_000,
}

VALID_INTERVALS = set(INTERVAL_MS.keys())
VALID_CATEGORIES = {"spot", "linear", "inverse", "option"}


@dataclass
class Config:
    symbols: List[str] = field(
        default_factory=lambda: ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]
    )
    interval: str = "240"
    category: str = "spot"
    days: int = 730

    base_url: str = "https://api.bybit.com"
    concurrency: int = 8
    max_retries: int = 4
    retry_delay: float = 1.0

    db_host: str = os.getenv("DB_HOST", "localhost")
    db_port: int = int(os.getenv("DB_PORT", "5432"))
    db_name: str = os.getenv("DB_NAME", "bybit")
    db_user: str = os.getenv("DB_USER", "postgres")
    db_password: str = os.getenv("DB_PASSWORD", "postgres")

    export_csv: bool = False
    csv_dir: str = "./csv"

    def validate(self) -> None:
        if self.interval not in VALID_INTERVALS:
            raise ValueError(
                f"interval={self.interval!r} не поддерживается. "
                f"Допустимо: {sorted(VALID_INTERVALS)}"
            )
        if self.category not in VALID_CATEGORIES:
            raise ValueError(f"category={self.category!r} не поддерживается")
        if self.days <= 0:
            raise ValueError("days должен быть > 0")