import asyncio
import os
import sys

import asyncpg
from dotenv import load_dotenv

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

load_dotenv()



async def main() -> None:
    dsn = (
        f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )
    safe = dsn.replace(os.getenv("DB_PASSWORD", ""), "***")
    print("DSN:", safe)

    conn = await asyncpg.connect(dsn)
    version = await conn.fetchval("SELECT version();")
    print("OK:", version)
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())