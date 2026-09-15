# Bybit Async Collector

Асинхронный сборщик исторических свечей с биржи Bybit. Собирает данные по любым торговым парам и таймфреймам, сохраняет в PostgreSQL, поддерживает инкрементальное обновление и режим непрерывного сбора.

## Возможности

* Любая торговая пара Bybit: спот, фьючерсы (linear), инверсные (inverse)
* Любой таймфрейм от 1 минуты до 1 месяца
* Глубина истории до 2 лет и более
* Автопагинация запросов к REST API Bybit
* Асинхронная загрузка чанков внутри одной пары и параллельная работа по нескольким парам
* Обработка лимитов API (retry с экспоненциальной задержкой при ответе 429)
* Запись в PostgreSQL без дублей через `ON CONFLICT DO NOTHING`
* Отдельная таблица для каждой пары и таймфрейма
* Режим watch для непрерывной докачки свежих свечей
* Опциональный экспорт в CSV

## Требования

* Python 3.11 или новее
* PostgreSQL 14 или новее

## Установка

```bash
git clone https://github.com/mahsxox/bybit_parser.git
cd bybit_parser
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

Создайте базу данных:

```sql
CREATE DATABASE bybit;
```

Настроить окружение

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=bybit
DB_USER=<логин>
DB_PASSWORD=your_<пароль>
```

Проверить подключение

```bash
python check_db.py
```

Ожидаемый вывод: `OK: PostgreSQL ...`.

## Использование

Запустите скрипт без аргументов. Он спросит пары, таймфреймы и глубину истории.

```bash
python main.py
```

Или указать сразу:

```bash
python main.py --symbols BTCUSDT ETHUSDT --intervals 60 240 D --days 730
```

### Режим watch

Скрипт работает непрерывно, периодически докачивая свежие свечи.

```bash
python main.py -s BTCUSDT ETHUSDT -i 60 240 --watch
```

Период опроса определяется автоматически по самому короткому таймфрейму, но не чаще 30 секунд. Можно задать вручную:

```bash
python main.py -s BTCUSDT -i 1 --watch --poll-sec 15
```

### Фьючерсы

По умолчанию используется спот. Для фьючерсов укажите категорию:

```bash
python main.py -s BTCUSDT -i 60 --category linear
```

### Экспорт в CSV

Добавьте флаг `--csv`, файлы сохранятся в папку `./csv`:

```bash
python main.py -s BTCUSDT -i 60 --csv
```

## Аргументы командной строки

| Аргумент | Описание | По умолчанию |
|---|---|---|
| `-s, --symbols` | Список торговых пар | интерактивный ввод |
| `-i, --intervals` | Список таймфреймов | интерактивный ввод |
| `-c, --category` | Категория: spot, linear, inverse | spot |
| `-d, --days` | Глубина истории в днях | 730 |
| `--concurrency` | Число одновременных HTTP запросов | 8 |
| `--csv` | Экспорт в CSV | выключено |
| `--watch` | Непрерывная докачка | выключено |
| `--poll-sec` | Период опроса в режиме watch | авто |

## Структура базы данных

Для каждой пары и таймфрейма создается отдельная таблица с именем вида `klines_<symbol>_<interval>`.

Схема таблицы:

```sql
CREATE TABLE klines_btcusdt_60 (
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
```

Колонка `timestamp` хранит миллисекунды в UTC и указывает на начало свечи. 

Защита от дублей реализована через `ON CONFLICT DO NOTHING`. Повторный запуск парсера не перезапишет уже загруженные строки, а добавит только те, которых еще нет.

## Автоматизация

### Windows: Task Scheduler

Вариант watch, постоянная работа:

```powershell
$action = New-ScheduledTaskAction `
    -Execute "C:\path\to\bybit_parser\.venv\Scripts\python.exe" `
    -Argument "main.py -s BTCUSDT ETHUSDT -i 60 240 --watch" `
    -WorkingDirectory "C:\path\to\bybit_parser"

$trigger = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME

$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 1) `
    -ExecutionTimeLimit ([TimeSpan]::Zero)

Register-ScheduledTask `
    -TaskName "BybitCollector" `
    -Action $action -Trigger $trigger -Settings $settings -Force
```

Управление задачей:

```powershell
Start-ScheduledTask   -TaskName "BybitCollector"
Stop-ScheduledTask    -TaskName "BybitCollector"
Get-ScheduledTask     -TaskName "BybitCollector" | Get-ScheduledTaskInfo
Unregister-ScheduledTask -TaskName "BybitCollector" -Confirm:$false
```

### Docker

Пример `Dockerfile`:

```dockerfile
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py ./

CMD ["python", "main.py", "-s", "BTCUSDT", "-i", "240"]
```

Пример `docker-compose.yml`:

```yaml
services:
  db:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: bybit
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres -d bybit"]
      interval: 5s
      timeout: 3s
      retries: 10

  app:
    build: .
    depends_on:
      db:
        condition: service_healthy
    environment:
      DB_HOST: db
      DB_PORT: 5432
      DB_NAME: bybit
      DB_USER: postgres
      DB_PASSWORD: postgres
    command: >
      python main.py
        -s BTCUSDT ETHUSDT
        -i 60 240
        --watch
    restart: unless-stopped

volumes:
  pgdata:
```

Запуск и управление:

```bash
docker compose up -d
docker compose logs -f app
docker compose down
```

## Структура проекта

```
bybit_parser/
  main.py             точка входа, разбор аргументов
  config.py           конфигурация и константы
  bybit_client.py     асинхронный клиент Bybit 
  collector.py        логика сбора и записи
  database.py         работа с PostgreSQL
  check_db.py         проверка соединения
  requirements.txt
  .env                
  csv/                
```

## Схема работы

1. Скрипт получает список пар и таймфреймов из аргументов или через интерактивный диалог.
2. Для каждой пары и таймфрейма определяется целевая таблица вида `klines_<symbol>_<interval>`.
3. Если таблицы нет, она создается автоматически.
4. Запрашивается последняя и первая свеча в таблице, чтобы определить пропущенные диапазоны.
5. Недостающие диапазоны разбиваются на чанки по 1000 свечей.
6. Чанки запрашиваются параллельно через `asyncio.gather`.
7. Результаты дедуплицируются по `timestamp` и сортируются.
8. Строки записываются в таблицу через `INSERT ... ON CONFLICT DO NOTHING`.
