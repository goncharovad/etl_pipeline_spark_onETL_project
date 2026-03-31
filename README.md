# NYC Yellow Taxi ETL Pipeline

ETL-пайплайн для обработки данных NYC Yellow Taxi с использованием Apache Spark и onETL.

## Датасет

[NYC Yellow Taxi Trip Data](https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data) — реальные данные поездок такси в Нью-Йорке за январь 2015 и январь-март 2016.

## Архитектура

```
Kaggle (CSV)
     ↓
load_kaggle_data.py
     ↓
PostgreSQL (nyc_taxi_raw)
     ↓
Apache Spark + onETL
     ↓
PostgreSQL (nyc_taxi_clean)
     ↑
FastAPI (api/main.py)
```

## Технологический стек

- **Python 3.13**
- **Apache Spark 3.4+** — движок обработки данных (клиентский режим)
- **onETL** — чтение/запись через DBReader/DBWriter, HWM для инкрементальной загрузки
- **PostgreSQL 15** — источник и назначение данных
- **FastAPI** — REST API для управления ETL
- **Docker** — контейнеризация Spark и PostgreSQL

## Структура проекта

```
nyc-taxi-etl/
├── docker-compose.yml       # инфраструктура (PostgreSQL + Spark)
├── .env.example             # шаблон переменных окружения
├── requirements.txt         # зависимости Python
├── prepared_df.py           # общие функции (Spark сессия, трансформации)
├── load_kaggle_data.py      # загрузка данных Kaggle → PostgreSQL
├── full_snapshot.py         # ETL полная загрузка
├── incremental_load.py      # ETL инкрементальная загрузка
├── api/
│   └── main.py              # FastAPI сервис
└── README.md
```

## Установка

### 1. Клонировать репозиторий

```bash
git clone <repo_url>
cd nyc-taxi-etl
```

### 2. Установить зависимости

```bash
pip install -r requirements.txt
```

### 3. Создать .env файл

```bash
cp .env.example .env
```

Заполните `.env`:
```bash
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=nyc_taxi
SPARK_MASTER=spark://localhost:7077
```

### 4. Настроить Kaggle API

Скачайте `kaggle.json` с [kaggle.com](https://www.kaggle.com) → Settings → API → Create New Token

```bash
mkdir -p ~/.kaggle
mv ~/Downloads/kaggle.json ~/.kaggle/
chmod 600 ~/.kaggle/kaggle.json
```

### 5. Поднять инфраструктуру

```bash
docker-compose up -d
```

## Запуск ETL

### Загрузка данных

```bash
# загрузить только 2015 год (для тестирования)
python load_kaggle_data.py --only2015 --limit 100000

# ИЛИ загрузить все данные
python load_kaggle_data.py
```
### Full Snapshot
```bash
Полная загрузка всех данных из `nyc_taxi_raw` → трансформации → `nyc_taxi_clean`:

python full_snapshot.py
```

### Incremental Load
```bash
# загрузить 2016 год (добавить новые данные для проверки инкрементальной загрузки только после full snapshot)

python load_kaggle_data.py --append --skip2015 --limit 100000

# Загрузка только новых данных после последнего HWM:

python incremental_load.py
```

### Запуск API

```bash
uvicorn api.main:app --reload --port 8000
```

Документация API: **http://localhost:8000/docs**

## API эндпоинты

| Метод | URL | Описание |
|-------|-----|----------|
| POST | `/etl/full` | Запуск Full Snapshot ETL |
| POST | `/etl/incremental` | Запуск Incremental Load ETL |
| GET | `/etl/status/{task_id}` | Статус задачи |
| GET | `/etl/history` | История запусков |
| GET | `/` | Проверка работоспособности |

## Трансформации данных

1. **Фильтрация аномалий** — удаление невалидных записей (нулевые дистанции, отрицательные суммы, > 6 пассажиров)
2. **Вычисляемые поля** — `trip_duration_min`, `avg_speed_mph`, `tip_percentage`, `hour_of_day`, `day_of_week`, `is_rush_hour`
3. **Классификация поездок** — Short (< 2 миль), Medium (2-10), Long (10-20), Very Long (> 20)

## Инкрементальная загрузка

HWM (High Water Mark) хранится в файле:
```
~/Library/Application Support/onETL/yml_hwm_store/nyc_taxi_pickup_hwm.yml
```

- Первый запуск: загружает все данные, сохраняет `max(tpep_pickup_datetime)` как HWM
- Последующие запуски: загружает только `WHERE tpep_pickup_datetime > HWM`
- HWM обновляется **только после успешной записи**
