import time
import logging
from dotenv import load_dotenv
from prepared_df import get_spark, get_postgres, apply_transformations
from onetl.db import DBReader, DBWriter
from onetl.connection import Postgres
from onetl.strategy import IncrementalStrategy

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def run():
    start = time.time()
    log.info("Incremental Load ETL запущен")

    spark = get_spark()
    postgres = get_postgres(spark)
    postgres.check()

    reader = DBReader(
        connection=postgres,
        source="public.nyc_taxi_raw",
        hwm=DBReader.AutoDetectHWM(
            name="nyc_taxi_pickup_hwm",
            expression="tpep_pickup_datetime",),)

    writer = DBWriter(
        connection=postgres,
        target="public.nyc_taxi_clean",
        options=Postgres.WriteOptions(if_exists="append"),)

    with IncrementalStrategy():
        df = reader.run()

        if df.limit(1).count() == 0:
            log.info("Новых данных нет — пропускаем запись")
            spark.stop()
            return {"status": "success", "rows": 0, "duration_sec": 0}
        

        log.info(f"Прочитано новых строк: {df.count():,}")

        log.info("Применяем трансформации")
        df_transformed = apply_transformations(df)
        total = df_transformed.count()
        log.info(f"Строк после трансформаций: {total:,}")

        writer.run(df_transformed)

    elapsed = round(time.time() - start, 2)
    log.info(f"Incremental Load завершён за {elapsed} сек, записано {total:,} строк")

    spark.stop()
    return {"status": "success", "rows": total, "duration_sec": elapsed}


if __name__ == "__main__":
    run()