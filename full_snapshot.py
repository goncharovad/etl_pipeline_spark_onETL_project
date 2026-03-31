import time
import logging
from dotenv import load_dotenv
from prepared_df import get_spark, get_postgres, apply_transformations
from onetl.db import DBReader, DBWriter
from onetl.connection import Postgres
from etl_entities.hwm import ColumnDateTimeHWM
from onetl.hwm.store import YAMLHWMStore

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def run():
    start = time.time()
    log.info(" Full Snapshot ETL запущен ")
 
    spark = get_spark()
    postgres = get_postgres(spark)
    postgres.check()
 
    reader = DBReader(
        connection=postgres,
        source="public.nyc_taxi_raw",)
    df = reader.run()
    log.info(f"Прочитано строк: {df.count():,}")
 
    df_transformed = apply_transformations(df)
    total = df_transformed.count()
    log.info(f"Строк после трансформаций: {total:,}")
 
    writer = DBWriter(
        connection=postgres,
        target="public.nyc_taxi_clean",
        options=Postgres.WriteOptions(if_exists="replace_entire_table"),)
    writer.run(df_transformed)

    max_date = df_transformed.agg(
        {"tpep_pickup_datetime": "max"}
    ).collect()[0][0]
 
    hwm = ColumnDateTimeHWM(
        name="nyc_taxi_pickup_hwm",
        entity="public.nyc_taxi_raw",
        expression="tpep_pickup_datetime",
        value=max_date,)
    hwm_store = YAMLHWMStore()
    hwm_store.set_hwm(hwm)
 
    log.info(f"HWM сохранён: {max_date}")
 
    elapsed = round(time.time() - start, 2)
    log.info(f" Full Snapshot завершён за {elapsed} сек, записано {total:,} строк ")
 
    spark.stop()
    return {"status": "success", "rows": total, "duration_sec": elapsed}
 
if __name__ == "__main__":
    run()