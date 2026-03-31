import os
import socket
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from onetl.connection import Postgres
from dotenv import load_dotenv
import sys


load_dotenv()

def get_local_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    ip = s.getsockname()[0]
    s.close()
    return ip


def get_spark():
    local_ip = get_local_ip()
    maven_packages = Postgres.get_packages()
    return (
        SparkSession.builder
        .appName("nyc-taxi")
        .master("spark://localhost:7077")
        .config("spark.driver.host", local_ip)
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.jars.packages", ",".join(maven_packages))
        .config("spark.pyspark.python", "python3")
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate())
 
def get_postgres(spark):
    return Postgres(
        host=get_local_ip(),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        database=os.getenv("POSTGRES_DB"),
        spark=spark)

def apply_transformations(df):
    df = df.filter(
        (F.col("tpep_pickup_datetime").isNotNull()) &
        (F.col("tpep_dropoff_datetime").isNotNull()) &
        (F.col("tpep_pickup_datetime") < F.col("tpep_dropoff_datetime")) &
        (F.col("trip_distance").isNull() | (F.col("trip_distance") > 0)) &
        (F.col("fare_amount").isNull() | (F.col("fare_amount") >= 0)) &
        (F.col("passenger_count").isNull() | (F.col("passenger_count") > 0)) &
        (F.col("passenger_count").isNull() | (F.col("passenger_count") <= 6)) &
        (F.col("extra").isNull() | (F.col("extra") >= 0)) &
        (F.col("mta_tax").isNull() | (F.col("mta_tax") >= 0)) &
        (F.col("improvement_surcharge").isNull() | (F.col("improvement_surcharge") >= 0)) &
        (F.col("tip_amount").isNull() | (F.col("tip_amount") >= 0)) &
        (F.col("tolls_amount").isNull() | (F.col("tolls_amount") >= 0)) &
        (F.col("total_amount").isNull() | (F.col("total_amount") > 0)))

    df = df \
        .withColumn("trip_duration",
            F.round((F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60, 2)) \
        .withColumn("avg_speed",
            F.when(F.col("trip_duration") > 0, F.round(F.col("trip_distance") / (F.col("trip_duration") / 60), 2)).otherwise(None)) \
        .withColumn("tip_percentage",
            F.when(F.col("fare_amount") > 0, F.round(F.col("tip_amount") / F.col("fare_amount") * 100, 2)).otherwise(None)) \
        .withColumn("hour_of_day", F.hour("tpep_pickup_datetime")) \
        .withColumn("day_of_week", F.dayofweek("tpep_pickup_datetime")) \
        .withColumn("is_rush_hour", F.col("hour_of_day").between(7, 10) | F.col("hour_of_day").between(17, 20))

    df = df.withColumn("trip_category",
        F.when(F.col("trip_distance") < 2, "Short")
        .when(F.col("trip_distance") < 10, "Medium")
        .when(F.col("trip_distance") < 20, "Long")
        .when(F.col("trip_distance") >= 20, "Very Long")
        .otherwise(None))

    return df