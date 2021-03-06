import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql import functions

if "SPARK_HOME" not in os.environ:
    os.environ['SPARK_HOME'] = "/home/andy/apache/spark"
SUBMIT_ARGS = "--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 pyspark-shell"
os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS

spark = SparkSession \
    .builder \
    .master("local[2]") \
    .config("spark.executor.memory", "1g") \
    .config("spark.cores.max", "2") \
    .appName("streaming") \
    .getOrCreate()

if __name__ == '__main__':
    tes_shema = StructType().add('number','integer').add('username','string')
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
        .option("subscribe", "messages") \
        .load()

    df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")\
        .select(functions.from_json(functions.col("value").cast("string"), tes_shema).alias("json"),"timestamp").select("json.*","timestamp")

    df = df.groupBy('username').agg({'username': 'count', 'number': 'sum'})

    writer = df.writeStream.format('console').outputMode("complete").trigger(processingTime="5 seconds").start()

    writer.awaitTermination()
