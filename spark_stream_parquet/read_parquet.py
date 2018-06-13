import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession

if "SPARK_HOME" not in os.environ:
    os.environ['SPARK_HOME'] = "/home/andy/apache/spark"
SUBMIT_ARGS = "pyspark-shell"
os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS

spark = SparkSession \
    .builder \
    .config("spark.executor.memory", "1g") \
    .config("spark.cores.max", "2") \
    .appName("read_parquet") \
    .getOrCreate()

if __name__ == '__main__':
    partition_date = datetime.now().strftime('%d.%m.%Y.%H')
    parquet_files = './storage/retes/partition_date={}'.format(partition_date)
    if not os.path.exists(parquet_files):
        print('No data for this period')
        sys.exit()
    data = spark.read.parquet(parquet_files)
    data.show()
