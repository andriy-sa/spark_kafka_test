import os
from datetime import datetime
from math import floor

from pyspark.sql import SparkSession, Row

if "SPARK_HOME" not in os.environ:
    os.environ['SPARK_HOME'] = "/home/elantix/apache/spark"
SUBMIT_ARGS = "--packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.0 pyspark-shell"
os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS

spark = SparkSession \
    .builder \
    .config("spark.executor.memory", "1g") \
    .config("spark.cores.max", "2") \
    .config('spark.mongodb.input.uri', 'mongodb://localhost/test') \
    .config('spark.mongodb.output.uri', 'mongodb://localhost/test') \
    .appName("test_mongo") \
    .getOrCreate()


def decimal_to_time(number):
    if not number:
        return None
    time_minutes = number * 60

    hours_part = "0{}".format(floor(number)) if floor(number) < 10 else floor(number)
    minutes_part = "0{}".format(floor(time_minutes % 60)) if floor(time_minutes % 60) < 10 else floor(time_minutes % 60)
    str_time = "{h}:{m}".format(h=hours_part, m=minutes_part)

    return datetime.strptime(str_time, '%H:%M').strftime('%I:%M %p')


def transform_data(item):
    all_count = item.all_count if item.all_count else 0
    occ_count = item.occ_count if item.occ_count else 0
    free_count = all_count - occ_count
    occ_rate = round(100 / all_count * occ_count)
    return Row(all_count=all_count, occ_count=occ_count, free_count=free_count, occ_rate=occ_rate,
               date=item.date, building_id=item.buildingId, arrival_time=decimal_to_time(item.arrival_time),
               departure_time=decimal_to_time(item.departure_time))


if __name__ == '__main__':
    data = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
        .option("spark.mongodb.input.collection", "test") \
        .load()

    arr_dep_data = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
        .option("spark.mongodb.input.collection", "test") \
        .load()

    data = data.groupBy('seat', 'date', 'buildingId').agg({"occ": "avg"})

    day_all_data = data.groupBy('date', 'buildingId').count() \
        .withColumnRenamed('count', 'all_count')

    day_occ_data = data.filter(data["avg(occ)"] > 0).groupBy('date', 'buildingId').count() \
        .withColumnRenamed('count', 'occ_count')

    arr_dep_day = arr_dep_data.groupBy('date', 'buildingId') \
        .agg({"ArrivalTime": "avg", "DepartureTime": "avg"}) \
        .withColumnRenamed('avg(ArrivalTime)', 'arrival_time') \
        .withColumnRenamed('avg(DepartureTime)', 'departure_time')

    result = day_all_data.join(day_occ_data, ['buildingId', 'date'], "left")

    result = result.join(arr_dep_day, ['buildingId', 'date'], "left").orderBy('date')

    result = result.rdd.map(transform_data).toDF()
    # print(result.show())

    result.write.format("com.mongodb.spark.sql.DefaultSource").mode("append") \
        .option("collection", "SparkReports") \
        .save()
