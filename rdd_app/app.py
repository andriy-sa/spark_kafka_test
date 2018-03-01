import os

from pyspark.sql import SparkSession

if "SPARK_HOME" not in os.environ:
    os.environ['SPARK_HOME'] = "/home/elantix/apache/spark"
sc = SparkSession \
    .builder \
    .config("spark.executor.memory", "1g") \
    .config("spark.cores.max", "2") \
    .appName("test_rdd") \
    .getOrCreate()

if __name__ == '__main__':
    text_file = sc.read.text("./text_file.txt").rdd

    rdd = text_file.flatMap(lambda line: line.value.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda x, y: x + y) \
        .sortBy(lambda x: x[1], False)

    # # rdd.foreach(lambda x: print(x))
    # # print(rdd.collect())
    df = sc.createDataFrame(rdd, ['word', 'count'])

    # print(df.show())
    # df.coalesce(1).write.format('json').mode('append').save('file_name.json')
    df.coalesce(1).write.format('com.databricks.spark.csv').options(header='true').csv('csv_out')
