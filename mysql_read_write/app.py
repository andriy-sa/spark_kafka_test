import os

from pyspark.sql import SparkSession

if "SPARK_HOME" not in os.environ:
    os.environ['SPARK_HOME'] = "/home/elantix/apache/spark"
SUBMIT_ARGS = "--packages mysql:mysql-connector-java:5.1.38 pyspark-shell"
os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS

spark = SparkSession \
    .builder \
    .config("spark.executor.memory", "1g") \
    .config("spark.cores.max", "2") \
    .appName("test_rdd") \
    .getOrCreate()

if __name__ == '__main__':
    chat_users = spark.read.format("jdbc").options(
        url="jdbc:mysql://localhost:3306/andy_chat",
        driver="com.mysql.jdbc.Driver",
        dbtable="users",
        user="root",
        password="1").load()

    chat_users = chat_users.select("id", "email").filter(chat_users['is_active'] == 1)

    users_messages = spark.read.format("jdbc").options(
        url="jdbc:mysql://localhost:3306/andy_chat",
        driver="com.mysql.jdbc.Driver",
        dbtable="messages",
        user="root",
        password="1").load()

    users_messages = users_messages.groupBy('sender_id').count().withColumnRenamed('count', 'messages_count')

    final_df = chat_users.join(users_messages, chat_users['id'] == users_messages['sender_id'], 'left') \
        .select('id', 'email', 'messages_count').withColumnRenamed('id', 'user_id')

    print(chat_users.show())
    print(users_messages.show())
    print(final_df.show())

    final_df.write.format("jdbc").options(
        url="jdbc:mysql://localhost:3306/andy_chat",
        driver="com.mysql.jdbc.Driver",
        dbtable="spark_result",
        user="root",
        password="1").mode('append').save()
