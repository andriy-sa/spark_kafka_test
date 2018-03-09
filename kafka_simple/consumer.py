import json

from kafka import KafkaConsumer

if __name__ == '__main__':
    consumer = KafkaConsumer('test_topic', bootstrap_servers='localhost:9092',
                             value_deserializer=lambda v: json.loads(v.decode('utf-8')))
    print("start consuming...")
    for msg in consumer:
        print(type(msg))
        print(msg.value)
        print("-----------------")
