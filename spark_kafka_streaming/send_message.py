import json
import random
from kafka import KafkaProducer

if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers='localhost:9092',
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    producer.send('messages', {'number': random.randint(1, 100), "username":"andy"})
    print("successfully sent!!!")
    producer.close()