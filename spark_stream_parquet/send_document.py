import json
import random
from datetime import datetime

from kafka import KafkaProducer

if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers='localhost:9092',
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    producer.send('spark_kafka', {'rate': random.randint(1, 100), "id": random.randint(1, 10),
                                  'datetime': datetime.utcnow().strftime('%Y-%m-%d %H:%M')})
    print("successfully sent!!!")
    producer.close()
