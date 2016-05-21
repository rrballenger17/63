from kafka import KafkaProducer
import random
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092')
while True:
	producer.send('spark_topic', str(random.randint(1,10)))
	time.sleep(1)

