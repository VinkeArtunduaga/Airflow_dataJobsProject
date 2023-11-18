#Consumer
from kafka import KafkaConsumer
import json
import pandas as pd
import requests
from json import loads
from datetime import datetime

API_ENDPOINT = "https://api.powerbi.com/beta/693cbea0-4ef9-4254-8977-76e05cb5f556/datasets/3e58f598-3876-4f3d-8c00-be3eb553182c/rows?experience=power-bi&key=1PotcXGWk4NfwfUlJUmc%2FPTCvwtvjbf4Mc6UYPtsZ7DjyjjNfD6kWy9vCnZpXz3%2Bzkm2Y6u4qPjklxdIPBe8oQ%3D%3D"

# Funcion para consumir mensajes de Kafka y enviar a la API de Power BI
def kafka_consumer():
    consumer = KafkaConsumer(
        'project',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda m: loads(m.decode('utf-8'))
    )

    consumer.poll()
    consumer.seek_to_end()

    for message in consumer:
        s = message.value
        df = pd.DataFrame.from_dict([s])
        df['job_discoverdate'] = pd.to_datetime(df['job_discoverdate'])
        df['job_discoverdate'] = df['job_discoverdate'].apply(lambda x: x.strftime("%Y-%m-%dT%H:%M:%S.%fZ"))
        print(df["job_discoverdate"])

        data = bytes(df.to_json(orient='records'), 'utf-8')
        req = requests.post(API_ENDPOINT, data)

if __name__ == "__main__":

    kafka_consumer()
