#Codigo de streaming

from kafka import KafkaProducer, KafkaConsumer
import json
import pandas as pd
import time
import psycopg2
import requests
from json import loads
from datetime import datetime

# Carga de la configuracion de la base de datos desde un archivo JSON
with open("/home/vinke1302/Apache/credentials/db_config.json") as config_file:
    db_config = json.load(config_file)

def conn_postgresql():

    try:

        with open('/home/vinke1302/Apache/credentials/db_config.json') as f:
            dbfile = json.load(f)
        
        conn = psycopg2.connect(
            host=dbfile["host"],
            user=dbfile["user"],
            password=dbfile["password"],
            database=dbfile["database"],
            port = 5432
        )

        print("Conexion a PostgreSQL exitosa")
        return conn
    
    except Exception as e:
        print(f"Error al conectar a PostgreSQL: {e}")
        return None
    
# Funcion para ejecutar una consulta y obtener un DataFrame
def query():
    conn = conn_postgresql()
    df = pd.read_sql_query("SELECT header_jobtitle, job_discoverdate, rating_starrating FROM datajobs_job", conn)
    print(df)
    return df

# Funcion para enviar mensajes a Kafka
def kafka_producer():
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    df = query()

    for index, row in df.iterrows():
        message = {
            "header_jobtitle": row["header_jobtitle"],
            "job_discoverdate": str(row["job_discoverdate"]),
            "rating_starrating": row["rating_starrating"]
        }

        producer.send('project', value=message)
        time.sleep(0.01) 

    producer.close()


