# Airflow_dataJobsProject

Para la entrega final del proyecto se especificó acomodar el modelo dimensional, en donde se enviarán los conjuntos de datos con herramientas de extracción, transformación y carga (ETL). A su vez envió datos en tiempo real como Apache Airflow y Apache Kafka. Para ello se seleccionó el conjunto de datos de los trabajos para ofrecer información de la calificación del trabajo y el título del trabajo. Ahora bien para el procesado de todas las dags se arrancó como se inició el proyecto con la base de datos grande y ya luego se fue arreglando el modelado dimensional.

El objetivo principal del proyecto es analizar las ofertas laborales en el campo de los datos y con el propósito de identificar patrones y tendencias que revelen las habilidades más demandadas en el mercado laboral actual. Esta investigación puede ayudar a las personas interesadas en este ámbito o que quieran meterse a este, ayudando a determinar si es la predicción deseada como una decisión laboral. Todo esto mediante el dataset de kaggle [Data Jobs Listings - Glassdoor](https://www.kaggle.com/datasets/andresionek/data-jobs-listings-glassdoor?select=glassdoor.csv).

## Requisitos

Antes de comenzar asegurate de tener instalados los isguientes componentes:

- Python, lo mas recomendable es que sea la ultima version.
- Docker, puedes hacer uso de docker desktop o descargarlo enl amaquina que dispongas.
- Jupyter, en caso de que quieras tener los notebooks.
- Airflow ya sea en una maquina virtual o en local.

## Configuracion de Postgresql 
1. Descarga e Instalación: Ve al sitio web oficial de PostgreSQL y descarga la versión adecuada para tu sistema operativo.
2. Configuración: Durante la instalación, se te pedirá establecer una contraseña para el usuario predeterminado postgres.
3. Herramientas Gráficas (opcional): Puedes instalar herramientas gráficas como pgAdmin para gestionar y trabajar con tus bases de datos de PostgreSQL de manera visual.

### Configuracion Postgresql con python
1. Instala el paquete psycopg2: En tu entorno de Python, instala el paquete psycopg2 que permite la conexión con PostgreSQL. Puedes hacerlo utilizando pip:
   ```bash
   pip install psycopg2

2. Conexión a la base de datos:
   ```bash
   import psycopg2
   
    conn = psycopg2.connect(
        user="postgres",
        password="tu_contraseña",
        host="localhost",
        database="tu_basededatos"
    )

## Ejecucion del codigo 

1. Clone el repositorio con el siguiente comando
   ```bash
   git clone https://github.com/VinkeArtunduaga/Airflow_dataJobsProject.git
2. Asegurate de que cumples con los requisitos anteriores.
3. Corra las imagenes de kafka y de postgres mediante
   ```bash
   sudo docker compose up -d
4. Entrar al contenedor de kafka y crear el topic con los siguientes comandos
   ```bash
   sudo docker exec -it kafka-test bash
   kafka-topics --bootstrap-server kafka-test:9092 --create --topic project
   kafka-console-consumer --bootstrap-server kafka-test:9092 --topic project --from-beginning
5. Correr el kafka consumer mediante
   ```bash
   python3 kafka_consumer.py
6. Luego correr airflow mediante
   ```bash
   airflow standalone
7. En caso de querer hacer ista de el dashboard en real time, crear las api con la estructura de datos especificada en el documento y conseguir el link de la api en caso de requerir mas informacion mirar el siguiente link [tutorial de power bi con kafka](https://desarrollopowerbi.com/dashboard-en-tiempo-real-con-apacha-kafka-python-y-power-bi/).
