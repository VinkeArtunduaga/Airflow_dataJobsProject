# ETL project

import pandas as pd
import logging
import json
import psycopg2

from extract_def import loadAndSave_data, create_and_populate_table, eliminar_columnas, reemplazar_valores_nulos, crear_columna_jobTitle_normalized, api_extract, categorize_job_titles, update_normalized_job_titles
from transform_def import update_jobtitle, update_empty, update_overview_nulls, drop_columns, drop_apiColumn
from merge_df import salaryStats_table, benefits_table, dimension_company, dropColumns, dimension_job
from kafka_stream import kafka_producer

def conn():

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
         

def extract_dataset():
    #Guardo el csv 
    file_path = '/home/vinke1302/Apache/csv/dataJobs_all.csv'
    output_filename = 'datajobs_all.csv'

    df = loadAndSave_data(file_path, output_filename)
    logging.info("csv read succesfully")

    create_and_populate_table(df)
    logging.info("created table")

    eliminar_columnas()
    logging.info("columns eliminated")

    reemplazar_valores_nulos()
    logging.info("nulls replaced")

    crear_columna_jobTitle_normalized()
    logging.info("column created")

    logging.info("extract dataset completed")

def transform_dataset():

    file_path = '/home/vinke1302/Apache/csv/dataJobs_all.csv'
    output_filename = 'datajobs_all.csv'
    df = loadAndSave_data(file_path, output_filename)

    category_mapping = {

        "Technology/IT": [
            "Java Team Lead", "Software Engineer", "Developer", "Technical Support Engineer",
            "AI Engineer", "Cloud Engineer", "Data Analyst", "Security Data Scientist",
            "Data Engineer", "Data Center Technician (DCO)",
            "Software Developer within Automotive", "Cloud Engineer/DevOps Engineer",
            "System Analyst/Backend Developer", "Junior Big Data Engineer",
            "Sr Software Engineer, Ad tech", "Software Engineer – Networks"
        ],

        "Management": ["Manager", "Director", "Lead"],

        "Data Science/Analytics": [
            "Data Scientist", "Statistician", "Market Research Analyst",
            "Big Data Analyst (m/w)"
        ],
        "Business": [
            "Sales Manager", "Account Manager", "Business Development",
            "Marketing Manager", "Product Manager", "Digital Marketing",
            "Finance Analyst", "Financial Manager", "Consultant", "Advisor",
            "Associate Practice Engagement Manager",
            "Proactive Monitoring Engineer-Commerce Cloud",
            "Agile Business Analyst", "Associate"
        ],
        "Healthcare/Science": [
            "Doctor", "Nurse", "Medical Scientist", "Biomedical Scientist",
            "Clinical Research Associate", "Genetic Counselor",
            "Senior Research Scientist – Antibody reagents: in vivo biology",
            "Public Health Specialist Manager",
            "Clinical Scientist in Pharma Research and Early Development (m/f/d)"
        ],
        "Engineering": [
            "Engineer", "Project Engineer", "Geotechnical Engineer",
            "Controls Engineer", "Energy Engineer", "Aerospace Engineer",
            "Structural and Foundation Engineer",
            "BMS Controls Engineer", "Energy Engineer",
            "Automotive Data Analyst", "Construction Engineer",
            "Structural and Foundation Engineer",
            "Associate, Big Data Analyst (m/w)", "Sr.Equipment Engineer"
        ],
        "Education": [
            "Assistant Professor", "Lecturer", "Educational Consultant",
            "Assistant Professor - Computer Sciences",
            "Territory Manager (Ponta Grossa/PR)"
        ],
        "Logistics/Supply Chain": ["Supply Chain Analyst", "Logistics Manager"],

        "Design": ["Designer", "Art Director", "Interior Designer"],

        "Retail": ["Retail Manager"],

        "Research": [
            "Research Scientist", "Research Analyst",
            "Research Scientist - Computer Vision",
            "Senior Research Scientist", "Assistant Professor - Computer Sciences"
        ],

        "Human Resources": ["HR Manager", "Recruiter"],

        "Architecture/Construction": ["Architect", "Construction Engineer"],

        "Manufacturing": [
            "Manufacturing Engineer", "Quality Control Analyst"
        ],
        "Food Industry": ["Executive Chef", "Food Quality Analyst"],

        "Tourism/Hospitality": [
            "Tourism Coordinator", "Hospitality Manager", "Travel Consultant"
        ],
        "Language/Linguistics": ["Linguist", "Language Teacher"],

        "Transportation": [
            "Transportation Planner", "Aviation Engineer", "Logistics Analyst",
            "Roving Project Manager", "Project Manager - Mechanical + Electrical",
            "Territory Manager (Outdoor Sales Consultant)"
        ]
    }

    categorize_job_titles(df, category_mapping)
    logging.info("job title categorized")

    update_normalized_job_titles(df)
    logging.info("updated job title")

    update_jobtitle()
    logging.info("updated jobtitle")
    
    update_empty()
    logging.info("updated emptys jobs")

    update_overview_nulls()
    logging.info("updated overview nulls")

    drop_columns()
    logging.info("columns droped")

    # print("transformaciones del dataset")

def extract():
    print("extraccion dataset")

def extract_api():

    api_extract()
    logging.info("CSV of the API extracted")
    #print("Extracted API")

def transform_api():

    drop_apiColumn()
    logging.info("Tranformations done")
    #print("Transformations API")

def merge():

    salaryStats_table()
    logging.info("Salary stats table created")

    benefits_table()
    logging.info("Benefits table created")

    dimension_company()
    logging.info("Company dimension created")

    dropColumns()
    logging.info("columns moved from company droped")

    dimension_job()
    logging.info("dimension of job created")

def load():

    print("dimensional model done")

def kafka_stream():

    kafka_producer()
    logging.info("producer started")



    