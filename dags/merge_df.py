import pandas as pd
import psycopg2
import json

# Funciones del merge
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
    
def salaryStats_table():

    df = pd.read_csv("/home/vinke1302/Apache/csv/glassdoor_salary_salaries.csv", delimiter=',')

    conn = conn_postgresql()
    cursor = conn.cursor()

    create_table = """
        CREATE TABLE IF NOT EXISTS datajobs_salaryStats (
            id SERIAL PRIMARY KEY,
            job_title VARCHAR,
            pay_period VARCHAR,
            payPercentil_10 FLOAT,
            payPercentil_90 FLOAT,
            payPercentil_50 FLOAT,
            salary_type VARCHAR
        );
    """
    cursor.execute(create_table)
    conn.commit()
    print("Table created of salary stats")

    for index, row in df.iterrows():
        query = f"""INSERT INTO datajobs_salaryStats (
        job_title, pay_period, payPercentil_10, payPercentil_90, payPercentil_50, salary_type) VALUES( %s, %s, %s, %s, %s, %s)"""
        cursor.execute(query, (row["salary.salaries.val.jobTitle"],row["salary.salaries.val.payPeriod"], 
                               row["salary.salaries.val.salaryPercentileMap.payPercentile10"], row["salary.salaries.val.salaryPercentileMap.payPercentile90"], row["salary.salaries.val.salaryPercentileMap.payPercentile50"], row["salary.salaries.val.salaryType"]))
    
    conn.commit()
    print("Tabla creada of salaries")

    # delete_null_index = """
    #     DELETE FROM datajobs_salaryStats
    #     WHERE index IS NULL;
    # """
    # cursor.execute(delete_null_index)
    # conn.commit()
    # print("Rows with NULLS on 'index' of salary stats droped")

    update_null_payPercentil_50 = """
        UPDATE datajobs_salaryStats
        SET payPercentil_50 = 0
        WHERE payPercentil_50 IS NULL;
    """
    cursor.execute(update_null_payPercentil_50)
    conn.commit()
    print("Values NULL on salary stats 'payPercentil_50' updated to 0")

    cursor.close()
    conn.close()

def benefits_table():

    df = pd.read_csv("/home/vinke1302/Apache/csv/glassdoor_benefits_highlights.csv", delimiter=',')

    conn = conn_postgresql()
    cursor = conn.cursor()

    create_table_benefits = """
        CREATE TABLE IF NOT EXISTS datajobs_benefitsHighlights (
            id SERIAL PRIMARY KEY,
            highlighted_phrase VARCHAR,
            highlighted_icon VARCHAR,
            highlighted_name VARCHAR
        );
    """
    cursor.execute(create_table_benefits)
    conn.commit()
    print("Table created of benefits")

    for index, row in df.iterrows():
        query = f"""INSERT INTO datajobs_benefitsHighlights (highlighted_phrase, highlighted_icon,
        highlighted_name) VALUES( %s, %s, %s)"""
        cursor.execute(query, (row["benefits.highlights.val.highlightPhrase"], row["benefits.highlights.val.icon"], row["benefits.highlights.val.name"]))
    
    conn.commit()
    print("Table created of benefits")

    # delete_null_index_benefits = """
    #     DELETE FROM datajobs_benefitsHighlights
    #     WHERE index IS NULL;
    # """
    # cursor.execute(delete_null_index_benefits)
    # conn.commit()
    # print("Rows with NULL on 'index' of benefits droped")

    cursor.close()
    conn.close()

def dimension_company():

    conn = conn_postgresql()
    cursor = conn.cursor()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS datajobs_company (
        company_id SERIAL PRIMARY KEY,
        overview_foundedyear INTEGER,
        overview_hq VARCHAR,
        overview_industry VARCHAR,
        overview_revenue VARCHAR,
        overview_sector VARCHAR,
        overview_size VARCHAR,
        overview_type VARCHAR,
        overview_description TEXT,
        overview_website VARCHAR,
        rating_ceo_name VARCHAR,
        header_employername VARCHAR
    )
    """
    cursor.execute(create_table_query)
    conn.commit()

    move_data_query = """
        INSERT INTO datajobs_company (
            overview_foundedyear, overview_hq, overview_industry, overview_revenue,
            overview_sector, overview_size, overview_type, overview_description,
            overview_website, rating_ceo_name, header_employername
        )
        SELECT
            overview_foundedyear, overview_hq, overview_industry, overview_revenue,
            overview_sector, overview_size, overview_type, overview_description,
            overview_website, rating_ceo_name, header_employername
        FROM datajobs_glassdoor
    """
    cursor.execute(move_data_query)
    conn.commit()
    print("Data moved to company dimension")

    link_tables_query = """
        ALTER TABLE datajobs_glassdoor
        ADD COLUMN IF NOT EXISTS companies_id INTEGER,
        ADD CONSTRAINT fk_companies_id
        FOREIGN KEY (companies_id)
        REFERENCES datajobs_company(company_id)
    """
    cursor.execute(link_tables_query)
    conn.commit()
    print("Tables linked")

    update_company_query = """
        UPDATE datajobs_glassdoor
        SET companies_id = datajobs_company.company_id
        FROM datajobs_company
        WHERE datajobs_glassdoor.id = datajobs_company.company_id
    """
    cursor.execute(update_company_query)
    conn.commit()
    print("Companies linked to glassdoor data")

    cursor.close()
    conn.close()

def dropColumns():

    conn = conn_postgresql()
    cursor = conn.cursor()

    drop_columns_query = """
        ALTER TABLE datajobs_glassdoor
        DROP COLUMN IF EXISTS overview_foundedyear,
        DROP COLUMN IF EXISTS overview_hq,
        DROP COLUMN IF EXISTS overview_industry,
        DROP COLUMN IF EXISTS overview_revenue,
        DROP COLUMN IF EXISTS overview_sector,
        DROP COLUMN IF EXISTS overview_size,
        DROP COLUMN IF EXISTS overview_type,
        DROP COLUMN IF EXISTS overview_description,
        DROP COLUMN IF EXISTS overview_website,
        DROP COLUMN IF EXISTS rating_ceo_name,
        DROP COLUMN IF EXISTS header_employername
    """
    cursor.execute(drop_columns_query)
    conn.commit()

    cursor.close()
    conn.close()

def dimension_job():

    conn = conn_postgresql()
    cursor = conn.cursor()

    create_table_query = """
        CREATE TABLE IF NOT EXISTS datajobs_job (
            job_id SERIAL PRIMARY KEY,
            header_jobtitle VARCHAR,
            header_posted VARCHAR,
            job_discoverdate TIMESTAMP,
            job_description VARCHAR,
            job_jobsource VARCHAR,
            map_country VARCHAR,
            map_lat FLOAT,
            map_lng FLOAT,
            map_location VARCHAR,
            rating_starrating FLOAT,
            gatrackerdata_industry VARCHAR
        )
    """
    cursor.execute(create_table_query)
    conn.commit()
    print("Dimension job created")

    move_data_query = """
        INSERT INTO datajobs_job (
            header_jobtitle, header_posted, job_discoverdate, job_description, job_jobsource,
            map_country, map_lat, map_lng, map_location, rating_starrating, gatrackerdata_industry
        )
        SELECT
            header_jobtitle, header_posted, job_discoverdate, job_description, job_jobsource,
            map_country, map_lat, map_lng, map_location, rating_starrating, gatrackerdata_industry
        FROM datajobs_glassdoor
    """
    cursor.execute(move_data_query)
    conn.commit()
    print("Data moved to job dimension")

    add_column_query = """
        ALTER TABLE datajobs_glassdoor
        ADD COLUMN IF NOT EXISTS jobs_id INTEGER,
        ADD CONSTRAINT fk_jobs_id
        FOREIGN KEY (jobs_id)
        REFERENCES datajobs_job(job_id)
    """
    cursor.execute(add_column_query)
    conn.commit()
    print("Tables job linked")

    update_job_query = """
        UPDATE datajobs_glassdoor
        SET jobs_id = datajobs_job.job_id
        FROM datajobs_job
        WHERE datajobs_glassdoor.id = datajobs_job.job_id
    """
    cursor.execute(update_job_query)
    conn.commit()
    print("Jobs linked to glassdoor data")

    drop_columns_query = """
        ALTER TABLE datajobs_glassdoor
        DROP COLUMN IF EXISTS header_jobtitle,
        DROP COLUMN IF EXISTS header_posted,
        DROP COLUMN IF EXISTS job_description,
        DROP COLUMN IF EXISTS job_jobsource,
        DROP COLUMN IF EXISTS map_country,
        DROP COLUMN IF EXISTS map_lat,
        DROP COLUMN IF EXISTS map_lng,
        DROP COLUMN IF EXISTS map_location,
        DROP COLUMN IF EXISTS rating_starrating,
        DROP COLUMN IF EXISTS salary_salaries,
        DROP COLUMN IF EXISTS gatrackerdata_jobtitle,
        DROP COLUMN IF EXISTS gatrackerdata_industry,
        DROP COLUMN IF EXISTS salary_country_defaultname,
        DROP COLUMN IF EXISTS job_discoverdate
    """
    cursor.execute(drop_columns_query)
    conn.commit()
    print("columnas eliminadas")


    







