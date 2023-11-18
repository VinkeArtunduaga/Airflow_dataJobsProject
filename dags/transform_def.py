import pandas as pd
import psycopg2
import json

# Funciones de transform

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
    
def update_jobtitle():

    conn = conn_postgresql()
    cursor = conn.cursor()
    update_jobtitle = """
        UPDATE dataJobs_glassdoor
        SET jobTitle_normalized = CASE
            WHEN header_jobTitle ILIKE '%Business Analyst%' THEN 'Business'
            WHEN header_jobTitle ILIKE '%Business%' THEN 'Business'
            WHEN header_jobTitle ILIKE '%Product Owner%' THEN 'Business' 
            WHEN header_jobTitle ILIKE '%Schadebeheerder%' THEN 'Business' 
            WHEN header_jobTitle ILIKE '%Marketing Specialist%' THEN 'Business' 
            WHEN header_jobTitle ILIKE '%Product%' THEN 'Business' 
            WHEN header_jobTitle ILIKE '%Project Buyer%' THEN 'Business' 
            WHEN header_jobTitle ILIKE '%Accountancy%' THEN 'Business' 
            WHEN header_jobTitle ILIKE '%Accountant%' THEN 'Business' 
            WHEN header_jobTitle ILIKE '%DOSSIERBEHEERDER%' THEN 'Business' 
            WHEN header_jobTitle ILIKE '%Dossierbeheerder%' THEN 'Business' 
            WHEN header_jobTitle ILIKE '%Contractbeheerder%' THEN 'Business'
            WHEN header_jobTitle ILIKE '%Løsningsarkitekter%' THEN 'Business' 
            WHEN header_jobTitle ILIKE '%Klantenbeheerder%' THEN 'Business' 
            WHEN header_jobTitle ILIKE '%Operationeel Beheerder%' THEN 'Business' 
            WHEN header_jobTitle ILIKE '%Pöyry%' THEN 'Business' 
            WHEN header_jobTitle ILIKE '%Project Administration%' THEN 'Business' 
            WHEN header_jobTitle ILIKE '%Relatiebeheerder%' THEN 'Business'
            WHEN header_jobTitle ILIKE '%Materials Science%' THEN 'Healthcare/Science'
            WHEN header_jobTitle ILIKE '%Clinical Research%' THEN 'Healthcare/Science' 
            WHEN header_jobTitle ILIKE '%Scientist%' THEN 'Healthcare/Science'
            WHEN header_jobTitle ILIKE '%Computational Biologist%' THEN 'Healthcare/Science' 
            WHEN header_jobTitle ILIKE '%Medical%' THEN 'Healthcare/Science'
            WHEN header_jobTitle ILIKE '%Sr Lab%' THEN 'Healthcare/Science' 
            WHEN header_jobTitle ILIKE '%Molecular%' THEN 'Healthcare/Science'
            WHEN header_jobTitle ILIKE '%Global HR%' THEN 'Human Resources' 
            WHEN header_jobTitle ILIKE '%Payrollbeheerder%' THEN 'Human Resources'
            WHEN header_jobTitle ILIKE '%Analyst%' THEN 'Data Science/Analytics' 
            WHEN header_jobTitle ILIKE '%Database Administrator%' THEN 'Data Science/Analytics'  
            WHEN header_jobTitle ILIKE '%Datenbank%' THEN 'Data Science/Analytics'
            WHEN header_jobTitle ILIKE '%Databasebeheerder%' THEN 'Data Science/Analytics' 
            WHEN header_jobTitle ILIKE '%Deep Learning%' THEN 'Data Science/Analytics' 
            WHEN header_jobTitle ILIKE '%Deep-learning%' THEN 'Data Science/Analytics' 
            WHEN header_jobTitle ILIKE '%Data%' THEN 'Data Science/Analytics'
            WHEN header_jobTitle ILIKE '%Server DBA%' THEN 'Data Science/Analytics' 
            WHEN header_jobTitle ILIKE '%Données%' THEN 'Data Science/Analytics'
            WHEN header_jobTitle ILIKE '%DBA%' THEN 'Data Science/Analytics'
            WHEN header_jobTitle ILIKE '%INGÉNIEUR%' THEN 'Engineering' 
            WHEN header_jobTitle ILIKE '%Eng%' THEN 'Engineering'
            WHEN header_jobTitle ILIKE '%Engr%' THEN 'Engineering' 
            WHEN header_jobTitle ILIKE '%ingenieur%' THEN 'Engineering' 
            WHEN header_jobTitle ILIKE '%Projectbeheerder%' THEN 'Engineering' 
            WHEN header_jobTitle ILIKE '%Wagenparkbeheerder%' THEN 'Engineering'
            WHEN header_jobTitle ILIKE '%Project Administrator%' THEN 'Management'
            WHEN header_jobTitle ILIKE '%Projects Financial%' THEN 'Management' 
            WHEN header_jobTitle ILIKE '%Project Coordinator%' THEN 'Management'
            WHEN header_jobTitle ILIKE '%Project Management%' THEN 'Management' 
            WHEN header_jobTitle ILIKE '%Project Planning%' THEN 'Management'
            WHEN header_jobTitle ILIKE '%Functioneel Beheerder%' THEN 'Management' 
            WHEN header_jobTitle ILIKE '%Program Coordinator%' THEN 'Management' 
            WHEN header_jobTitle ILIKE '%Project Specialist%' THEN 'Management' 
            WHEN header_jobTitle ILIKE '%Project Assistant%' THEN 'Management'
            WHEN header_jobTitle ILIKE '%Project Support%' THEN 'Management' 
            WHEN header_jobTitle ILIKE '%Delivery Project%' THEN 'Management' 
            WHEN header_jobTitle ILIKE '%Program Management%' THEN 'Management' 
            WHEN header_jobTitle ILIKE '%Project Co-ordinator%' THEN 'Management' 
            WHEN header_jobTitle ILIKE '%Project Officer%' THEN 'Management'
            WHEN header_jobTitle ILIKE '%MANAGEMENT%' THEN 'Management' 
            WHEN header_jobTitle ILIKE '%Project Planner%' THEN 'Management'
            WHEN header_jobTitle ILIKE '%Project Controller%' THEN 'Management' 
            WHEN header_jobTitle ILIKE '%Project Office%' THEN 'Management' 
            WHEN header_jobTitle ILIKE '%Project Admin%' THEN 'Management' 
            WHEN header_jobTitle ILIKE '%Projectvoorbereider%' THEN 'Management' 
            WHEN header_jobTitle ILIKE '%Projectleider%' THEN 'Management'
            WHEN header_jobTitle ILIKE '%Applicatiebeheerder%' THEN 'Technology/IT' 
            WHEN header_jobTitle ILIKE '%Systeembeheerder%' THEN 'Technology/IT' 
            WHEN header_jobTitle ILIKE '%Programmer%' THEN 'Technology/IT' 
            WHEN header_jobTitle ILIKE '%SOFTWARE%' THEN 'Technology/IT'
            WHEN header_jobTitle ILIKE '%Computer Scientist%' THEN 'Technology/IT' 
            WHEN header_jobTitle ILIKE '%Software Developer%' THEN 'Technology/IT' 
            WHEN header_jobTitle ILIKE '%Machine Learning%' THEN 'Technology/IT' 
            WHEN header_jobTitle ILIKE '%Werkplekbeheerder%' THEN 'Technology/IT' 
            WHEN header_jobTitle ILIKE '%Netwerkbeheerder%' THEN 'Technology/IT' 
            WHEN header_jobTitle ILIKE '%MJUKVARUUTVECKLARE%' THEN 'Technology/IT' 
            WHEN header_jobTitle ILIKE '%Technology Consulting%' THEN 'Technology/IT' 
            WHEN header_jobTitle ILIKE '%Technischer Berater%' THEN 'Technology/IT' 
            WHEN header_jobTitle ILIKE '%Beheerder IT%' THEN 'Technology/IT' 
            WHEN header_jobTitle ILIKE '%Développement Java%' THEN 'Technology/IT' 
            WHEN header_jobTitle ILIKE '%Cloud applicatie%' THEN 'Technology/IT' 
            WHEN header_jobTitle ILIKE '%ICT beheerder%' THEN 'Technology/IT' 
            WHEN header_jobTitle ILIKE '%Linux Beheerder%' THEN 'Technology/IT' 
            WHEN header_jobTitle ILIKE '%Project Executive%' THEN 'Technology/IT' 
            WHEN header_jobTitle ILIKE '%Angular%' THEN 'Technology/IT' 
            WHEN header_jobTitle ILIKE '%Informatie analist%' THEN 'Technology/IT'
            WHEN header_jobTitle ILIKE '%IT%' THEN 'Technology/IT' 
            WHEN header_jobTitle ILIKE '%Technisch Beheerder%' THEN 'Technology/IT' 
            WHEN header_jobTitle ILIKE '%Technisch%' THEN 'Technology/IT' 
            WHEN header_jobTitle ILIKE '%Technieker%' THEN 'Technology/IT'
            WHEN header_jobTitle ILIKE '%Burgerzaken%' THEN 'Government' 
            WHEN header_jobTitle ILIKE '%Ketenbeheerder%' THEN 'Logistics/Supply Chain' 
            WHEN header_jobTitle ILIKE '%stockbeheerder%' THEN 'Logistics/Supply Chain' 
            WHEN header_jobTitle ILIKE '%Magazijnbeheerder%' THEN 'Logistics/Supply Chain' 
            WHEN header_jobTitle ILIKE '%Orderbeheerder%' THEN 'Logistics/Supply Chain' 
            WHEN header_jobTitle ILIKE '%Magazijn%' THEN 'Logistics/Supply Chain'  
            WHEN header_jobTitle ILIKE '%Project Material%' THEN 'Logistics/Supply Chain'
            WHEN header_jobTitle ILIKE '%Research%' THEN 'Research' 
            WHEN header_jobTitle ILIKE '%Building%' THEN 'Architecture/Construction' 
            WHEN header_jobTitle ILIKE '%BIM Modeller%' THEN 'Architecture/Construction' 
            WHEN header_jobTitle ILIKE '%Modelleur%' THEN 'Architecture/Construction' 
            WHEN header_jobTitle ILIKE '%Construction%' THEN 'Architecture/Construction'
            WHEN header_jobTitle ILIKE '%到校學前康復服務%' THEN 'Education' 
            WHEN header_jobTitle ILIKE '%School%' THEN 'Education' 
            WHEN header_jobTitle ILIKE '%Cateringbeheerder%' THEN 'Food Industry' 
            WHEN header_jobTitle ILIKE '%Manufacturing%' THEN 'Manufacturing'  
            WHEN header_jobTitle ILIKE '%Debiteurenbeheerder%' THEN 'Manufacturing'
            WHEN header_jobTitle ILIKE '%Transportation%' THEN 'Transportation' 
            WHEN header_jobTitle ILIKE '%Fleet Programs%' THEN 'Transportation' 
            WHEN header_jobTitle ILIKE '%Polisbeheerder%' THEN 'Transportation' 
            WHEN header_jobTitle ILIKE '%Hotelbeheerder%' THEN 'Tourism/Hospitality'  
            WHEN header_jobTitle ILIKE '%Commercieel bediende%' THEN 'Retail' 
            WHEN header_jobTitle ILIKE '%Modelmaker%' THEN 'Design'
            ELSE jobTitle_normalized
        END;
    """
    cursor.execute(update_jobtitle)
    conn.commit()
    print("Valores actualizados en jobTitle_normalized")
    cursor.close()
    conn.close()

def update_empty():

    conn = conn_postgresql()
    cursor = conn.cursor()

    update_empty = """
        UPDATE dataJobs_glassdoor
        SET jobTitle_normalized = 'Other'
        WHERE jobTitle_normalized = '';
    """
    cursor.execute(update_empty)
    conn.commit()
    print("Campos vacios cambiados")
    cursor.close()
    conn.close()

def update_overview_nulls():

    conn = conn_postgresql()
    cursor = conn.cursor()

    update_null_overview_type= """
        UPDATE dataJobs_glassdoor
        SET overview_type = 'Unknown'
        WHERE overview_type IS NULL;
    """
    cursor.execute(update_null_overview_type)
    conn.commit()
    print("Valores nulos en overview_type actualizados")
    cursor.close()
    conn.close()

def drop_columns():

    conn = conn_postgresql()
    cursor = conn.cursor()

    drop_column = ['benefits_benefitRatingdecimal', 'benefits_comments','gaTrackerData_empName',
                   'header_normalizedjobtitle', 'map_employerName', 'header_location',
                   'gaTrackerData_sector','benefits_numRatings','benefits_employerSummary',
                   'gaTrackerData_location','reviews']
    
    for columna in drop_column:
        drop_column_query = f"ALTER TABLE dataJobs_glassdoor DROP COLUMN IF EXISTS {columna};"
        cursor.execute(drop_column_query)
        print(f"Columna {columna} eliminada")

    conn.commit()
    cursor.close()
    conn.close()

def drop_apiColumn():

    conn = conn_postgresql()
    cursor = conn.cursor()

    delete_publisherLink = """
        ALTER TABLE datajobs_salaries
        DROP COLUMN publisher_link;
    """
    cursor.execute(delete_publisherLink)
    conn.commit()
    print("Column publisher_link dropped")
    cursor.close()
    conn.close()