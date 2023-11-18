# Funciones para la extraccion
import pandas as pd
import psycopg2
import json

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
    

def loadAndSave_data(file_path, output_filename):
    # Carga los datos desde un archivo CSV
    df = pd.read_csv(file_path, delimiter=',')
    # Guarda los datos en un archivo CSV
    df.to_csv(output_filename, index=False)
    print(f"CSV guardado como '{output_filename}' exitosamente.")

    return df

def create_and_populate_table(df):

    conn = conn_postgresql()
    cursor = conn.cursor()

    create_table = """
        CREATE TABLE IF NOT EXISTS dataJobs_glassdoor (
            id SERIAL PRIMARY KEY,
            benefits_benefitRatingDecimal FLOAT,
            benefits_comments FLOAT,
            benefits_highlights FLOAT,
            benefits_numRatings INTEGER,
            benefits_employerSummary VARCHAR,
            breadCrumbs INTEGER,
            gaTrackerData_category INTEGER,
            gaTrackerData_empId INTEGER,
            gaTrackerData_empName VARCHAR,
            gaTrackerData_empSize VARCHAR,
            gaTrackerData_expired BOOLEAN,
            gaTrackerData_industry VARCHAR,
            gaTrackerData_industryId INTEGER,
            gaTrackerData_jobId_long FLOAT,
            gaTrackerData_jobId_int FLOAT,
            gaTrackerData_jobTitle VARCHAR,
            gaTrackerData_location VARCHAR,
            gaTrackerData_locationId INTEGER,
            gaTrackerData_locationType VARCHAR,
            gaTrackerData_pageRequestGuid_guid VARCHAR,
            gaTrackerData_pageRequestGuid_guidValid BOOLEAN,
            gaTrackerData_pageRequestGuid_part1 BIGINT,
            gaTrackerData_pageRequestGuid_part2 BIGINT,
            gaTrackerData_sector VARCHAR,
            gaTrackerData_sectorId INTEGER,
            gaTrackerData_profileConversionTrackingParams_trackingCAT VARCHAR,
            gaTrackerData_profileConversionTrackingParams_trackingSRC FLOAT,
            gaTrackerData_profileConversionTrackingParams_trackingXSP FLOAT,
            gaTrackerData_jobViewTrackingResult_jobViewDisplayTimeMillis FLOAT,
            gaTrackerData_jobViewTrackingResult_requiresTracking VARCHAR,
            gaTrackerData_jobViewTrackingResult_trackingUrl VARCHAR,
            header_adOrderId INTEGER,
            header_advertiserType VARCHAR,
            header_applicationId INTEGER,
            header_applyButtonDisabled BOOLEAN,
            header_applyUrl VARCHAR,
            header_blur BOOLEAN,
            header_coverPhoto VARCHAR,
            header_easyApply BOOLEAN,
            header_employerId INTEGER,
            header_employerName VARCHAR,
            header_expired BOOLEAN,
            header_gocId INTEGER,
            header_hideCEOInfo BOOLEAN,
            header_jobTitle VARCHAR,
            header_locId INTEGER,
            header_location VARCHAR,
            header_locationType VARCHAR,
            header_logo VARCHAR,
            header_logo2x VARCHAR,
            header_organic BOOLEAN,
            header_overviewUrl VARCHAR,
            header_posted VARCHAR,
            header_rating FLOAT,
            header_saved BOOLEAN,
            header_savedJobId INTEGER,
            header_sgocId INTEGER,
            header_sponsored BOOLEAN,
            header_userAdmin BOOLEAN,
            header_uxApplyType VARCHAR,
            header_featuredVideo VARCHAR,
            header_normalizedJobTitle VARCHAR,
            header_urgencyLabel VARCHAR,
            header_urgencyLabelForMessage VARCHAR,
            header_urgencyMessage VARCHAR,
            header_needsCommission VARCHAR,
            header_payHigh FLOAT,
            header_payLow FLOAT,
            header_payMed FLOAT,
            header_payPeriod VARCHAR,
            header_salaryHigh FLOAT,
            header_salaryLow FLOAT,
            header_salarySource VARCHAR,
            job_description VARCHAR,
            job_discoverDate TIMESTAMP,
            job_eolHashCode INTEGER,
            job_importConfigId INTEGER,
            job_jobReqId_long FLOAT,
            job_jobReqId_int FLOAT,
            job_jobSource VARCHAR,
            job_jobTitleId INTEGER,
            job_listingId_long FLOAT,
            job_listingId_int FLOAT,
            map_country VARCHAR,
            map_employerName VARCHAR,
            map_lat FLOAT,
            map_lng FLOAT,
            map_location VARCHAR,
            map_address VARCHAR,
            map_postalCode VARCHAR,
            overview_allBenefitsLink VARCHAR,
            overview_allPhotosLink VARCHAR,
            overview_allReviewsLink VARCHAR,
            overview_allSalariesLink VARCHAR,
            overview_foundedYear INTEGER,
            overview_hq VARCHAR,
            overview_industry VARCHAR,
            overview_industryId INTEGER,
            overview_revenue VARCHAR,
            overview_sector VARCHAR,
            overview_sectorId INTEGER,
            overview_size VARCHAR,
            overview_stock VARCHAR,
            overview_type VARCHAR,
            overview_description VARCHAR,
            overview_mission VARCHAR,
            overview_website VARCHAR,
            overview_allVideosLink VARCHAR,
            overview_competitors FLOAT,
            overview_companyVideo VARCHAR,
            photos FLOAT,
            rating_ceo_name VARCHAR,
            rating_ceo_photo VARCHAR,
            rating_ceo_photo2x VARCHAR,
            rating_ceo_ratingsCount FLOAT,
            rating_ceoApproval FLOAT,
            rating_recommendToFriend FLOAT,
            rating_starRating FLOAT,
            reviews INTEGER,
            salary_country_cc3LetterISO VARCHAR,
            salary_country_ccISO VARCHAR,
            salary_country_continent_continentCode VARCHAR,
            salary_country_continent_continentName VARCHAR,
            salary_country_continent_id FLOAT,
            salary_country_continent_new VARCHAR,
            salary_country_countryFIPS VARCHAR,
            salary_country_currency_currencyCode VARCHAR,
            salary_country_currency_defaultFractionDigits FLOAT,
            salary_country_currency_displayName VARCHAR,
            salary_country_currency_id FLOAT,
            salary_country_currency_name VARCHAR,
            salary_country_currency_negativeTemplate VARCHAR,
            salary_country_currency_new VARCHAR,
            salary_country_currency_positiveTemplate VARCHAR,
            salary_country_currency_symbol VARCHAR,
            salary_country_currencyCode VARCHAR,
            salary_country_defaultLocale VARCHAR,
            salary_country_defaultName VARCHAR,
            salary_country_defaultShortName VARCHAR,
            salary_country_employerSolutionsCountry VARCHAR,
            salary_country_id FLOAT,
            salary_country_longName VARCHAR,
            salary_country_major VARCHAR,
            salary_country_name VARCHAR,
            salary_country_new VARCHAR,
            salary_country_population FLOAT,
            salary_country_shortName VARCHAR,
            salary_country_tld VARCHAR,
            salary_country_type VARCHAR,
            salary_country_uniqueName VARCHAR,
            salary_country_usaCentricDisplayName VARCHAR,
            salary_currency_currencyCode VARCHAR,
            salary_currency_defaultFractionDigits FLOAT,
            salary_currency_displayName VARCHAR,
            salary_currency_id FLOAT,
            salary_currency_name VARCHAR,
            salary_currency_negativeTemplate VARCHAR,
            salary_currency_new VARCHAR,
            salary_currency_positiveTemplate VARCHAR,
            salary_currency_symbol VARCHAR,
            salary_lastSalaryDate VARCHAR,
            salary_salaries FLOAT,
            wwfu FLOAT
        )
    """
    cursor.execute(create_table)
    conn.commit()
    print("Tabla creada")

    for index, row in df.iterrows():
        query = f"""INSERT INTO dataJobs_glassdoor ( benefits_benefitRatingDecimal, benefits_comments, benefits_highlights, 
            benefits_numRatings, benefits_employerSummary, breadCrumbs, gaTrackerData_category,
            gaTrackerData_empId, gaTrackerData_empName, gaTrackerData_empSize, gaTrackerData_expired,
            gaTrackerData_industry, gaTrackerData_industryId, gaTrackerData_jobId_long,
            gaTrackerData_jobId_int, gaTrackerData_jobTitle, gaTrackerData_location,
            gaTrackerData_locationId, gaTrackerData_locationType, gaTrackerData_pageRequestGuid_guid,
            gaTrackerData_pageRequestGuid_guidValid, gaTrackerData_pageRequestGuid_part1,
            gaTrackerData_pageRequestGuid_part2, gaTrackerData_sector, gaTrackerData_sectorId,
            gaTrackerData_profileConversionTrackingParams_trackingCAT,
            gaTrackerData_profileConversionTrackingParams_trackingSRC,
            gaTrackerData_profileConversionTrackingParams_trackingXSP,
            gaTrackerData_jobViewTrackingResult_jobViewDisplayTimeMillis,
            gaTrackerData_jobViewTrackingResult_requiresTracking,
            gaTrackerData_jobViewTrackingResult_trackingUrl, header_adOrderId,
            header_advertiserType, header_applicationId, header_applyButtonDisabled,
            header_applyUrl, header_blur, header_coverPhoto, header_easyApply,
            header_employerId, header_employerName, header_expired, header_gocId,
            header_hideCEOInfo, header_jobTitle, header_locId, header_location,
            header_locationType, header_logo, header_logo2x, header_organic,
            header_overviewUrl, header_posted, header_rating, header_saved,
            header_savedJobId, header_sgocId, header_sponsored, header_userAdmin,
            header_uxApplyType, header_featuredVideo, header_normalizedJobTitle,
            header_urgencyLabel, header_urgencyLabelForMessage, header_urgencyMessage,
            header_needsCommission, header_payHigh, header_payLow, header_payMed,
            header_payPeriod, header_salaryHigh, header_salaryLow, header_salarySource,
            job_description, job_discoverDate, job_eolHashCode, job_importConfigId,
            job_jobReqId_long, job_jobReqId_int, job_jobSource, job_jobTitleId,
            job_listingId_long, job_listingId_int, map_country, map_employerName,
            map_lat, map_lng, map_location, map_address, map_postalCode,
            overview_allBenefitsLink, overview_allPhotosLink, overview_allReviewsLink,
            overview_allSalariesLink, overview_foundedYear, overview_hq, overview_industry,
            overview_industryId, overview_revenue, overview_sector, overview_sectorId,
            overview_size, overview_stock, overview_type, overview_description,
            overview_mission, overview_website, overview_allVideosLink, overview_competitors,
            overview_companyVideo, photos, rating_ceo_name, rating_ceo_photo,
            rating_ceo_photo2x, rating_ceo_ratingsCount, rating_ceoApproval,
            rating_recommendToFriend, rating_starRating, reviews, salary_country_cc3LetterISO,
            salary_country_ccISO, salary_country_continent_continentCode,
            salary_country_continent_continentName, salary_country_continent_id,
            salary_country_continent_new, salary_country_countryFIPS,
            salary_country_currency_currencyCode, salary_country_currency_defaultFractionDigits,
            salary_country_currency_displayName, salary_country_currency_id,
            salary_country_currency_name, salary_country_currency_negativeTemplate,
            salary_country_currency_new, salary_country_currency_positiveTemplate,
            salary_country_currency_symbol, salary_country_currencyCode,
            salary_country_defaultLocale, salary_country_defaultName,
            salary_country_defaultShortName, salary_country_employerSolutionsCountry,
            salary_country_id, salary_country_longName, salary_country_major,
            salary_country_name, salary_country_new, salary_country_population,
            salary_country_shortName, salary_country_tld, salary_country_type,
            salary_country_uniqueName, salary_country_usaCentricDisplayName,
            salary_currency_currencyCode, salary_currency_defaultFractionDigits,
            salary_currency_displayName, salary_currency_id, salary_currency_name,
            salary_currency_negativeTemplate, salary_currency_new,
            salary_currency_positiveTemplate, salary_currency_symbol,
            salary_lastSalaryDate, salary_salaries, wwfu) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
             %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
             %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
             %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        cursor.execute(query, (row["benefits.benefitRatingDecimal"], row["benefits.comments"], row["benefits.highlights"], row["benefits.numRatings"], row["benefits.employerSummary"], row["breadCrumbs"], row["gaTrackerData.category"], row["gaTrackerData.empId"], row["gaTrackerData.empName"], row["gaTrackerData.empSize"],
                               row["gaTrackerData.expired"], row["gaTrackerData.industry"], row["gaTrackerData.industryId"], row["gaTrackerData.jobId.long"], row["gaTrackerData.jobId.int"], row["gaTrackerData.jobTitle"], row["gaTrackerData.location"], row["gaTrackerData.locationId"], row["gaTrackerData.locationType"], row["gaTrackerData.pageRequestGuid.guid"],
                               row["gaTrackerData.pageRequestGuid.guidValid"], row["gaTrackerData.pageRequestGuid.part1"], row["gaTrackerData.pageRequestGuid.part2"], row["gaTrackerData.sector"], row["gaTrackerData.sectorId"], row["gaTrackerData.profileConversionTrackingParams.trackingCAT"], row["gaTrackerData.profileConversionTrackingParams.trackingSRC"], row["gaTrackerData.profileConversionTrackingParams.trackingXSP"], row["gaTrackerData.jobViewTrackingResult.jobViewDisplayTimeMillis"], row["gaTrackerData.jobViewTrackingResult.requiresTracking"],
                               row["gaTrackerData.jobViewTrackingResult.trackingUrl"], row["header.adOrderId"], row["header.advertiserType"], row["header.applicationId"], row["header.applyButtonDisabled"], row["header.applyUrl"], row["header.blur"], row["header.coverPhoto"], row["header.easyApply"], row["header.employerId"],
                               row["header.employerName"], row["header.expired"], row["header.gocId"], row["header.hideCEOInfo"], row["header.jobTitle"], row["header.locId"], row["header.location"], row["header.locationType"], row["header.logo"], row["header.logo2x"],
                               row["header.organic"], row["header.overviewUrl"], row["header.posted"], row["header.rating"], row["header.saved"], row["header.savedJobId"], row["header.sgocId"], row["header.sponsored"], row["header.userAdmin"], row["header.uxApplyType"],
                               row["header.featuredVideo"], row["header.normalizedJobTitle"], row["header.urgencyLabel"], row["header.urgencyLabelForMessage"], row["header.urgencyMessage"], row["header.needsCommission"], row["header.payHigh"], row["header.payLow"], row["header.payMed"], row["header.payPeriod"],
                               row["header.salaryHigh"], row["header.salaryLow"], row["header.salarySource"], row["job.description"], row["job.discoverDate"], row["job.eolHashCode"], row["job.importConfigId"], row["job.jobReqId.long"], row["job.jobReqId.int"], row["job.jobSource"],
                               row["job.jobTitleId"], row["job.listingId.long"], row["job.listingId.int"], row["map.country"], row["map.employerName"], row["map.lat"], row["map.lng"], row["map.location"], row["map.address"], row["map.postalCode"],
                               row["overview.allBenefitsLink"], row["overview.allPhotosLink"], row["overview.allReviewsLink"], row["overview.allSalariesLink"], row["overview.foundedYear"], row["overview.hq"], row["overview.industry"], row["overview.industryId"], row["overview.revenue"], row["overview.sector"],
                               row["overview.sectorId"], row["overview.size"], row["overview.stock"], row["overview.type"], row["overview.description"], row["overview.mission"], row["overview.website"], row["overview.allVideosLink"], row["overview.competitors"], row["overview.companyVideo"],
                               row["photos"], row["rating.ceo.name"], row["rating.ceo.photo"], row["rating.ceo.photo2x"], row["rating.ceo.ratingsCount"], row["rating.ceoApproval"], row["rating.recommendToFriend"], row["rating.starRating"], row["reviews"], row["salary.country.cc3LetterISO"],
                               row["salary.country.ccISO"], row["salary.country.continent.continentCode"], row["salary.country.continent.continentName"], row["salary.country.continent.id"], row["salary.country.continent.new"], row["salary.country.countryFIPS"], row["salary.country.currency.currencyCode"], row["salary.country.currency.defaultFractionDigits"], row["salary.country.currency.displayName"], row["salary.country.currency.id"],
                               row["salary.country.currency.name"], row["salary.country.currency.negativeTemplate"], row["salary.country.currency.new"], row["salary.country.currency.positiveTemplate"], row["salary.country.currency.symbol"], row["salary.country.currencyCode"], row["salary.country.defaultLocale"], row["salary.country.defaultName"], row["salary.country.defaultShortName"], row["salary.country.employerSolutionsCountry"],
                               row["salary.country.id"], row["salary.country.longName"], row["salary.country.major"], row["salary.country.name"], row["salary.country.new"], row["salary.country.population"], row["salary.country.shortName"], row["salary.country.tld"], row["salary.country.type"], row["salary.country.uniqueName"],
                               row["salary.country.usaCentricDisplayName"], row["salary.currency.currencyCode"], row["salary.currency.defaultFractionDigits"], row["salary.currency.displayName"], row["salary.currency.id"], row["salary.currency.name"], row["salary.currency.negativeTemplate"], row["salary.currency.new"], row["salary.currency.positiveTemplate"], row["salary.currency.symbol"],
                               row["salary.lastSalaryDate"], row["salary.salaries"], row["wwfu"]))
    conn.commit()
    print("Datos agregados")
    cursor.close()
    conn.close()

def eliminar_columnas():

    conn = conn_postgresql()
    cursor = conn.cursor()

    columnas_eliminadas = [
        'breadCrumbs', 'gaTrackerData_category', 'gaTrackerData_empId',
        'gaTrackerData_empSize', 'gaTrackerData_expired', 'gaTrackerData_industryId',
        'gaTrackerData_jobId_long', 'gaTrackerData_jobId_int', 'gaTrackerData_locationId',
        'gaTrackerData_locationType', 'gaTrackerData_pageRequestGuid_guid',
        'gaTrackerData_pageRequestGuid_guidValid', 'gaTrackerData_pageRequestGuid_part1',
        'gaTrackerData_pageRequestGuid_part2', 'gaTrackerData_sectorId',
        'gaTrackerData_profileConversionTrackingParams_trackingCAT',
        'gaTrackerData_profileConversionTrackingParams_trackingSRC',
        'gaTrackerData_profileConversionTrackingParams_trackingXSP',
        'gaTrackerData_jobViewTrackingResult_jobViewDisplayTimeMillis',
        'gaTrackerData_jobViewTrackingResult_requiresTracking',
        'gaTrackerData_jobViewTrackingResult_trackingUrl', 'header_adOrderId',
        'header_advertiserType', 'header_applicationId', 'header_applyButtonDisabled',
        'header_applyUrl', 'header_blur', 'header_coverPhoto', 'header_easyApply',
        'header_employerId', 'header_expired', 'header_gocId', 'header_hideCEOInfo',
        'header_locId', 'header_locationType', 'header_logo', 'header_logo2x',
        'header_organic', 'header_overviewUrl', 'header_rating', 'header_saved',
        'header_savedJobId', 'header_sgocId', 'header_sponsored', 'header_userAdmin',
        'header_uxApplyType', 'header_featuredVideo', 'header_urgencyLabel',
        'header_urgencyLabelForMessage', 'header_urgencyMessage', 'header_needsCommission',
        'header_payHigh', 'header_payLow', 'header_payMed', 'header_payPeriod',
        'header_salaryHigh', 'header_salaryLow', 'header_salarySource', 'job_eolHashCode',
        'job_importConfigId', 'job_jobReqId_long', 'job_jobReqId_int', 'job_jobTitleId',
        'job_listingId_long', 'job_listingId_int', 'map_address', 'map_postalCode',
        'overview_allBenefitsLink', 'overview_allPhotosLink', 'overview_allReviewsLink',
        'overview_allSalariesLink', 'overview_industryId', 'overview_sectorId',
        'overview_stock', 'overview_mission', 'overview_allVideosLink',
        'overview_competitors', 'overview_companyVideo', 'photos', 'rating_ceo_photo',
        'rating_ceo_photo2x', 'rating_ceo_ratingsCount', 'rating_ceoApproval',
        'rating_recommendToFriend', 'salary_country_cc3LetterISO', 'salary_country_ccISO',
        'salary_country_continent_continentCode', 'salary_country_continent_continentName',
        'salary_country_continent_id', 'salary_country_continent_new', 'salary_country_countryFIPS',
        'salary_country_currency_currencyCode', 'salary_country_currency_defaultFractionDigits',
        'salary_country_currency_displayName', 'salary_country_currency_id',
        'salary_country_currency_name', 'salary_country_currency_negativeTemplate',
        'salary_country_currency_new', 'salary_country_currency_positiveTemplate',
        'salary_country_currency_symbol', 'salary_country_currencyCode',
        'salary_country_defaultLocale', 'salary_country_defaultShortName',
        'salary_country_employerSolutionsCountry', 'salary_country_id',
        'salary_country_longName', 'salary_country_major', 'salary_country_name',
        'salary_country_new', 'salary_country_population', 'salary_country_shortName',
        'salary_country_tld', 'salary_country_type', 'salary_country_uniqueName',
        'salary_country_usaCentricDisplayName', 'salary_currency_currencyCode',
        'salary_currency_defaultFractionDigits', 'salary_currency_displayName',
        'salary_currency_id', 'salary_currency_name', 'salary_currency_negativeTemplate',
        'salary_currency_new', 'salary_currency_positiveTemplate', 'salary_currency_symbol',
        'salary_lastSalaryDate', 'wwfu'
    ]

    for columna in columnas_eliminadas:
        drop_column_query = f"ALTER TABLE dataJobs_glassdoor DROP COLUMN IF EXISTS {columna};"
        cursor.execute(drop_column_query)
        print(f"Columna {columna} eliminada")

    conn.commit()
    cursor.close()
    conn.close()

def reemplazar_valores_nulos():

    conn = conn_postgresql()
    cursor = conn.cursor()

    update_query = """
        UPDATE dataJobs_glassdoor
        SET
            benefits_employerSummary = COALESCE(benefits_employerSummary, 'Without a summary of benefits'),
            gaTrackerData_location = COALESCE(gaTrackerData_location, 'NA'),
            header_normalizedJobTitle = COALESCE(header_normalizedJobTitle, 'Not normalized'),
            overview_description = COALESCE(overview_description, 'Without description'),
            overview_website = COALESCE(overview_website, 'No website'),
            rating_ceo_name = COALESCE(rating_ceo_name, 'No ceo name')
    """
    cursor.execute(update_query)
    conn.commit()
    print("Valores nulos reemplazados")
    cursor.close()
    conn.close()

def crear_columna_jobTitle_normalized():

    conn = conn_postgresql()
    cursor = conn.cursor()

    job_normalized_column = """
        ALTER TABLE IF EXISTS dataJobs_glassdoor
        ADD COLUMN IF NOT EXISTS jobTitle_normalized VARCHAR;
    """
    cursor.execute(job_normalized_column)
    conn.commit()
    print("Columna jobTitle_normalized agregada")
    cursor.close()
    conn.close()

def get_category_mapping():

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

    return category_mapping

def categorize_job_titles(df, category_mapping):
    # Categoriza los titulos de trabajo segun el mapeo
    df['jobTitle_normalized'] = ""

    for cat, titles in category_mapping.items():
        matching_rows = df['header.jobTitle'].str.contains('|'.join(titles), case=False)
        df.loc[matching_rows, 'jobTitle_normalized'] = cat
        print(f"Registrados {matching_rows.sum()} titulos en la categoria {cat}")

    other_rows = df['jobTitle_normalized'].isnull()
    df.loc[other_rows, 'jobTitle_normalized'] = "Other"

    print(f"Registrados {other_rows.sum()} titulos en la categoria Other")
    return df

def update_normalized_job_titles(df):

    conn = conn_postgresql()

    cursor = conn.cursor()
    for index, row in df.iterrows():
        header_jobtitle = row['jobTitle_normalized']
        update_query = f"""
            UPDATE dataJobs_glassdoor
            SET jobTitle_normalized = %s
            WHERE id = %s
        """
        cursor.execute(update_query, (header_jobtitle, index + 1))
        print(f"Actualizada fila {index + 1} en la base de datos")
    conn.commit()
    cursor.close()
    conn.close()

def api_extract():

    df = pd.read_csv("/home/vinke1302/Apache/csv/job_salaries.csv", encoding='ISO-8859-1', delimiter=',')

    conn = conn_postgresql()
    cursor = conn.cursor()

    create_table = """
        CREATE TABLE IF NOT EXISTS datajobs_salaries (
            location VARCHAR,
            job_title VARCHAR,
            publisher_name VARCHAR,
            publisher_link VARCHAR,
            min_salary FLOAT,
            max_Salary FLOAT,
            median_Salary FLOAT,
            salary_period VARCHAR,
            salary_currency VARCHAR
        )
    """
    cursor.execute(create_table)
    conn.commit()
    print("Tabla creada")

    for index, row in df.iterrows():
        query = f"""INSERT INTO datajobs_salaries (location, job_title, publisher_name,
        publisher_link, min_salary, max_salary, median_salary, salary_period, salary_currency) VALUES( %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        cursor.execute(query, (row["location"], row["job_title"], row["publisher_name"], row["publisher_link"],row["min_salary"], 
                               row["max_salary"], row["median_salary"], row["salary_period"], row["salary_currency"]))
    
    conn.commit()
    print("Values inserted")
    cursor.close()
    conn.close()


    
