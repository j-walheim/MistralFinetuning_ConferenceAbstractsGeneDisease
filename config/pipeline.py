from datetime import datetime, timedelta
import os

# Determine the environment
#ENVIRONMENT = 'development' 
ENVIRONMENT = 'production' #os.getenv('AIRFLOW_ENVIRONMENT', 'development').lower()

# Airflow DAG configuration
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}

# Number of partitions for literature processing
#NUM_PARTITIONS = 200 if ENVIRONMENT == 'production' else 2
N_PARTITIONS_DEV = 5
# FTP configuration
LITERATURE_FTP_URL = "ftp.ebi.ac.uk"
LITERATURE_FTP_DIR = "/pub/databases/opentargets/platform/22.04/output/literature-etl/parquet/matches/"

DISEASES_FTP_URL = "ftp.ebi.ac.uk"
DISEASES_FTP_DIR = "pub/databases/opentargets/platform/22.04/output/etl/parquet/diseases/"

# Storage configuration
#STORAGE_DIR = os.path.join(os.getenv("AIRFLOW_HOME", ""), 'storage')
STORAGE_DIR = '/teamspace/studios/this_studio/MistralFinetuning_ConferenceAbstractsGeneDisease/data_raw'


# You can add more environment-specific configurations here
if ENVIRONMENT == 'production':
    # Production-specific settings
    EMAIL_ON_FAILURE = True
    EMAIL_ON_RETRY = True
else:
    # Development-specific settings
    EMAIL_ON_FAILURE = False
    EMAIL_ON_RETRY = False

# Update DEFAULT_ARGS with environment-specific settings
DEFAULT_ARGS.update({
    'email_on_failure': EMAIL_ON_FAILURE,
    'email_on_retry': EMAIL_ON_RETRY,
})