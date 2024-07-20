from airflow.decorators import task
from ftplib import FTP
from airflow_gene_disease_data.config import LITERATURE_FTP_URL, LITERATURE_FTP_DIR, ENVIRONMENT, N_PARTITIONS_DEV
from .helpers import log_progress

@task
def get_ftp_file_list(**kwargs):
    ti = kwargs['ti']
    with FTP(LITERATURE_FTP_URL) as ftp:
        ftp.login()
        ftp.cwd(LITERATURE_FTP_DIR)
        files = ftp.nlst()
    # filter for files that end with .parquet
    files = [file for file in files if file.endswith('.parquet')]
    
    log_progress(ti, f"Found {len(files)} files in FTP directory")
    
    
    # if we are not in production environment, only use a subset of files
    if ENVIRONMENT != 'production':
        files = files[:N_PARTITIONS_DEV]
        log_progress(ti, f"Using only {len(files)} files in development environment")
    return files
