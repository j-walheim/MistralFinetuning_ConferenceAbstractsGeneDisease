from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.ftp.hooks.ftp import FTPHook

from datetime import timedelta
import polars as pl
import pyarrow.parquet as pq
import os
import pyarrow as pa
from ftplib import FTP
import sys
sys.path.append('/teamspace/studios/this_studio/MistralFinetuning_ConferenceAbstractsGeneDisease/')

from config.pipeline import DEFAULT_ARGS, ENVIRONMENT,DISEASES_FTP_URL,DISEASES_FTP_DIR,  N_PARTITIONS_DEV, LITERATURE_FTP_URL, LITERATURE_FTP_DIR, STORAGE_DIR
from utils.pipeline.disease_processing import process_cancer_diseases
from utils.pipeline.literature_processing import process_literature_file, combine_literature_files
from utils.pipeline.ftp_helpers import get_ftp_file_list

MAX_ACTIVE_TASKS = 4

@dag(
    dag_id=f'opentargets_pipeline_dynamic_{ENVIRONMENT}',
    default_args=DEFAULT_ARGS,
    description=f'A DAG to process OpenTargets literature data ({ENVIRONMENT} environment)',
    schedule_interval=None,
    concurrency=MAX_ACTIVE_TASKS,
)
def ProcessOpenTargets():
    
    file_list = get_ftp_file_list()

    process_files = process_literature_file.expand(file_name=file_list)

    combined_literature = combine_literature_files(process_files)
    cancer_diseases = process_cancer_diseases()

    file_list >> process_files >> combined_literature >> cancer_diseases

dag = ProcessOpenTargets()


