from airflow.decorators import task
from config.pipeline import DEFAULT_ARGS, ENVIRONMENT,DISEASES_FTP_URL,DISEASES_FTP_DIR,  N_PARTITIONS_DEV, LITERATURE_FTP_URL, LITERATURE_FTP_DIR, STORAGE_DIR
import polars as pl
import pyarrow.parquet as pq
import os
import pyarrow as pa
from ftplib import FTP
from utils.pipeline.helpers import log_progress

import pandas as pd
import io

@task
def process_cancer_diseases(**kwargs):
    ti = kwargs['ti']
    log_progress(ti, "Processing cancer diseases")
    
        
    disease = []
    with FTP(DISEASES_FTP_URL) as ftp:
        ftp.login()
        ftp.cwd(DISEASES_FTP_DIR)
        
        parquet_files = [file for file in ftp.nlst() if file.endswith('.parquet')]
        
        for file in parquet_files:
            log_progress(ti, f"Processing {file}")
            with io.BytesIO() as buffer:
                ftp.retrbinary(f'RETR {file}', buffer.write)
                buffer.seek(0)
                data_cur = pd.read_parquet(buffer)
                
                data_cur = data_cur[['id', 'name', 'therapeuticAreas']]
                data_cur = data_cur.explode('therapeuticAreas')
                data_cur = data_cur[data_cur['therapeuticAreas'] == 'MONDO_0045024']
                data_cur = data_cur.drop('therapeuticAreas', axis=1)
                
                disease.append(data_cur)
    
    # write to temporary csv
    disease = pd.concat(disease)
    
    disease = disease[disease['id'].str.contains('EFO')]
    # drop abstract categories
    disease = disease[~disease['name'].isin(['cancer', 'neoplasm', 'cancer', 'carcinoma', 'cirrhosis of liver'])]

    log_progress(ti,f"Found {disease.shape[0]} cancer diseases")
    log_progress(ti,disease.head())
            
            
    output_file = os.path.join(STORAGE_DIR, 'cancer_diseases.parquet')
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    disease.to_parquet(output_file)
    return output_file
    