from airflow.decorators import task
from config.pipeline import DEFAULT_ARGS, ENVIRONMENT,DISEASES_FTP_URL,DISEASES_FTP_DIR,  N_PARTITIONS_DEV, LITERATURE_FTP_URL, LITERATURE_FTP_DIR, STORAGE_DIR
import polars as pl
import pyarrow.parquet as pq
import os
import pyarrow as pa
from ftplib import FTP
<<<<<<<< HEAD:utils/pipeline/disease_processing.py
from utils.pipeline.helpers import log_progress

import pandas as pd
import io

@task
def get_cancer_diseases(**kwargs):
    ti = kwargs['ti']
    log_progress(ti, "Processing cancer diseases")
    
        
    disease = []
    with FTP(DISEASES_FTP_URL) as ftp:
        ftp.login()
        ftp.cwd(DISEASES_FTP_DIR)
        
        parquet_files = [file for file in ftp.nlst() if file.endswith('.parquet')]
========
from dagster import asset, AssetExecutionContext
import pandas as pd
import io


@asset
def cancer_diseases_opentargets(context: AssetExecutionContext,) -> pd.DataFrame:
    
    url = "ftp.ebi.ac.uk"
    directory = "pub/databases/opentargets/platform/22.04/output/etl/parquet/diseases/"
    
    with FTP(url) as ftp:
        ftp.login()
        ftp.cwd(directory)
        parquet_files = [file for file in ftp.nlst() if file.endswith('.parquet')]
            
    disease = []
    with FTP(url) as ftp:
        ftp.login()
        ftp.cwd(directory)
>>>>>>>> main:abstracts_gene_disease/abstracts_gene_disease/assets/open_targets_evidence/diseases_opentargets.py
        
        for file in parquet_files:
            context.log.info(f"Processing {file}")
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

<<<<<<<< HEAD:utils/pipeline/disease_processing.py
    log_progress(ti,f"Found {disease.shape[0]} cancer diseases")
    log_progress(ti,disease.head())
            
            
    output_file = os.path.join(STORAGE_DIR, 'cancer_diseases.parquet')
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    disease.to_parquet(output_file)
    return output_file
    
========
    context.log.info(f"Found {disease.shape[0]} cancer diseases")
    context.log.info(disease.head())
            
    return disease

>>>>>>>> main:abstracts_gene_disease/abstracts_gene_disease/assets/open_targets_evidence/diseases_opentargets.py
