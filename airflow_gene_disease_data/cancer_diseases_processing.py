from ftplib import FTP
import pandas as pd
import io
import os
from airflow_gene_disease_data.config import DISEASES_FTP_URL, DISEASES_FTP_DIR, STORAGE_DIR
from airflow_gene_disease_data.utils import ensure_dir, log_progress

def process_cancer_diseases(**kwargs):
    ti = kwargs['ti']
    log_progress(ti, "Starting cancer diseases processing")
    
    with FTP(DISEASES_FTP_URL) as ftp:
        ftp.login()
        ftp.cwd(DISEASES_FTP_DIR)
        parquet_files = [file for file in ftp.nlst() if file.endswith('.parquet')]
        disease = []
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
    
    disease = pd.concat(disease)
    disease = disease[disease['id'].str.contains('EFO')]
    disease = disease[~disease['name'].isin(['cancer', 'neoplasm', 'cancer', 'carcinoma', 'cirrhosis of liver'])]
    
    log_progress(ti, f"Found {disease.shape[0]} cancer diseases")
    
    output_file = os.path.join(STORAGE_DIR, 'cancer_diseases_opentargets.parquet')
    ensure_dir(os.path.dirname(output_file))
    disease.to_parquet(output_file)
    
    log_progress(ti, "Finished cancer diseases processing")
    return output_file