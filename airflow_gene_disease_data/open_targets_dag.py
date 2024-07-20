from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.ftp.hooks.ftp import FTPHook
from airflow_gene_disease_data.config import DEFAULT_ARGS, ENVIRONMENT,DISEASES_FTP_URL,DISEASES_FTP_DIR,  N_PARTITIONS_DEV, LITERATURE_FTP_URL, LITERATURE_FTP_DIR, STORAGE_DIR
from datetime import timedelta
import polars as pl
import pyarrow.parquet as pq
import os
import pyarrow as pa
from ftplib import FTP

# Todo: drop pandas and io
import pandas as pd
import io


MAX_ACTIVE_TASKS = 4

def ensure_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

def log_progress(ti, message):
    ti.xcom_push(key='progress_log', value=message)
    print(message)

def filter_opentargets_literature(df: pl.DataFrame) -> pl.DataFrame:
    if 'section' in df.columns:
        df = df.filter(pl.col('section') == 'abstract')
    return df.filter(pl.col('type').is_in(['DS', 'GP'])).select(['pmid', 'text', 'keywordId', 'type', 'organisms'])

@dag(
    dag_id=f'opentargets_pipeline_dynamic_{ENVIRONMENT}',
    default_args=DEFAULT_ARGS,
    description=f'A DAG to process OpenTargets literature data ({ENVIRONMENT} environment)',
    schedule_interval=None,
    concurrency=MAX_ACTIVE_TASKS,
)
def ProcessOpenTargets():
    
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

    @task
    def process_literature_file(file_name, **kwargs):
        ti = kwargs['ti']
        dir_raw = os.path.join(STORAGE_DIR, 'raw_opentargets_literature')
        ensure_dir(dir_raw)
        fname = os.path.join(dir_raw, file_name)
        if not os.path.exists(fname):
            temp_fname = fname + '.tmp'
            log_progress(ti, f"Processing literature file {file_name}")

            with FTP(LITERATURE_FTP_URL) as ftp:
                ftp.login()
                ftp.cwd(LITERATURE_FTP_DIR)
                with open(temp_fname, 'wb') as temp_file:
                    ftp.retrbinary(f'RETR {file_name}', temp_file.write)

            log_progress(ti, f"Download complete, processing {file_name}")

            reader = pq.ParquetFile(temp_fname)
            num_row_groups = reader.num_row_groups

            first_table = reader.read_row_group(0)
            first_df = pl.from_arrow(first_table)
            filtered_df = filter_opentargets_literature(first_df)
            output_schema = pa.Schema.from_pandas(filtered_df.to_pandas())

            with pq.ParquetWriter(fname, schema=output_schema) as writer:
                writer.write_table(pa.Table.from_pandas(filtered_df.to_pandas()))
                del filtered_df, first_df

                for i in range(1, num_row_groups):
                    table = reader.read_row_group(i)
                    df = pl.from_arrow(table)
                    filtered_df = filter_opentargets_literature(df)
                    writer.write_table(pa.Table.from_pandas(filtered_df.to_pandas()))
                    del df, filtered_df
                    log_progress(ti, f"Processed chunk {i+1}/{num_row_groups} of file {file_name}")

            os.remove(temp_fname)
            log_progress(ti, f"Finished processing literature file {file_name}, output written to {fname}")
        else:
            log_progress(ti, f"Skipping {file_name} as it already exists in {fname}")

        return fname

    @task
    def combine_literature_files(file_list, **kwargs):
        ti = kwargs['ti']
        log_progress(ti, f"Starting to combine literature files. Retrieved {len(file_list) if file_list else 0} files.")
        
        valid_files = [file for file in file_list if file and os.path.exists(file)]
        log_progress(ti, f"Found {len(valid_files)} valid files out of {len(file_list)} total files.")
        
        if not valid_files:
            raise ValueError("No valid files found to combine.")
        
        lazy_dfs = [pl.scan_parquet(file) for file in valid_files]
        combined_df = pl.concat(lazy_dfs).collect()
        
        output_file = os.path.join(STORAGE_DIR, 'combined_opentargets_literature.parquet')
        ensure_dir(os.path.dirname(output_file))
        combined_df.write_parquet(output_file)
        
        log_progress(ti, "Finished combining literature files")
        return output_file

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
        ensure_dir(os.path.dirname(output_file))
        disease.to_parquet(output_file)
        return output_file
        
        

    file_list = get_ftp_file_list()

    process_files = process_literature_file.expand(file_name=file_list)

    combined_literature = combine_literature_files(process_files)
    cancer_diseases = process_cancer_diseases()

    file_list >> process_files >> combined_literature >> cancer_diseases

dag = ProcessOpenTargets()


