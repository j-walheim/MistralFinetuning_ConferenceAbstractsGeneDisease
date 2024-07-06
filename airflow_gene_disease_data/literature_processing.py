from ftplib import FTP
import polars as pl
import pyarrow.parquet as pq
import io
import os
from airflow_gene_disease_data.config import LITERATURE_FTP_URL, LITERATURE_FTP_DIR, STORAGE_DIR
from airflow_gene_disease_data.utils import ensure_dir, log_progress

def filter_opentargets_literature(df: pl.DataFrame) -> pl.DataFrame:
    if 'section' in df.columns:
        df = df.filter(pl.col('section') == 'abstract')
    return df.filter(pl.col('type').is_in(['DS', 'GP'])).select(['pmid', 'text', 'keywordId', 'type', 'organisms'])

def process_literature_partition(partition_number, **kwargs):
    ti = kwargs['ti']
    file_name = f"part-00{str(partition_number).zfill(3)}-b0cd91a5-00e9-43af-9e6e-8f735d297686-c000.snappy.parquet"
    
    dir_raw = os.path.join(STORAGE_DIR, 'raw_opentargets_literature')
    os.makedirs(dir_raw, exist_ok=True)
    ensure_dir(dir_raw)
    fname = os.path.join(dir_raw, f'{partition_number}.parquet')
    
    log_progress(ti, f"Processing literature partition {partition_number}")
    
    with FTP(LITERATURE_FTP_URL) as ftp:
        ftp.login()
        ftp.cwd(LITERATURE_FTP_DIR)
        
        with io.BytesIO() as buffer:
            ftp.retrbinary('RETR ' + file_name, buffer.write)
            buffer.seek(0)
            
            table = pq.read_table(buffer)
            df = pl.from_arrow(table)
            df = filter_opentargets_literature(df)

    log_progress(ti, f"Finished processing literature partition {partition_number}, writing output to {fname}")    
    df.write_parquet(fname)

    return fname

def combine_literature_partitions(**kwargs):
    ti = kwargs['ti']
    partition_files = ti.xcom_pull(task_ids='process_literature_partition_*')
    
    log_progress(ti, "Starting to combine literature partitions")
    
    lazy_dfs = [pl.scan_parquet(file) for file in partition_files if file]
    combined_df = pl.concat(lazy_dfs).collect()
    
    output_file = os.path.join(STORAGE_DIR, 'combined_opentargets_literature.parquet')
    ensure_dir(os.path.dirname(output_file))
    combined_df.write_parquet(output_file)
    
    log_progress(ti, "Finished combining literature partitions")
    return output_file