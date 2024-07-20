from airflow.decorators import task
from config.pipeline import DEFAULT_ARGS, ENVIRONMENT,DISEASES_FTP_URL,DISEASES_FTP_DIR,  N_PARTITIONS_DEV, LITERATURE_FTP_URL, LITERATURE_FTP_DIR, STORAGE_DIR
import polars as pl
import pyarrow.parquet as pq
import os
import pyarrow as pa
from ftplib import FTP
from utils.pipeline.helpers import log_progress



def filter_opentargets_literature(df: pl.DataFrame) -> pl.DataFrame:
    
    # keep only rows that took evidence from the abstract, that contain disease or gene evidence and only keep relevant rows
    if 'section' in df.columns:
        df = df.filter(pl.col('section') == 'abstract')
    return df.filter(pl.col('type').is_in(['DS', 'GP'])).select(['pmid', 'text', 'keywordId', 'type', 'organisms'])


@task
def process_literature_file(file_name, **kwargs):
    ti = kwargs['ti']
    dir_raw = os.path.join(STORAGE_DIR, 'raw_opentargets_literature')
    os.makedirs(dir_raw, exist_ok=True)
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
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    combined_df.write_parquet(output_file)
    
    log_progress(ti, "Finished combining literature files")
    return output_file
