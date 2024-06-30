from dagster import asset, AssetExecutionContext, StaticPartitionsDefinition,AssetIn,AllPartitionMapping
from ftplib import FTP
import polars as pl
import io
import gc
import os 

def filter_opentargets_literature(df: pl.DataFrame) -> pl.DataFrame:
    # Filter out non-abstracts if 'section' column exists
    if 'section' in df.columns:
        df = df.filter(pl.col('section') == 'abstract')
    
    # Filter type to be 'DS' or 'GP' and select only the required columns
    return df.filter(pl.col('type').is_in(['DS', 'GP'])).select(['pmid', 'text', 'keywordId', 'type', 'organisms'])

# Create a static partition definition for numbers 0 through 199
number_partitions = StaticPartitionsDefinition([str(i) for i in range(200)])

@asset(
    name="raw_opentargets_literature",
    partitions_def=number_partitions#,
#    io_manager_key="polars_parquet_io_manager"
)
def raw_opentargets_literature(context: AssetExecutionContext) -> str:
    url = "ftp.ebi.ac.uk"
    directory = "/pub/databases/opentargets/platform/22.04/output/literature-etl/parquet/matches/"
    partition_number = context.partition_key
    file_name = f"part-00{str(partition_number).zfill(3)}-b0cd91a5-00e9-43af-9e6e-8f735d297686-c000.snappy.parquet"
    
    context.log.info(f"Downloading {file_name}")
    
    dir_raw = os.path.join(os.getenv("DAGSTER_HOME", ""),'/storage/raw_opentargets_literature')
    fname = os.path.join(dir_raw, f'{partition_number}.parquet')
    
    with FTP(url) as ftp:
        ftp.login()
        ftp.cwd(directory)
        
        # Use BytesIO as a buffer to store the downloaded file
        with io.BytesIO() as buffer:
            ftp.retrbinary('RETR ' + file_name, buffer.write)
            buffer.seek(0)  # Reset buffer position to the beginning
            
            context.log.info(f"Processing {file_name}")
            
            # Read the Parquet file using pyarrow - polars has some problems with the parquet format
            table = pq.read_table(buffer)
            
            # Convert to Polars DataFrame
            df = pl.from_arrow(table)
            
            # Apply filtering
            df = filter_opentargets_literature(df)
    
    gc.collect()
    return fname



@asset(ins={"raw_opentargets_literature": AssetIn(partition_mapping=AllPartitionMapping())})
def combined_partitions_literature(context: AssetExecutionContext,raw_opentargets_literature: dict):# -> pl.DataFrame:
    return None
#     context.log.info(parquet_files)
#     parquet_files = raw_opentargets_literature
# #    parquet_files = glob(f"{dir_raw}/*.parquet")
#     context.log.info(f"Processing {len(parquet_files)} files")
#     context.log.info(f"Parquet files: {parquet_files}")
    
#     lazy_dfs = []
#     for f in parquet_files:
#         context.log.info(f"Processing {f}")
#         try:
#             lazy_df = pl.scan_parquet(f)
#             lazy_dfs.append(lazy_df)
#         except Exception as e:
#             context.log.error(f"Error processing {f}: {str(e)}")
#             raise
    
#     try:
#         combined_lazy = pl.concat(lazy_dfs)
        
#         return combined_lazy.collect()
#     except Exception as e:
#         context.log.error(f"Error during concatenation or collection: {str(e)}")
#         raise