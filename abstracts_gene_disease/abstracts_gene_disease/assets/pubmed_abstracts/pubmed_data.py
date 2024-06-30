from dagster import asset, AssetExecutionContext, StaticPartitionsDefinition
from Bio import Entrez
import polars as pl
import os
import time

Entrez.email = os.getenv("EMAIL", "your.email@example.com")

BATCH_SIZE = 1000
NUM_PARTITIONS = 100

# Create a StaticPartitionsDefinition
pubmed_partitions = StaticPartitionsDefinition([f"partition_{i}" for i in range(NUM_PARTITIONS)])

@asset(partitions_def=pubmed_partitions)
def data_with_pubmed(context: AssetExecutionContext, data_subsampled: pl.DataFrame) -> pl.DataFrame:
    # Get the current partition key
    partition_key = context.partition_key
    partition_index = int(partition_key.split("_")[1])
    
    # Calculate the range for this partition
    start_index = partition_index * BATCH_SIZE
    end_index = start_index + BATCH_SIZE
    
    if start_index >= len(data_subsampled):
        context.log.info(f"Skipping partition {partition_key} as it is empty")
        return pl.DataFrame()
    
    if end_index > len(data_subsampled):
        end_index = len(data_subsampled)
    
    # Get the PMIDs for this partition
    pmid_list = data_subsampled['pmid'].to_list()[start_index:end_index]
    data_subsampled = data_subsampled.filter(pl.col('pmid').is_in(pmid_list))
    context.log.info(f"Processing partition {partition_key} with {len(pmid_list)} PMIDs")
    
    results = []
    sleep_between_batches = 1
    
    handle = Entrez.efetch(db="pubmed", id=pmid_list, retmode="xml")
    records = Entrez.read(handle, validate=False)
    handle.close()
    
    for record in records['PubmedArticle']:
        pmid = record['MedlineCitation']['PMID']
        article = record['MedlineCitation']['Article']
        title = article['ArticleTitle']
        abstract = article.get('Abstract', {}).get('AbstractText', ["No abstract available"])
        abstract = ' '.join(abstract) if isinstance(abstract, list) else abstract
        results.append({'pmid': pmid, 'title': title, 'abstract': abstract})
    
    context.log.info(f"Successfully fetched {len(results)} records for partition {partition_key}")
    time.sleep(sleep_between_batches)  # Be nice to NCBI servers
    
    df = pl.DataFrame(results)
    data_abstracts = data_subsampled.join(df, on='pmid', how='left')
    context.log.info(f"Successfully merged {len(df)} records for partition {partition_key}")
    context.log.info(data_abstracts.head(5))
    
    # Add some metadata
    context.add_output_metadata({
        "num_records": len(data_abstracts),
        "partition": partition_key,
    })
    
    return data_abstracts
