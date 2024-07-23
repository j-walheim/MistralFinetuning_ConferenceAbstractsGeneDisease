# File: utils/pipeline/pubmed_abstracts.py

from airflow.decorators import task
import polars as pl
from typing import List, Dict
from Bio import Entrez
import time
from airflow.models import Variable
import os
import math

# Set up Entrez email
Entrez.email = Variable.get("EMAIL", default_var="your.email@example.com")

BATCH_SIZE = 100

@task
def generate_batch_indices(data_path, **kwargs):
    df = pl.read_parquet(data_path)
    num_batches = math.ceil(len(df) / BATCH_SIZE)
    return [{'data_path': data_path, 'batch_index': i} for i in range(num_batches)]

@task
def process_pubmed_batch(data_path: str, batch_index: int, **kwargs):
    df = pl.read_parquet(data_path)
    start_index = batch_index * BATCH_SIZE
    end_index = min(start_index + BATCH_SIZE, len(df))
    pmid_batch = df['pmid'][start_index:end_index].to_list()
    print(f"Processing batch {batch_index} with {len(pmid_batch)} PMIDs")
    
    results = {}
    sleep_between_batches = 1
    
    handle = Entrez.efetch(db="pubmed", id=pmid_batch, retmode="xml")
    records = Entrez.read(handle, validate=False)
    handle.close()
    
    for record in records['PubmedArticle']:
        pmid = record['MedlineCitation']['PMID']
        article = record['MedlineCitation']['Article']
        title = article['ArticleTitle']
        abstract = article.get('Abstract', {}).get('AbstractText', ["No abstract available"])
        abstract = ' '.join(abstract) if isinstance(abstract, list) else abstract
        results[pmid] = {'title': title, 'abstract': abstract}
    
    print(f"Successfully fetched {len(results)} records for batch {batch_index}")
    time.sleep(sleep_between_batches)  # Be nice to NCBI servers
    
    # Create pubmed_partitions subfolder if it doesn't exist
    output_dir = os.path.join(os.path.dirname(data_path), "pubmed_partitions")
    os.makedirs(output_dir, exist_ok=True)
    
    # Save results to a parquet file in the pubmed_partitions subfolder
    output_path = os.path.join(output_dir, f"pubmed_batch_{batch_index}.parquet")
    pl.DataFrame([{"pmid": pmid, **data} for pmid, data in results.items()]).write_parquet(output_path)
    
    return output_path

@task
def combine_pubmed_results(data_path: str, batch_result_paths: List[str], **kwargs):
    batch_result_paths = [path for path in batch_result_paths if path is not None]

    df = pl.read_parquet(data_path)
    pubmed_data = pl.concat([pl.read_parquet(path) for path in batch_result_paths])
    df = df.join(pubmed_data, on='pmid', how='left')
    final_path = data_path.replace(".parquet", "_with_pubmed.parquet")
    df.write_parquet(final_path)
    return final_path