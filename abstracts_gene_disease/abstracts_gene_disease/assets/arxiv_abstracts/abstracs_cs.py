import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
from dagster import asset, AssetExecutionContext, StaticPartitionsDefinition
import polars as pl
import time

BATCH_SIZE = 100
NUM_PARTITIONS = 200
SLEEP_BETWEEN_BATCHES = 3

arxiv_partitions = StaticPartitionsDefinition([str(i) for i in range(200)])

def fetch_cs_abstracts(start_index, max_results):
    base_url = 'http://export.arxiv.org/api/query?'
    query_params = {
        'search_query': 'cat:cs.AI',
        'start': start_index,
        'max_results': max_results,
        'sortBy': 'lastUpdatedDate',
        'sortOrder': 'descending'
    }
    query_url = base_url + urllib.parse.urlencode(query_params)
    
    with urllib.request.urlopen(query_url) as url:
        response = url.read()
    
    root = ET.fromstring(response)
    namespace = {'atom': 'http://www.w3.org/2005/Atom'}
    
    abstracts = []
    for entry in root.findall('atom:entry', namespace):
        title = entry.find('atom:title', namespace).text
        abstract = entry.find('atom:summary', namespace).text
        abstracts.append({
            'title': title,
            'abstract': abstract        })
    
    return abstracts

@asset(partitions_def=arxiv_partitions)
def arxiv_abstracts(context: AssetExecutionContext) -> pl.DataFrame:
    partition_key = context.partition_key
    partition_index = int(partition_key.split("_")[1])
    start_index = partition_index * BATCH_SIZE
    
    context.log.info(f"Fetching abstracts for partition {partition_key}, starting from index {start_index}")
    
    abstracts = fetch_cs_abstracts(start_index=start_index, max_results=BATCH_SIZE)
    
    if not abstracts:
        context.log.warning(f"No abstracts found for partition {partition_key}")
        return pl.DataFrame()
    
    df = pl.DataFrame(abstracts)
    
    # Add empty columns for disease_name and gene
    df = df.with_columns([
        pl.lit('n/a').alias('disease_name'),
        pl.lit('n/a').alias('gene')
    ])
    
    
    context.add_output_metadata({
        "num_records": len(df),
        "partition": partition_key,
    })
    
    context.log.info(f"Successfully fetched {len(df)} records for partition {partition_key}")
    context.log.info(f"Sample data for partition {partition_key}:")
    context.log.info(df.head(5))
    
    time.sleep(SLEEP_BETWEEN_BATCHES)
    
    return df