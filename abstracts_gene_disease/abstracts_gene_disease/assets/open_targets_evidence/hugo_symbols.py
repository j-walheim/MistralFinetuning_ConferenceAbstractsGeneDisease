from biomart import BiomartServer
import pandas as pd
from dagster import asset
import os

import polars as pl
import os
from biomart import BiomartServer

@asset(name="hugo_symbols")  # group_name = 'ingestion', description = 'Download mapping ENSG -> HUGO symbols'
def get_hugo_symbols_df() -> pl.DataFrame:
    # Connect to the Ensembl Biomart server
    server = BiomartServer("http://www.ensembl.org/biomart")
    # Select the dataset
    dataset = server.datasets['hsapiens_gene_ensembl']
    # Define the attributes you want to retrieve
    attributes = ['ensembl_gene_id', 'external_gene_name']
    # Perform the query
    response = dataset.search({'attributes': attributes})
    
    # Process the results
    ensg_list = []
    hugo_list = []
    for line in response.iter_lines():
        line = line.decode('utf-8').strip()
        if not line:  # Skip empty lines
            continue
        parts = line.split('\t')
        if len(parts) == 2:
            ensg, hugo = parts
            if hugo:  # Only add non-empty Hugo symbols
                ensg_list.append(ensg)
                hugo_list.append(hugo)
    
    # Create a DataFrame using Polars
    df = pl.DataFrame({
        'ENSG': ensg_list,
        'HUGO': hugo_list
    })
        
    return df


