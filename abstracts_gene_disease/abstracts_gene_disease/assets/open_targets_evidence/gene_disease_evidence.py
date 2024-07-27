from dagster import asset, AssetExecutionContext, StaticPartitionsDefinition, AllPartitionMapping, AssetIn
from ftplib import FTP
import polars as pl
import pandas as pd
from glob import glob
from collections import defaultdict
from .literature_opentargets import raw_opentargets_literature, combined_partitions_literature
#from .diseases_opentargets import cancer_diseases_opentargets
import polars as pl
import pandas as pd
from dagster import asset, AssetExecutionContext, AssetIn, AllPartitionMapping

@asset(
    deps=['raw_opentargets_literature', 'combined_partitions_literature'],
)
def disease_evidence(
    context: AssetExecutionContext,
    cancer_diseases_opentargets: pl.DataFrame
) -> pl.DataFrame:
    # Convert pandas DataFrame to Polars LazyFrame
    df_disease_list = pl.LazyFrame(cancer_diseases_opentargets)
    
    combined_partitions_file = '../.dagster_home/storage/raw_opentargets_literature.parquet'
    df_disease_lit = pl.scan_parquet(combined_partitions_file)
    
    df_disease_lit = df_disease_lit.filter(pl.col('type') == 'DS')
    
    df_disease_lit = (df_disease_lit
        .join(df_disease_list, left_on='keywordId', right_on='id', how='left')
        .drop('keywordId')
        .drop_nulls(subset=['name'])
        .rename({'name': 'disease_name', 'text': 'text_sub'}))    
    # Corrected aggregation
    df_disease_lit = (df_disease_lit
        .groupby('pmid')
        .agg([
            pl.col('disease_name').unique().cast(pl.Utf8).str.concat(', ').alias('disease_name')#,
          #  pl.col('text_sub').cast(pl.Utf8).str.concat(', ').alias('text_sub')
        ])
    )
    
    # Collect the result
    return df_disease_lit.collect()


import polars as pl
from dagster import asset, AssetExecutionContext
@asset(io_manager_key="fs_io_manager")
def pmid_cancer(context: AssetExecutionContext, disease_evidence: pl.DataFrame) -> pl.DataFrame:
    # create a dataframe with the unique PMIDs
    df = pl.DataFrame({'pmid': disease_evidence['pmid'].unique()})
    
    # add head of df to metadata
    context.add_output_metadata(
        {"head": df.head().to_dict(as_series=False)}
    )
    
    return df

@asset(io_manager_key="fs_io_manager", deps=['combined_partitions_literature'])
def gene_evidence(context: AssetExecutionContext, hugo_symbols: pl.DataFrame, pmid_cancer: pl.DataFrame) -> pl.DataFrame:
    combined_partitions_file = '../.dagster_home/storage/raw_opentargets_literature.parquet'
    df_gene_lit = pl.scan_parquet(combined_partitions_file)
    df_hugo = hugo_symbols.lazy()
    cancer_pmids = pmid_cancer['pmid'].to_list()
    
    n_in = df_gene_lit.select(pl.count()).collect().item()
    df_gene_lit = df_gene_lit.filter(pl.col('pmid').is_in(cancer_pmids))
    n_cancer = df_gene_lit.select(pl.count()).collect().item()
    context.log.info(f'Found {n_cancer}/{n_in} genes with cancer evidence')
    
    df_gene_lit = df_gene_lit.filter(pl.col('type') == 'GP')
    df_gene_lit = df_gene_lit.join(df_hugo, left_on='keywordId', right_on='ENSG', how='left').drop('keywordId')
    
    HUGO_na = df_gene_lit.filter(pl.col('HUGO').is_null())
    df_gene_lit = df_gene_lit.filter(pl.col('HUGO').is_not_null())
    df_gene_lit = df_gene_lit.rename({'HUGO': 'gene', 'text': 'text_sub'}).with_row_count()
    
    if HUGO_na.select(pl.count()).collect().item() > 0:
        df_NAs = HUGO_na.collect()
        # Logging code here if needed
    
    df_gene_lit = (
        df_gene_lit.groupby('pmid')
        .agg(
            pl.col('gene').unique().alias('unique_genes')#,
#            pl.col('text_sub').cast(pl.Utf8).str.concat(', ').alias('text_sub')
        )
        .with_columns(
            pl.col('unique_genes').cast(pl.List(pl.Utf8)).list.join(', ').alias('gene')
        )
        .drop('unique_genes')
    )
    
    return df_gene_lit.collect()


@asset(
    deps=['raw_opentargets_literature', 'combined_partitions_literature'],
)
def organism(
    context: AssetExecutionContext,pmid_cancer: pl.DataFrame
) -> pl.DataFrame:
        
    combined_partitions_file = '../.dagster_home/storage/raw_opentargets_literature.parquet'
    df_organisms = pl.scan_parquet(combined_partitions_file)    
    
    cancer_pmids = pmid_cancer['pmid'].to_list()
    df_organisms = df_organisms.filter(pl.col('pmid').is_in(cancer_pmids))
    
    # select pmid and organisms
    df_organisms = df_organisms.select(['pmid', 'organisms']).unique()
    
    # Collect the result
    return df_organisms




@asset
def gene_disease_combined(context: AssetExecutionContext, gene_evidence: pl.DataFrame,
                          disease_evidence: pl.DataFrame, organism: pl.DataFrame,
                          pmid_cancer: pl.DataFrame) -> pl.DataFrame:
    # Merge gene_evidence and disease_evidence
    gene_evidence = gene_evidence.join(
        disease_evidence,
        on='pmid',
        how='left',
        suffix='_disease'
    )
    gene_evidence = gene_evidence.join(organism, on='pmid', how='left')

    # Filter gene_evidence based on cancer_pmids
    cancer_pmids = pmid_cancer['pmid'].to_list()
    gene_evidence = gene_evidence.filter(pl.col('pmid').is_in(cancer_pmids))

    # Check for missing genes
    missing_genes_count = gene_evidence.filter(pl.col('gene').is_null()).shape[0]
    if missing_genes_count > 0:
        context.log.info(f'Found {missing_genes_count} missing genes')
        
        missing_genes_pmids = (
            gene_evidence
            .filter(pl.col('gene').is_null())
            .group_by('pmid')
            .agg(pl.count('pmid').alias('count'))
            .sort('count', descending=True)
        )
        context.log.info(missing_genes_pmids.to_markdown())
        
    return gene_evidence.collect()


