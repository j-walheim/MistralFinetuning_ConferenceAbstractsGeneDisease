from airflow import DAG
from airflow.decorators import task
import polars as pl
import pandas as pd
import numpy as np
from glob import glob
import os
from config.pipeline import STORAGE_DIR
from utils.pipeline.helpers import log_progress

@task
def get_cancer_disease_evidence( combined_literature_partitions_file, cancer_diseases_opentargets_file,**kwargs):
    
    ti= kwargs['ti']
    log_progress(ti, "Starting disease_evidence task")
    log_progress(ti, "Name of combined_literature_partitions_file: " + combined_literature_partitions_file)
    df_cancer_list = pl.scan_parquet(cancer_diseases_opentargets_file)
    
    df_disease_lit = pl.scan_parquet(combined_literature_partitions_file)
    
    df_disease_lit = df_disease_lit.filter(pl.col('type') == 'DS')
    
    df_disease_lit = (df_disease_lit
        .join(df_cancer_list, left_on='keywordId', right_on='id', how='left')
        .drop('keywordId')
        .drop_nulls(subset=['name'])
        .rename({'name': 'disease_name', 'text': 'text_sub'}))    
    
    df_disease_lit = (df_disease_lit
        .group_by('pmid')
        .agg([
            pl.col('disease_name').unique().cast(pl.Utf8).str.concat(', ').alias('disease_name')
        ])
    )
    
    out_path = os.path.join(STORAGE_DIR, 'disease_evidence.parquet')
    df_disease_lit.collect().write_parquet(out_path)
    log_progress(ti, f"Disease evidence saved to {out_path}")
    return out_path

@task
def get_pmid_cancer(disease_evidence_file,**kwargs):
    ti = kwargs['ti']
    log_progress(ti, "Starting pmid_cancer task")
    disease_evidence = pl.read_parquet(disease_evidence_file)
    df = pl.DataFrame({'pmid': disease_evidence['pmid'].unique()})
    
    out_path = os.path.join(STORAGE_DIR, 'pmid_cancer.parquet')
    df.write_parquet(out_path)
    log_progress(ti, f"PMID cancer data saved to {out_path}")
    return out_path

@task
def get_gene_evidence(combined_literature_partitions_file,cancer_evidence, hugo_symbols_file,**kwargs):
    ti = kwargs['ti']
    log_progress(ti, "Starting gene_evidence task")
    hugo_symbols = pl.scan_parquet(hugo_symbols_file)
    
    df_gene_lit = pl.scan_parquet(combined_literature_partitions_file)
    df_cancer_evidence = pl.scan_parquet(cancer_evidence)
    df_hugo = hugo_symbols.lazy()
    # Get unique pmids from cancer_evidence
    cancer_pmids = (pl.scan_parquet(cancer_evidence)
                    .select('pmid')
                    .unique()
                    .collect()
                    .to_series())

    
    n_in = df_gene_lit.select(pl.count()).collect().item()
    df_gene_lit = df_gene_lit.filter(pl.col('pmid').is_in(cancer_pmids))
    n_cancer = df_gene_lit.select(pl.count()).collect().item()
    log_progress(ti, f'Found {n_cancer}/{n_in} genes with cancer evidence')
    
    df_gene_lit = df_gene_lit.filter(pl.col('type') == 'GP')
    df_gene_lit = df_gene_lit.join(df_hugo, left_on='keywordId', right_on='ENSG', how='left').drop('keywordId')
    
    df_gene_lit = df_gene_lit.filter(pl.col('HUGO').is_not_null())
    df_gene_lit = df_gene_lit.rename({'HUGO': 'gene', 'text': 'text_sub'}).with_row_count()
    
    df_gene_lit = (
        df_gene_lit.group_by('pmid')
        .agg(
            pl.col('gene').unique().alias('unique_genes')
        )
        .with_columns(
            pl.col('unique_genes').cast(pl.List(pl.Utf8)).list.join(', ').alias('gene')
        )
        .drop('unique_genes')
    )
    
    out_path = os.path.join(STORAGE_DIR, 'gene_evidence.parquet')
    df_gene_lit.collect().write_parquet(out_path)
    log_progress(ti, f"Gene evidence saved to {out_path}")
    return out_path

@task
def get_organism(combined_literature_partitions_file,cancer_evidence, **kwargs):
    ti = kwargs['ti']
    log_progress(ti, "Starting organism task")
    
    # Get unique pmids from cancer_evidence
    cancer_pmids = (pl.scan_parquet(cancer_evidence)
                    .select('pmid')
                    .unique()
                    .collect())

    
    # Use the unique pmids to filter df_organisms
    df_organisms = (pl.scan_parquet(combined_literature_partitions_file)
                    .filter(pl.col('pmid').is_in(cancer_pmids['pmid']))
                    .select(['pmid', 'organisms'])
                    .unique())
    
    out_path = os.path.join(STORAGE_DIR, 'organism.parquet')
    df_organisms.collect().write_parquet(out_path)
    log_progress(ti, f"Organism data saved to {out_path}")
    return out_path

@task
def combine_evidence(gene_evidence_file, disease_evidence_file, organism_file, **kwargs):
    ti = kwargs['ti']
    log_progress(ti, "Starting combine_evidence task")
    gene_evidence = pl.read_parquet(gene_evidence_file)
    disease_evidence = pl.read_parquet(disease_evidence_file)
    organism = pl.read_parquet(organism_file)

    gene_evidence = gene_evidence.join(
        disease_evidence,
        on='pmid',
        how='left',
        suffix='_disease'
    )
    gene_evidence = gene_evidence.join(organism, on='pmid', how='left')

#    cancer_pmids = pmid_cancer['pmid'].to_list()
#    gene_evidence = gene_evidence.filter(pl.col('pmid').is_in(cancer_pmids))

    missing_genes_count = gene_evidence.filter(pl.col('gene').is_null()).shape[0]
    if missing_genes_count > 0:
        log_progress(ti, f'Found {missing_genes_count} missing genes')
        
    out_path = os.path.join(STORAGE_DIR, 'gene_disease_combined.parquet')
    gene_evidence.write_parquet(out_path)
    log_progress(ti, f"Gene disease combined data saved to {out_path}")
    return out_path

@task
def subsample_data(gene_disease_combined_file, **kwargs):
    ti = kwargs['ti']
    log_progress(ti, "Starting data_subsampled task")
    gene_disease_combined = pl.read_parquet(gene_disease_combined_file)
    
    NUM_ABSTRACTS_SUB = 20000  # take only a subset of abstracts
    NUM_DISEASE_MAX = 2000  # upper limit to get more equal representation in training data
    random_state = 123

    gene_disease_combined = gene_disease_combined.explode('organisms')        
    gene_disease_combined = gene_disease_combined.filter(
        pl.col('organisms').cast(pl.Utf8).str.to_lowercase().is_in(['human', 'humans', 'woman', 'man']))
    gene_disease_combined = gene_disease_combined.drop('organisms').unique()
    
    disease_counts = gene_disease_combined['disease_name'].value_counts().head(20)
    log_progress(ti, f"Top 20 disease counts:\n{disease_counts}")

    gene_disease_combined = gene_disease_combined.sample(fraction=1.0, seed=random_state)
    gene_disease_combined = gene_disease_combined.group_by('disease_name').head(int(NUM_DISEASE_MAX))
    
    fraction = NUM_ABSTRACTS_SUB / len(gene_disease_combined)

    def stratified_sample(group):
        n = int(len(group) * fraction)
        return group.sample(n, seed=random_state)

    gene_disease_sub = gene_disease_combined.groupby('disease_name').map_groups(stratified_sample)

    n_additional = NUM_ABSTRACTS_SUB - len(gene_disease_sub)
    
    if n_additional > 0:
        gene_disease_sub = pl.concat([
            gene_disease_sub,
            gene_disease_combined.filter(~pl.col('pmid').is_in(gene_disease_sub.get_column('pmid')))
                                    .head(int(n_additional))
        ])

    gene_disease_sub = gene_disease_sub.sample(fraction=1.0, seed=random_state)
    
    log_progress(ti, f'Sampled {len(gene_disease_sub)} rows out of {len(gene_disease_combined)} rows')
    log_progress(ti, f"Top 20 sampled disease counts:\n{gene_disease_sub['disease_name'].value_counts().head(20)}")

    out_path = os.path.join(STORAGE_DIR, 'data_subsampled.parquet')
    gene_disease_sub.write_parquet(out_path)
    log_progress(ti, f"Subsampled data saved to {out_path}")
    return out_path

