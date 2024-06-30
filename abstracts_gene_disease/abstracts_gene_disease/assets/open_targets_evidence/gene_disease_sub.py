from dagster import asset, AssetExecutionContext
import polars as pl
import os
from glob import glob
NUM_ABSTRACTS_SUB=20E3 # take only a subset of abstracts - keep price for finetuning low
NUM_DISEASE_MAX=2E3 # very different numbers for different indications - put upper limit to get more equal representation in training dataimport polars as pl
import numpy as np

from .gene_disease_evidence import gene_disease_combined

@asset()
def data_subsampled(context: AssetExecutionContext, gene_disease_combined: pl.DataFrame) -> pl.DataFrame:

        random_state = 123
        # keep only human organism
        gene_disease_combined = gene_disease_combined.explode('organisms')        
        gene_disease_combined = gene_disease_combined.filter(
            pl.col('organisms').cast(pl.Utf8).str.to_lowercase().is_in(['human', 'humans', 'woman', 'man']))
        gene_disease_combined = gene_disease_combined.drop('organisms').unique()
        
        # Log the value counts of disease_name
        disease_counts = gene_disease_combined['disease_name']\
                .value_counts().to_pandas().reset_index()
        disease_counts = disease_counts.sort_values(disease_counts.columns[1], ascending=False) \
                .head(20)   
        # Print the table
        context.log.info(disease_counts.to_markdown())


        # Shuffle the DataFrame
        gene_disease_combined = gene_disease_combined.sample(fraction=1.0, seed=random_state)

        # Limit max number of entries per disease
        gene_disease_combined = gene_disease_combined.groupby('disease_name').head(int(NUM_DISEASE_MAX))

                
        # Calculate the fraction to sample (adjust this value as needed)
        fraction = NUM_ABSTRACTS_SUB / len(gene_disease_combined)

        #disease_counts = np.ceil(gene_disease_combined['disease_name'].value_counts() * fraction)

        # Perform stratified sampling
        def stratified_sample(group):
                n = int(len(group) * fraction)
                return group.sample(n, seed=random_state)

        gene_disease_sub = gene_disease_combined.groupby('disease_name').map_groups(stratified_sample)

        # we have to sample integers which means that we will have less rows than NUM_ABSTRACTS_SUB - add some more
        n_additional = NUM_ABSTRACTS_SUB - len(gene_disease_sub)
        
        if(n_additional > 0):
                gene_disease_sub = pl.concat([
                        gene_disease_sub,
                        gene_disease_combined.filter(~pl.col('pmid').is_in(gene_disease_sub.get_column('pmid')))
                                                .head(int(n_additional))
                        ])

        # shuffle data - otherwise they will be ordered by disease
        gene_disease_sub = gene_disease_sub.sample(fraction=1.0, seed=random_state)
        
        context.log.info(f'Sampled {len(gene_disease_sub)} rows out of {len(gene_disease_combined)} rows')

        # Log the value counts of disease_name
        context.log.info(gene_disease_sub['disease_name'].value_counts().head(20).to_dict())

        return gene_disease_sub