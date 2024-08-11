from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.ftp.hooks.ftp import FTPHook
from airflow.models.xcom_arg import XComArg

from datetime import timedelta
import polars as pl
import pyarrow.parquet as pq
import os
import pyarrow as pa
from ftplib import FTP
import sys
sys.path.append('/teamspace/studios/this_studio/MistralFinetuning_ConferenceAbstractsGeneDisease/')

from config.pipeline import DEFAULT_ARGS, ENVIRONMENT,DISEASES_FTP_URL,DISEASES_FTP_DIR,  N_PARTITIONS_DEV, LITERATURE_FTP_URL, LITERATURE_FTP_DIR, STORAGE_DIR
from utils.pipeline.disease_processing import get_cancer_diseases
from utils.pipeline.literature_processing import process_literature_file, combine_literature_files
from utils.pipeline.ftp_helpers import get_ftp_file_list
from utils.pipeline.hugo_symbols import get_hugo_symbols_df
from utils.pipeline.combine_gene_disease_evidence import get_cancer_disease_evidence, get_gene_evidence, get_organism, combine_evidence, subsample_data
from utils.pipeline.pubmed_abstracts import process_pubmed_batch, combine_pubmed_results, generate_batch_indices
from utils.pipeline.training_data import createPromptsJsonl
MAX_ACTIVE_TASKS = 4

@dag(
    dag_id=f'opentargets_pipeline_dynamic_{ENVIRONMENT}',
    default_args=DEFAULT_ARGS,
    description=f'A DAG to process OpenTargets literature data ({ENVIRONMENT} environment)',
    schedule_interval=None,
    concurrency=MAX_ACTIVE_TASKS,
)
def ProcessOpenTargets():
    
    file_list = get_ftp_file_list()

    process_files = process_literature_file.expand(file_name=file_list)

    combined_literature = combine_literature_files(process_files)
    cancer_diseases = get_cancer_diseases()

    # # Integrate the functions from combine_gene_disease_evidence.py
    cancer_evidence = get_cancer_disease_evidence(combined_literature, cancer_diseases)
    
#    pmid_cancer_task = get_pmid_cancer(disease_evidence_task)
    
    # # Assuming we have a task that generates hugo_symbols_file
    hugo_symbols = get_hugo_symbols_df()
    
    gene_evidence = get_gene_evidence(combined_literature,cancer_evidence, hugo_symbols)
    
    organism = get_organism(combined_literature, cancer_evidence)
    
    gene_disease_combined = combine_evidence(
        gene_evidence,
        cancer_evidence,
        organism
    )
    
    data_sub = subsample_data(gene_disease_combined)


    # Generate batch indices
    batch_params = generate_batch_indices(data_sub)

    # Process Pubmed batches
    processed_pubmed_batches = process_pubmed_batch.expand(
        data_path=batch_params.map(lambda x: x['data_path']),
        batch_index=batch_params.map(lambda x: x['batch_index'])
    )

    # Combine results
    pubmed_final_result = combine_pubmed_results(
        data_path=data_sub,
        batch_result_paths=processed_pubmed_batches
    )
    prompts_jsonl = createPromptsJsonl(pubmed_final_result)

    file_list >> process_files >> combined_literature
    [combined_literature, cancer_diseases] >> cancer_evidence
    [combined_literature, cancer_evidence, hugo_symbols] >> gene_evidence
    [combined_literature, cancer_evidence] >> organism
    [gene_evidence, cancer_evidence, organism] >> gene_disease_combined >> data_sub >> batch_params
    batch_params >> processed_pubmed_batches >> pubmed_final_result




dag = ProcessOpenTargets()


