from airflow.decorators import task
import pandas as pd
import json
import csv
import pickle
import os
from config.pipeline import STORAGE_DIR


@task
def createPromptsJsonl(fname_abstract_evidence,**kwargs):
    prompt = open('prompts/prompt_finetuning.txt', 'r').read()

    df = pd.read_parquet(fname_abstract_evidence )
    # Create the formatted list of dictionaries
    df_formatted = []
    for index, row in df.iterrows():
        # Skip empty abstracts
        if pd.isna(row['abstract']) or row['abstract'].strip() == '':
            continue

        # Replace missing genes/diseases with 'n/a'
        disease = row['disease_name'] if pd.notna(row['disease_name']) else 'n/a'
        gene = row['gene'] if pd.notna(row['gene']) else 'n/a'

        formatted_item = {
            "messages": [
                {"role": "user", "content": prompt.replace('[[[abstract]]]', row['abstract'])},
                {"role": "assistant", "content": json.dumps({
                    "Disease": disease,
                    "Genes": gene
                })}
            ]
        }
        df_formatted.append(formatted_item)

    # Write to jsonl
    out_path = os.path.join(STORAGE_DIR, 'abstracts_prompts.jsonl')
    with open(out_path, 'w') as f:
        for item in df_formatted:
            json.dump(item, f)
            f.write('\n')

    return df_formatted
