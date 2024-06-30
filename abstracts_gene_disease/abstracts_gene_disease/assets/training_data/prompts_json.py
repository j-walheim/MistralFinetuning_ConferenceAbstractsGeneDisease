from dagster import asset, AssetExecutionContext
import pandas as pd
import json
import csv
import pickle
import os

from dagster import asset, AllPartitionMapping, AssetIn  

import json
import pandas as pd

def createPromptsJsonl(df, fname_out):
    prompt = open('../prompts/prompt_finetuning.txt', 'r').read()

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
    with open('../data/' + fname_out + '.jsonl', 'w') as f:
        for item in df_formatted:
            json.dump(item, f)
            f.write('\n')

    return df_formatted


@asset(ins={"data_with_pubmed": AssetIn(partition_mapping=AllPartitionMapping())})
def prompts_jsonl_pubmed(context: AssetExecutionContext, data_with_pubmed: dict) -> None:

    # Todo: move code to polars
    df = pd.concat([df.to_pandas() for df in data_with_pubmed.values()], ignore_index=True)
    createPromptsJsonl(df,'pubmed_qa')
    
@asset(ins={"arxiv_abstracts": AssetIn(partition_mapping=AllPartitionMapping())})
def prompts_jsonl_arxiv(context: AssetExecutionContext, arxiv_abstracts: dict) -> None:

    # Todo: move code to polars
    df = pd.concat([df.to_pandas() for df in arxiv_abstracts.values()], ignore_index=True)
    createPromptsJsonl(df,'arxiv_qa')