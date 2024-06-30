from datasets import load_dataset
from huggingface_hub import HfApi, DatasetCard
import os
import json
import numpy as np
import pandas as pd 
import subprocess

# %%

def createPromptsJsonl(df, fname_out):
    prompt = open('./prompts/prompt_finetuning.txt', 'r').read()

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
    with open(fname_out + '.jsonl', 'w') as f:
        for item in df_formatted:
            json.dump(item, f)
            f.write('\n')

    return df_formatted

if not os.path.exists('data/synthetic_qa.jsonl'):
    df = pd.read_csv('data/synthetic_diagnostic_abstracts.csv')

    # rename disease to disease_name and genes to gene_name
    df = df.rename(columns={'disease': 'disease_name', 'genes': 'gene'})

    df_formatted = createPromptsJsonl(df,'data/synthetic_qa')

# %%

os.environ["HUGGINGFACE_TOKEN"] = open('../.keys/.hf').read().strip()


with open("data/pubmed_qa.jsonl", "r") as f:
    data_pm = [json.loads(line) for line in f]
    # take only a part of the data - using everyhing would cost 100s of dollars
    data_pm = data_pm[:8000]
with open("data/synthetic_qa.jsonl", "r") as f:
    data_synthetic = [json.loads(line) for line in f]
    data_synthetic = data_synthetic[:4000]

# Combine the data and shuffle them randomly to have a mix of abstracts with and without gene-disease associations and 
# Todo: check if the mistral API does this anyway
data = data_pm + data_synthetic
del data_pm, data_synthetic
data = [data[i] for i in np.random.permutation(len(data))]


# %%




split_train = int(0.95 * len(data))

train_data = data[:split_train]
val_data = data[split_train:]


os.makedirs('data', exist_ok=True)

with open("data/train_data.jsonl", "w") as f:
    for entry in train_data:
        json.dump(entry, f)
        f.write("\n")

with open("data/val_data.jsonl", "w") as f:
    for entry in val_data:
        json.dump(entry, f)
        f.write("\n")


# %%

# run shell command
def run_command(command):
    try:
        # Run the command and wait for it to complete
        subprocess.run(command, check=True, shell=True)
        print(f"Command executed successfully: {command}")
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {command}")
        print(f"Error details: {e}")

# Run the commands
run_command("python utils/reformat_data.py data/train_data.jsonl")
run_command("python utils/reformat_data.py data/val_data.jsonl")



# %%

# Load your local JSONL files
dataset = load_dataset('json', data_files={
    'train': 'data/train_data.jsonl',
    'validation': 'data/val_data.jsonl'
})

# The name you want to give your dataset
dataset_name = "opentargets_abstracts_gene_disease"

# Push the dataset to the Hugging Face Hub
dataset.push_to_hub(dataset_name, token=os.environ["HUGGINGFACE_TOKEN"])

print(f"Dataset uploaded successfully to https://huggingface.co/datasets/{dataset_name}")
# %%
