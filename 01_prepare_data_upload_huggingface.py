from datasets import load_dataset
from huggingface_hub import HfApi, DatasetCard
import os
import json
import numpy as np
import pandas as pd 
import subprocess
from utils.generate_synthetic_abstracts import generate_synthetic_abstracts, createPromptsJsonl


#
if not os.path.exists("data"):
    os.makedirs("data", exist_ok=True)
if not os.path.exists('results'):
    os.makedirs('results', exist_ok=True)
    

# %% create synthetic dataset
fname_synthetic = "data/synthetic_abstracts.csv"
fname_synthetic_json = 'data/synthetic_qa.jsonl'
df_synth = generate_synthetic_abstracts(dir_out = fname_synthetic, target_rows=10)

df = pd.read_csv(fname_synthetic)
df_formatted = createPromptsJsonl(df,fname_synthetic_json)

# %%

if not os.path.exists("data_raw/pubmed_qa.jsonl"):
    print('Error: The required file "pubmed_qa.jsonl" was not found in the "data" directory.')
    print('Please ensure that you have run the Dagster pipeline to generate this file.')
    print('If you have not run the pipeline, please do so before proceeding.')
    print('Alternatively, you can use the prepared data on Huggingface by running "2_finetuning_gene_disease.py" instead.')
    exit()
    
with open('data/pubmed_qa.jsonl', "r") as f:
    data_pm = [json.loads(line) for line in f]
    # take only a part of the data - using everyhing would cost 100s of dollars
    data_pm = data_pm[:8000]
with open("data/synthetic_qa.jsonl", "r") as f:
    data_synthetic = [json.loads(line) for line in f]
    data_synthetic = data_synthetic[:4000]

# Combine the data and shuffle them randomly to have a mix of abstracts with and without gene-disease associations  
# Todo: check if the mistral API does this anyway
data = data_pm + data_synthetic
del data_pm, data_synthetic
data = [data[i] for i in np.random.permutation(len(data))]


# %% Train-test split

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
