from datasets import load_dataset
from huggingface_hub import HfApi, DatasetCard
import os
import json
import numpy as np

os.environ["HUGGINGFACE_TOKEN"] = open('../.keys/.hf').read().strip()


with open("data/pubmed_qa.jsonl", "r") as f:
    data_pm = [json.loads(line) for line in f]
with open("data/synthetic.jsonl", "r") as f:
    data_synthetic = [json.loads(line) for line in f]

# Combine the data and shuffle them randomly to have a mix of abstracts with and without gene-disease associations and 
# Todo: check if the mistral API does this anyway
data = data_pm + data_CS
del data_pm, data_CS
data = [data[i] for i in np.random.permutation(len(data))]


# %%

# take only a part of the data - using everyhing would cost 100s of dollars
data = data[:20E3]

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
os.system("python utils/reformat_data.py data/train_data.jsonl")
os.system("python utils/reformat_data.py data/val_data.jsonl")



# %%

# Load your local JSONL files
dataset = load_dataset('json', data_files={
    'train': 'data/train_data.jsonl',
    'validation': 'data/train_data.jsonl'
})

# The name you want to give your dataset
dataset_name = "opentargets_abstracts_gene_disease"

# Push the dataset to the Hugging Face Hub
dataset.push_to_hub(dataset_name, token=os.environ["HUGGINGFACE_TOKEN"])

print(f"Dataset uploaded successfully to https://huggingface.co/datasets/{dataset_name}")