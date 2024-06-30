# %%
import os
import pandas as pd
import json
import random
from mistralai.client import MistralClient
from mistralai.models.jobs import TrainingParameters
from mistralai.models.chat_completion import ChatMessage
from mistralai.models.jobs import WandbIntegrationIn, TrainingParameters

import numpy as np


os.environ["MISTRAL_API_KEY"] = open('../.keys/.key_mistral').read()
os.environ["WANDB_API_KEY"] = open('../.keys/.wandb').read()
random.seed(123)


with open("data/pubmed_qa.jsonl", "r") as f:
    data_pm = [json.loads(line) for line in f]
with open("data/arxiv_qa.jsonl", "r") as f:
    data_CS = [json.loads(line) for line in f]

# Combine the data and shuffle them randomly to have a mix of abstracts with and without gene-disease associations and 
# Todo: check if the mistral API does this anyway
data = data_pm + data_CS
del data_pm, data_CS
data = [data[i] for i in np.random.permutation(len(data))]



# %%

# take only a part of the data - using everyhing would cost 100s of dollars
#data = data[:len(data)//5]
data = data[:len(data)//10]

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

# Initialize Mistral client and Upload dataset
client = MistralClient(api_key=os.environ.get("MISTRAL_API_KEY"))

with open("data/train_data.jsonl", "rb") as f:
    train_file = client.files.create(file=("train_data.jsonl", f))

# %%

with open("data/val_data.jsonl", "rb") as f:
    val_file = client.files.create(file=("val_data.jsonl", f))



# %%
# Create fine-tuning job
created_job = client.jobs.create(
    model="open-mistral-7b",
    training_files=[train_file.id],
    validation_files=[val_file.id],
    hyperparameters=TrainingParameters(
        training_steps=30,
        learning_rate=1e-4,
    ),
    dry_run=False,
    integrations=[
        WandbIntegrationIn(
            project="test_api",
            run_name="test",
            api_key=os.environ.get("WANDB_API_KEY"),
        ).dict()
    ]
)
#object='job.metadata' training_steps=10 train_tokens_per_step=131072 data_tokens=77476943 train_tokens=1310720 epochs=0.0169 expected_duration_seconds=80


# object='job.metadata' training_steps=10 train_tokens_per_step=131072 data_tokens=77476943 train_tokens=1310720 epochs=0.0169 expected_duration_seconds=80

print(created_job)

# %%

# Monitor progress
retrieved_job = client.jobs.retrieve(created_job.id)
print(retrieved_job)
# %%print all models are ready
tmp = client.list_models()

# %%
