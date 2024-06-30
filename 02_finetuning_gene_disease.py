# %%
import os
import pandas as pd
import json
import random
from datasets import load_dataset
from mistralai.client import MistralClient
from mistralai.models.jobs import TrainingParameters
from mistralai.models.chat_completion import ChatMessage
from mistralai.models.jobs import WandbIntegrationIn, TrainingParameters
from datasets import load_dataset

import numpy as np
from huggingface_hub import login


os.environ["HUGGINGFACE_TOKEN"] = open('../.keys/.hf').read()
os.environ["MISTRAL_API_KEY"] = open('../.keys/.key_mistral').read()
os.environ["WANDB_API_KEY"] = open('../.keys/.wandb').read()
random.seed(123)
# Define the dataset name
dataset_name = "opentargets_abstracts_gene_disease"

login(token=os.environ.get("HUGGINGFACE_TOKEN"))

ds = load_dataset("jowah/opentargets_abstracts_gene_disease")

# write ds['train'] and ds['validation'] to jsonl files

with open("data/.tmp_train_data.jsonl", "w") as f:
    for item in ds["train"]:
        json.dump(item, f)
        f.write("\n")

with open("data/.tmp_val_data.jsonl", "w") as f:
    for item in ds["validation"]:
        json.dump(item, f)
        f.write("\n")

# %%
dataset_name
# Upload data and Initialize Mistral client
client = MistralClient(api_key=os.environ.get("MISTRAL_API_KEY"))

with open("data/.tmp_train_data.jsonl", "rb") as f:
    train_file = client.files.create(file=("train_data.jsonl", f))

# %%

with open("data/.tmp_val_data.jsonl", "rb") as f:
    val_file = client.files.create(file=("val_data.jsonl", f))



# %%
# Create fine-tuning job




created_job = client.jobs.create(
    model="open-mistral-7b",
    training_files=[train_file.id],
    validation_files=[val_file.id],
    hyperparameters=TrainingParameters(
        training_steps=55,
        learning_rate=1e-4,
    ),
    dry_run=False,
    integrations=[
        WandbIntegrationIn(
            project="mistral_finetuning",
            run_name="with_synthetic",
            api_key=os.environ.get("WANDB_API_KEY"),
        ).dict()
    ]
)

print(created_job)

# %%

# Monitor progress
retrieved_job = client.jobs.retrieve(created_job.id)
print(retrieved_job)
# %%print all models are ready
client.list_models()

# %%
