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

import numpy as np


os.environ["HUGGINGFACE_TOKEN"] = open('../.keys/.hf').read().strip()
os.environ["MISTRAL_API_KEY"] = open('../.keys/.key_mistral').read()
os.environ["WANDB_API_KEY"] = open('../.keys/.wandb').read()
random.seed(123)

# Define the dataset name
dataset_name = "opentargets_abstracts_gene_disease"

# Load the dataset
dataset = load_dataset(dataset_name, use_auth_token=True)


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
