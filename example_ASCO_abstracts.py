# %%
import csv
import json
import pandas as pd
from mistralai.client import MistralClient
from mistralai.models.chat_completion import ChatMessage
import json
import csv
import sys
from tqdm import tqdm
import os
import pickle
import requests

os.environ["MISTRAL_API_KEY"] = open('../.keys/.key_mistral').read()

# %% get conference abstracts

pdf_path = "data/AM24-Abstracts.pdf"
    
if not os.path.exists("data/AM24-Abstracts.pdf"):
    
    url = "https://s3.amazonaws.com/files.oncologymeetings.org/prod/s3fs-public/2024-05/AM24-Abstracts.pdf"
    os.makedirs(os.path.dirname(pdf_path), exist_ok=True)
    response = requests.get(url)
    response.raise_for_status()

    with open(pdf_path, 'wb') as file:
        file.write(response.content)

    print(f"Abstracts downloaded to {pdf_path}")
else:
    print("Abstracts already downloaded.")

# %%
from utils.ASCO_Abstract_parser import extract_abstracts

abstracts_df = extract_abstracts(pdf_path)
if not os.path.exists("data/extracted_abstracts.csv"):
    if abstracts_df is not None:    
        abstracts_df.to_csv("data/extracted_abstracts.csv", index=False)
    else:
        print("Failed to extract abstracts.")
else:
    print("Abstracts already extracted.")
    abstracts_df = pd.read_csv("data/extracted_abstracts.csv")

# %%
# Initialize the client
client = MistralClient(api_key=os.environ["MISTRAL_API_KEY"])
client.list_models()

#model = 'open-mistral-7b'
model = 'ft:open-mistral-7b:b22eb6cb:20240630:fff68fc7'

model_supp = model.replace(':', '_')
# %%

with open('prompts/prompt_finetuning.txt', 'r') as f:
    prompt = f.read()

abstracts_file = 'data/extracted_abstracts.csv'
out_file_json = f'results/abstracts_features_{model_supp}.json'
out_file_csv = f'results/abstracts_features_{model_supp}.csv'

# List to store the results
results = []

# Open the input CSV file
with open(abstracts_file, "r") as csv_file:
    reader = csv.DictReader(csv_file)

    # Iterate through the rows in the input CSV file
    iteration_counter = 0
    
    
    for row in tqdm(reader, desc="Processing rows"):
        iteration_counter += 1
        if iteration_counter >= 100:
            break

        prompt_cur = prompt
        prompt_cur = prompt_cur.replace('[[[abstract]]]', row['Abstract'])

        try:
            # Query the model and store the response
            chat_response = client.chat(
                model=model,
                messages=[ChatMessage(role="user", content=prompt_cur)]
            )
            row['response'] = chat_response.choices[0].message.content
        except Exception as e:
            print(f"Error querying model: {e}")
            row['response'] = "Error querying model"

        results.append(row)
                
#export results as pickle - sometimes json parsing fails, don't want to lose all results
with open('results/tmp.pkl', 'w') as f:
    json.dump(results, f, indent=4)
# %%
results_json = []
for row in tqdm(results, desc="produce JSON"):
    try:
        response_json = json.loads(row['response'])
        response_json['abstract'] = row['Abstract'] 
        results_json.append(response_json)
    except Exception as e:
        print(f"Error parsing JSON: {e}")

# %% export results
with open(out_file_json, "w") as json_file:
    json.dump(results, json_file, indent=4)


