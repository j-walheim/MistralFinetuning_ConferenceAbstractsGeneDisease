#from abstracts_gene_disease.abstracts_gene_disease.assets.training_data import createPromptsJsonl
import json
import pandas as pd


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
    with open('./data/' + fname_out + '.jsonl', 'w') as f:
        for item in df_formatted:
            json.dump(item, f)
            f.write('\n')

    # If you want to print the result
    print(json.dumps(df_formatted, indent=2))

    return df_formatted

df = pd.read_csv('data/synthetic_diagnostic_abstracts.csv')

# rename disease to disease_name and genes to gene_name
df = df.rename(columns={'disease': 'disease_name', 'genes': 'gene'})

createPromptsJsonl(df,'synthetic_qa')