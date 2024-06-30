import os
from mistralai.client import MistralClient
from mistralai.models.chat_completion import ChatMessage
import pandas as pd
import random

# Initialize the Mistral AI client
client = MistralClient(api_key=open('../.keys/.key_mistral').read().strip())
model = "open-mistral-7b"

# Read cancer types and define non-cancer topics
cancer_diseases = pd.read_csv("data/cancer_types.csv")['cancer_type'].tolist()
non_cancer_topics = [
    "General Diagnostic Methods", "Imaging Techniques", "Molecular Diagnostics",
    "Biomarker Discovery", "Point-of-Care Testing", "Laboratory Techniques",
    "Histopathology", "Liquid Biopsy", "Genomic Sequencing", "Proteomics", 
    'General Oncology - No Specific Cancer Type'
]

def generate_abstract():
    is_cancer = random.random() >= 0.2
    topic = random.choice(cancer_diseases if is_cancer else non_cancer_topics)
    
    prompt = f"""Generate a 250-350 character abstract about research related to {topic}. 
    Choose one of the following focus areas:
    1. A diagnostic method or technique {'for this cancer type' if is_cancer else 'in medical research'}.
    2. The effects of a novel treatment approach {'for this cancer' if is_cancer else 'in this field'}.
    3. Results from a recent clinical trial {'for this cancer type' if is_cancer else 'in this area of medical research'}.
    4. Advancements in {'cancer' if is_cancer else 'medical'} research methodology.

    Focus on recent developments, improvements in patient outcomes, or potential clinical applications. 
    Do not mention any specific genes, biomarkers, or molecular targets. 
    Use general terms like 'genetic markers', 'protein indicators', or 'molecular pathways' if necessary."""
    
    messages = [
        ChatMessage(role="system", content="You are a medical research assistant specializing in oncology and general medical research. Create concise, realistic abstracts about diagnostic techniques, treatment approaches, clinical trials, and research methods without mentioning specific genes, biomarkers, or molecular targets. Use general terms to discuss biological aspects."),
        ChatMessage(role="user", content=prompt)
    ]
    
    response = client.chat(model=model, messages=messages)
    return {
        "abstract": response.choices[0].message.content.strip(),
        "disease": topic if is_cancer else "n/a",
        "genes": "n/a"
    }

def generate_abstracts(target_rows=10000):
    df = pd.DataFrame(columns=["abstract", "disease", "genes"])
    if os.path.exists("data/synthetic_diagnostic_abstracts.csv"):
        df = pd.read_csv("data/synthetic_diagnostic_abstracts.csv")
        print(f"Loaded existing CSV with {len(df)} rows.")
    
    while len(df) < target_rows:
        try:
            df = pd.concat([df, pd.DataFrame([generate_abstract()])], ignore_index=True)
            print(f"Generated abstract {len(df)}/{target_rows}")
            if len(df) % 10 == 0:
                df.to_csv("data/synthetic_diagnostic_abstracts.csv", index=False)
                print(f"Progress saved. Current rows: {len(df)}")
        except Exception as e:
            print(f"An error occurred: {e}")
            break
    
    df.to_csv("data/synthetic_diagnostic_abstracts.csv", index=False)
    print(f"Final CSV saved with {len(df)} rows.")
    return df

if __name__ == "__main__":
    df = generate_abstracts()
    print(df.head())
    print("\nDistribution of diseases/methods:")
    print(df['disease'].value_counts(dropna=False))
    non_cancer_percentage = (df['disease'] == 'n/a').mean() * 100
    print(f"\nPercentage of non-cancer topics: {non_cancer_percentage:.2f}%")
    
    
    