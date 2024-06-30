# Gene and Disease Information Extraction from Conference Abstracts

## Overview

The flood of information in medical abstracts presents a challenge for manual review. At major conferences in oncology and cancer research, such as ASCO (American Society of Clinical Oncology), AACR (American Association for Cancer Research), and ESMO (European Society for Medical Oncology), thousands of abstract get published every year. Keeping track of all this information manually is virtually impossible, leading to potential missed opportunities in research and clinical practice. This tool aims to automate the extraction process, saving time and effort while maintaining accuracy.

This project aims to structure information from medical conference abstracts. To this end, we use a fine-tuned version of Mistral 7B. We leverage existing data from the OpenTargets platform to build a dataset with PubMed abstracts, associated genes, and associated gene and train Mistral 7B on this dataset to recognize gene and disease-associations in conference abstracts. 

For visualisation of gene and disease information we provide a streamlit app. We showcase the results using the abstracts from ASCO 2024

## Use cases

- Hypothesis generation for downstream analysis in real-world patient datasets, such as TCGA
- Competitive landscape assessment

## Future Developments

This is the first version of our tool. Future iterations will include:

- Additional information extraction, such as:
- - Patient cohort characteristics
  - Clinical trial information
  - Treatments
  - Biomarker information

- Inclusion of additional data sources, e.g. clinical-trials.gov
- Utilization of the full dataset (~190,000 abstracts) improved model performance
  


## Data Sources and Feature Preparation

1. **OpenTargets Platform**: We use the OpenTargets Platform as our primary source of gene-disease associations in PubMed abstracts.
   - We ingest literature information from the platform
   - We filter entries for cancer indications and in-human results

2. **Complementary Information**:
   - **Biomart**: Retrieve mapping of HGNSC symbols to HUGO symbols
   - **PubMed**: OpenTargets only provides PubMed IDs, we retrieve the full abstracts from PubMed

3. **Data Volume**:
   - Full dataset: ~190,000 labeled abstracts
   - This version uses only 8,000 abstracts to keep fine-tuning costs low. As we refine and extend the functionality, we will eventually utilize the entire dataset of ~190,000 abstracts for more comprehensive training and improved performance

4. **Synthetic data**:
   - OpenTargets provides an extremely rich source of gene-disease associations but no negative examples - this can lead to hallucationations for the fine-tuned model
   - We generated synthetic data with Mistral7B which contain either disease information, but not gene information, or general medical research (e.g. diagnostics) without any reference to genes or diseases

5. **Final data**:
   - 8,000 real abstracts with gene-disease associations
   - 4,000 synthetic abstracts with disease but not gene-association, or neither of the two




We set up a data pipeline in Dagster. The training data were published on Huggingface. If you nevertheless want to rerun the pipeline follow these steps:

1. Open the folder `abstracts_gene_disease` and install the package with:
   ```bash
   pip install -e ".[dev]"
   ```

2. Then, start the Dagster UI web server:
   ```bash
   dagster dev
   ```

3. Open http://localhost:3000 with your browser to see the project and run the materialisation of the different assets. (Running the full materialisation from the command line does not work in this release due to several partitioned assets, this will be fixed in one of the next releases).

## Fine Tuning

Execute the Python script `[finetuning_gene_disease](02_finetuning_gene_disease.py).py` to finetune the model yourself.

## Parsing Conference Abstracts

`03_example_ASCO_abstracts.py` provides an example script to download and parse the contributions to this year's ASCO conference. 

## Exploration with Streamlit App

[Details about the Streamlit app would go here]


