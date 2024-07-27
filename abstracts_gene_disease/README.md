# Gene and Disease Information Extraction from Conference Abstracts

## Overview

This project aims to extract gene and disease information from medical conference abstracts using a fine-tuned version of Mistral 7B in Python. Our goal is to help researchers and healthcare professionals efficiently process the vast amount of information available in medical literature.

## Motivation

The flood of information in medical abstracts presents a challenge for manual review. Major conferences in oncology and cancer research, such as ASCO (American Society of Clinical Oncology), AACR (American Association for Cancer Research), and ESMO (European Society for Medical Oncology), receive thousands of abstract submissions each year. Keeping track of all this information manually is virtually impossible, leading to potential missed opportunities in research and clinical practice. This tool aims to automate the extraction process, saving time and effort while maintaining accuracy.

## Key Features

- Extracts gene and disease information from conference abstracts
- Results are stored in JSON format
- Explore results using a Streamlit GUI

## Data Sources and Feature Preparation

1. **OpenTargets Platform**: We use the OpenTargets Platform as our primary source of gene-disease associations in PubMed abstracts.
   - We ingest literature information from the platform
   - Filter entries for cancer indications and in-human results

2. **Complementary Information**:
   - **Biomart**: Retrieve mapping of HGNSC symbols to HUGO symbols
   - **PubMed**: OpenTargets only provides PubMed IDs, we retrieve the full abstracts from PubMed.

3. **Data Volume**:
   - Full dataset: ~190,000 labeled abstracts
   - This version uses only 20k abstracts to keep fine-tuning costs low. As we refine and extend the functionality, we will eventually utilize the entire dataset of ~190,000 abstracts for more comprehensive training and improved performance.

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

Execute the Python script `finetuning_gene_disease.py` to finetune the model yourself.

## Parsing Conference Abstracts

`example_ASCO_abstracts.py` provides an example script to download and parse the contributions to this year's ASCO.

## Exploration with Streamlit App

[Details about the Streamlit app would go here]

## Future Developments

This is the first version of our tool. Future iterations will include:
- Additional information extraction capabilities, such as:
  - Clinical trial identification and staging
  - Therapy types used in studies
  - Biomarker information
  - Patient cohort characteristics
- Utilization of the full dataset (~190,000 abstracts) for improved model performance
