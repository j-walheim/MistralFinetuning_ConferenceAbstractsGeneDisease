from ftplib import FTP
from dagster import asset, AssetExecutionContext
import pandas as pd
import io


@asset
def cancer_diseases_opentargets(context: AssetExecutionContext,) -> pd.DataFrame:
    
    url = "ftp.ebi.ac.uk"
    directory = "pub/databases/opentargets/platform/22.04/output/etl/parquet/diseases/"
    
    with FTP(url) as ftp:
        ftp.login()
        ftp.cwd(directory)
        parquet_files = [file for file in ftp.nlst() if file.endswith('.parquet')]
            
    disease = []
    with FTP(url) as ftp:
        ftp.login()
        ftp.cwd(directory)
        
        for file in parquet_files:
            context.log.info(f"Processing {file}")
            with io.BytesIO() as buffer:
                ftp.retrbinary(f'RETR {file}', buffer.write)
                buffer.seek(0)
                data_cur = pd.read_parquet(buffer)
                
                data_cur = data_cur[['id', 'name', 'therapeuticAreas']]
                data_cur = data_cur.explode('therapeuticAreas')
                data_cur = data_cur[data_cur['therapeuticAreas'] == 'MONDO_0045024']
                data_cur = data_cur.drop('therapeuticAreas', axis=1)
                
                disease.append(data_cur)
    
    # write to temporary csv
    disease = pd.concat(disease)
    
    disease = disease[disease['id'].str.contains('EFO')]
    # drop abstract categories
    disease = disease[~disease['name'].isin(['cancer', 'neoplasm', 'cancer', 'carcinoma', 'cirrhosis of liver'])]

    context.log.info(f"Found {disease.shape[0]} cancer diseases")
    context.log.info(disease.head())
            
    return disease

