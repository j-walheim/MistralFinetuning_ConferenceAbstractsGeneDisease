import PyPDF2
import pandas as pd
import re

def extract_abstracts(pdf_path):
    abstracts = []
    
    try:
        with open(pdf_path, 'rb') as file:
            pdf_reader = PyPDF2.PdfReader(file)
            
            for page in pdf_reader.pages:
                text = page.extract_text()
                page_abstracts = split_page_into_abstracts(text)
                abstracts.extend(page_abstracts)
        
        df = pd.DataFrame(abstracts, columns=['Abstract'])
        
        print(f"Successfully extracted {len(df)} abstracts.")
        print(df.head())
    
        return df
    
    except FileNotFoundError:
        print(f"Error: The file {pdf_path} was not found.")
    except PyPDF2.errors.PdfReadError:
        print(f"Error: Unable to read the PDF file {pdf_path}.")
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")
    
    
    
    return None


 
def split_page_into_abstracts(text):
    pattern = r'\d{3,}\s+(Poster Session|Oral Abstract Session|Rapid Oral Abstract Session|Clinical Science Symposium)'
    #abstracts = re.split(pattern, text)

    matches = list(re.finditer(pattern, text))
    abstracts = []
    for i in range(len(matches)):
        start = matches[i].start()
        end = matches[i+1].start() if i+1 < len(matches) else len(text)
        abstract = text[start:end].strip()
        abstracts.append(abstract)
    return abstracts
