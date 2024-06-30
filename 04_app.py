import streamlit as st
import pandas as pd
import json
import plotly.graph_objects as go
import colorsys
import requests
from fuzzywuzzy import process
import re

def generate_dynamic_palette(indications):
    
    base_palette = ['#E6E1D6', '#8C9FAC', '#66CDAA', '#8FBC8F', 
                    '#D2B48C', '#696969', '#4682B4', '#20B2AA', 
                    '#9ACD32', '#DEB887', '#CD5C5C', '#708090', 
                    '#E9967A', '#00CED1', '#FF8C00', '#1E90FF']

    num_indications = len(indications)
    num_base_colors = len(base_palette)
    if num_indications <= num_base_colors:
        return dict(zip(indications, base_palette[:num_indications]))
    
    # If we need more colors, we'll add transparency to the base colors
    extended_palette = base_palette.copy()
    for i in range(num_base_colors, num_indications):
        base_color = base_palette[i % num_base_colors]
        # Convert hex to RGB
        rgb = tuple(int(base_color[j:j+2], 16) / 255.0 for j in (1, 3, 5))
        # Convert RGB to HSL
        h, l, s = colorsys.rgb_to_hls(*rgb)
        # Increase lightness (which effectively adds transparency in HSL space)
        l = min(1.0, l + 0.1 * ((i // num_base_colors) + 1))
        # Convert back to RGB
        rgb = colorsys.hls_to_rgb(h, l, s)
        # Convert RGB back to hex
        hex_color = '#' + ''.join([f'{int(x*255):02x}' for x in rgb])
        extended_palette.append(hex_color)
    return dict(zip(indications, extended_palette))

def get_tumor_types():
    url = "https://oncotree.mskcc.org:443/api/tumorTypes"
    headers = {"Accept": "application/json"}
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        return f"Error: {response.status_code}"
    
def clean_indication(indication):
    if indication is None:
        return ""
    
    # Convert to string if it's not already
    indication = str(indication)
    
    # Split the string and take the first entry if there are multiple
    first_indication = indication.split(',')[0].strip()
    
    # Remove content within parentheses
    cleaned = re.sub(r'\([^)]*\)', '', first_indication)
    
    # Remove special characters and convert to lowercase
    cleaned = re.sub(r'[^a-zA-Z0-9\s]', '', cleaned).lower().strip()
    
    return cleaned


def find_exact_match(cleaned_indication, tumor_types):
    for tumor in tumor_types:
        if cleaned_indication == tumor['name']:
            return tumor, 'name'
        if cleaned_indication == tumor['mainType']:
            return tumor, 'mainType'

    return None, None

def find_fuzzy_match(cleaned_indication, tumor_types):
    match_candidates = []
    for tumor in tumor_types:
        match_candidates.append((tumor['name'], tumor, 'name'))
        match_candidates.append((tumor['mainType'], tumor, 'mainType'))


    matches = process.extract(cleaned_indication, [candidate[0] for candidate in match_candidates], limit=5)
    best_match = matches[0]
    if best_match[1] > 80:  # 80% similarity threshold
        matched_candidate = next(candidate for candidate in match_candidates if candidate[0] == best_match[0])
        return matched_candidate[1], matched_candidate[2]
    return None, None

def create_match_dict(tumor, match_type):
    if match_type == 'name':
        return {
            'matched_name': tumor['name'],
            'code': tumor['code'],
            'mainType': tumor['mainType']
        }
    elif match_type == 'mainType':
        return {
            'matched_name': 'NA',
            'code': 'NA',
            'mainType': tumor['mainType']
        }

def map_indications(indications, tumor_types):
    mapped_indications = {}

    for indication in indications:
        cleaned_indication = clean_indication(indication)
        if not cleaned_indication:
            mapped_indications[indication] = None
            continue

        # Exact matching
        exact_match, match_type = find_exact_match(cleaned_indication, tumor_types)
        if exact_match:
            mapped_indications[indication] = create_match_dict(exact_match, match_type)
        else:
            # Fuzzy matching
            best_match, match_type = find_fuzzy_match(cleaned_indication, tumor_types)
            if best_match:
                mapped_indications[indication] = create_match_dict(best_match, match_type)
            else:
                mapped_indications[indication] = None

        # Debug information
        print(f"Mapping for '{indication}':")
        print(f" Cleaned: '{cleaned_indication}'")
        print(f" Mapped to: {mapped_indications[indication]}")
        print("---")

    return mapped_indications

# Define a function that maps a cancer type to its larger grouping
def map_cancer_type_to_top_level_group(cancer_type):
    return cancer_type_to_group.get(cancer_type, 'Other Cancer')

def aggregate_indications(mapped_indications):
    aggregated = {}
    for indication, mapping in mapped_indications.items():
        if mapping:
            main_type = mapping['mainType'] 
            if main_type not in aggregated:
                aggregated[main_type] = []
            aggregated[main_type].append(indication)
    
    return aggregated

def map_back_to_df(df, mapped_indications, aggregated_indications):
    # Create a dictionary to map original indications to their main types
    indication_to_main_type = {}
    for main_type, indications in aggregated_indications.items():
        for indication in indications:
            indication_to_main_type[indication] = main_type
    
    # Create new columns in the DataFrame
    df['Mapped_Indication'] = df['Disease'].map(lambda x: mapped_indications[x]['matched_name'] if mapped_indications[x] and mapped_indications[x]['matched_name'] != 'NA' else mapped_indications[x]['mainType'] if mapped_indications[x] else None)
    df['OncoTree_Code'] = df['Disease'].map(lambda x: mapped_indications[x]['code'] if mapped_indications[x] else None)
    df['Main_Type'] = df['Disease'].map(lambda x: mapped_indications[x]['mainType'] if mapped_indications[x] else None)
    df['Top_Level_Group'] = df['Main_Type'].map(map_cancer_type_to_top_level_group)
    
    return df

def extract_abstract_number(string):
    # Use regular expressions
    match = re.match(r'(\d+)', string)

    # Return the number if found, otherwise return None
    if match:
        return match.group(1)
    else:
        return None


@st.cache_data
def load_data():
    with open('temporary_res 2.json', 'r') as f:
        data = json.load(f)
    
    processed_data = []
    
    for item in data:
        abstract = item.get('abstract', '')
        disease = item.get('Disease')
        genes = item.get('Genes')
        
        # Extract the abstract number
        abstract_number = extract_abstract_number(abstract)
        
        # Convert 'n/a' to None for disease
        disease = None if disease == 'n/a' else disease
        
        # Handle genes
        if genes == 'n/a':
            genes = None
        elif isinstance(genes, str):
            genes = [gene.strip() for gene in genes.split(',')]
        
        # If genes is a list, create a row for each gene
        if isinstance(genes, list):
            for gene in genes:
                processed_data.append({
                    'Abstract_Number': abstract_number,
                    'Abstract': abstract,
                    'Disease': disease,
                    'Genes': gene.strip() if gene else None
                })
        else:
            # If no genes or genes is None, still create a row
            processed_data.append({
                'Abstract_Number': abstract_number,
                'Abstract': abstract,
                'Disease': disease,
                'Genes': None
            })
    
    # Create DataFrame
    df = pd.DataFrame(processed_data)
    
    # Remove rows where both 'Disease' and 'Gene' are None
    df = df[~((df['Disease'].isnull()) & (df['Genes'].isnull()))]
    
    return df

@st.cache_data
def load_and_process_data():
    df = load_data()
    tumor_types = get_tumor_types()
    original_indications = df['Disease'].unique().tolist()
    mapped = map_indications(original_indications, tumor_types)
    aggregated = aggregate_indications(mapped)
    df = map_back_to_df(df, mapped, aggregated)
    return df, mapped, aggregated

# Load and process data once
@st.cache_data
def prepare_data():
    df, mapped, aggregated = load_and_process_data()
    df_exploded = df.explode('Genes')
    df_exploded = df_exploded[df_exploded['Genes'].notna() & (df_exploded['Genes'] != '') & (df_exploded['Genes'] != 'n/a')]
    gene_counts = df_exploded.groupby(['Genes', 'Main_Type']).size().unstack(fill_value=0)
    return df, df_exploded, gene_counts, mapped, aggregated

df, df_exploded, gene_counts, mapped, aggregated = prepare_data()

# Generate color map
main_types = df_exploded['Main_Type'].unique().tolist()
color_map = generate_dynamic_palette(main_types)

# Calculate the total count for each main_type
total_counts = gene_counts.sum()
most_common_type = total_counts.idxmax()

# Create selection chart (this part remains largely the same)
st.header('📊 Number of Abstracts with Reference to a Gene Target')
fig_selection = go.Figure()

for main_type in gene_counts.columns:
    fig_selection.add_trace(go.Bar(
        x=['Indication'],
        y=[gene_counts[main_type].sum()],
        name=str(main_type),
        marker_color=color_map.get(str(main_type), '#808080'),
        visible=True
    ))

fig_selection.update_layout(
    title="",
    yaxis_title='Count',
    barmode='group',
    bargap=0.2,
    bargroupgap=0.1,  # Adds space between bars in a group
    showlegend=True,
    legend_title='Indication',
    height=500,
    plot_bgcolor='rgba(0,0,0,0)',
    paper_bgcolor='rgba(0,0,0,0)',
    font=dict(color='white')
)

st.plotly_chart(fig_selection, use_container_width=True, key='main_type_selection')

def update_visualization(selected_types, min_count):
    filtered_gene_counts = gene_counts[selected_types]
    genes_in_selected_indications = filtered_gene_counts[filtered_gene_counts.sum(axis=1) >= min_count]
    sorted_genes = genes_in_selected_indications.sum(axis=1).sort_values(ascending=False).index

    fig = go.Figure()

    for main_type in selected_types:
        genes_in_selected_indication = genes_in_selected_indications[main_type]
        sorted_genes_in_selected_indication = genes_in_selected_indication.loc[sorted_genes].index

        fig.add_trace(go.Bar(
            x=sorted_genes_in_selected_indication,
            y=genes_in_selected_indication.loc[sorted_genes_in_selected_indication],
            name=str(main_type),
            marker_color=color_map.get(str(main_type), '#808080')
        ))

    fig.update_layout(
        title='',
        xaxis_title='Gene',
        yaxis_title='Count',
        barmode='stack',
        showlegend=True,
        legend_title='Indication',
        legend={'traceorder':'normal'},
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='white'),
        hovermode='x unified'
    )

    fig.update_xaxes(tickangle=45, tickmode='array', tickvals=sorted_genes, ticktext=sorted_genes)

    return fig



# Get the selected types and update the visualization
selected_types = [trace['name'] for trace in fig_selection.data if trace['visible'] != 'legendonly']
selected_types.sort()

if not selected_types:
    st.warning("Please select at least one Indication by clicking on the legend items.")
    st.stop()

st.header('🧬 What Are The Most Popular Gene Targets?')
# Add a slider to filter the minimum number of genes
min_count = st.slider('Filter genes with minimum number of occurrences', min_value=1, value=5)
fig = update_visualization(selected_types, min_count)
st.plotly_chart(fig)

# Interactive Table
st.header('Have a look at the data! 🕵️‍♀️🔎')

# Prepare the data for the table
table_data = df[['Abstract_Number', 'Genes', 'Disease', 'Main_Type', 'Mapped_Indication', 'OncoTree_Code', 'Top_Level_Group']].copy()

# Explode the Genes column to create separate rows for each gene
table_data = table_data.explode('Genes')

# Remove rows where Genes is NaN, None, or an empty string
table_data = table_data[table_data['Genes'].notna() & (table_data['Genes'] != '') & (table_data['Genes'] != 'n/a')]

# Reset the index to make it easier to reference rows
table_data = table_data.reset_index(drop=True)

# Display the interactive table
st.dataframe(
    table_data,
    column_config={
        "Abstract_Number": st.column_config.TextColumn("Abstract Number", width="small"),
        "Genes": st.column_config.TextColumn("Genes", width="medium"),
        "Disease": st.column_config.TextColumn("Disease", width="medium"),
        "Main_Type": st.column_config.TextColumn("Main Type", width="medium"),
        "Mapped_Indication": st.column_config.TextColumn("Mapped Indication", width="medium"),
        "OncoTree_Code": st.column_config.TextColumn("OncoTree Code", width="small"),
        "Top_Level_Group": st.column_config.TextColumn("Top Level Group", width="medium"),
    },
    hide_index=True,
    use_container_width=True,
    height=400,
)

# Add a download button for the table data
csv = table_data.to_csv(index=False).encode('utf-8')
st.download_button(
    label="Download data as CSV",
    data=csv,
    file_name="oncotree_data.csv",
    mime="text/csv",
)
