import pandas as pd
import re
import csv
from collections import defaultdict

print("Starting the keyword search process...")

# Define a function to check if a keyword is in a row
def keyword_in_row(row, keywords):
    # Convert row to string and concatenate all cells
    row_str = ' '.join(row.astype(str))
    # Check if any keyword is in the row and return the keyword if found
    found_keywords = [keyword for keyword in keywords if keyword in row_str]
    return found_keywords if found_keywords else None

print("Reading the input file and keywords...")
# Read the input file
df = pd.read_csv('input.csv', chunksize=25000, quoting=csv.QUOTE_ALL, encoding='utf-8')
# Read the keywords
with open('keywords.txt', 'r') as f:
    keywords = f.read().splitlines()

# Create a new DataFrame to store the selected rows
selected_df = pd.DataFrame()

print("Searching for keywords in the data...")
# Process the data in chunks
for chunk in df:
    # Select rows that contain a keyword and add a new column with the keyword
    chunk['keyword'] = chunk.apply(lambda row: keyword_in_row(row, keywords), axis=1)
    selected_rows = chunk.dropna(subset=['keyword'])
    # Append the selected rows to the new DataFrame
    selected_df = pd.concat([selected_df, selected_rows])

# Deduping process
print("Deduping process started...")
# Group by all columns except 'keyword', and join all keywords for each group
selected_df['keyword'] = selected_df['keyword'].apply(lambda x: ', '.join(x))
selected_df = selected_df.groupby([col for col in selected_df.columns if col != 'keyword'], as_index=False).agg({'keyword': ', '.join})

print("Writing the selected rows to the output file...")
# Write the selected rows to the output file
selected_df.to_csv('output.csv', index=False, quoting=csv.QUOTE_ALL, encoding='utf-8')

print("Keyword search process completed.")

