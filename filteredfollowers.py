import pandas as pd
import re
import csv
from collections import defaultdict

print("Starting the keyword search process...")

# Define a function to check if a keyword is in the 'bio' and 'location' columns
# Check if any keyword is in the column and return the keyword if found
def keyword_in_row(row, keywords):
    bio_str = str(row['bio']) if 'bio' in row else ''
    location_str = str(row['location']) if 'location' in row else ''
    found_keywords_bio = [keyword for keyword in keywords if keyword in bio_str]
    found_keywords_location = [keyword for keyword in keywords if keyword in location_str]
    found_keywords = found_keywords_bio + found_keywords_location
    return found_keywords if found_keywords else None

# Read the input file
# Read the keywords
print("Reading the input file and keywords...")
df = pd.read_csv('input1.csv', chunksize=2500, quoting=csv.QUOTE_ALL, encoding='utf-8', engine='python')
with open('keywords1.txt', 'r') as f:
    keywords = f.read().splitlines()

# Create a new DataFrame to store the selected rows
selected_df = pd.DataFrame()

# Process the data in chunks
# Select rows that contain a keyword and add a new column with the keyword
# Append the selected rows to the new DataFrame
print("Searching for keywords in the data...")
for chunk in df:
    chunk['keyword'] = chunk.apply(lambda row: keyword_in_row(row, keywords), axis=1)
    selected_rows = chunk.dropna(subset=['keyword'])
    selected_df = pd.concat([selected_df, selected_rows])

# Deduping process
# Drop duplicates based on 'profileUrl' and 'screenName' columns
print("Deduping process started...")
selected_df['keyword'] = selected_df['keyword'].apply(lambda x: ', '.join(x))
selected_df = selected_df.drop_duplicates(subset=['profileUrl', 'screenName'])

'''
After the deduping process, I want to search the bios and locations of the new list for all the keywords in the keywords2.txt.
If the keyword is found in one of the rows, I want it added to that the cell in the "keyword" column.
Then I want the whole sheet deduped again
'''

# Read the second set of keywords
with open('keywords2.txt', 'r') as f:
    keywords2 = f.read().splitlines()

# Search the bios and locations of the new list for all the keywords in the keywords2.txt
print("Searching for second set of keywords in the data...")
for index, row in selected_df.iterrows():
    found_keywords = keyword_in_row(row, keywords2)
    if found_keywords:
        # If the keyword is found in one of the rows, add it to that the cell in the "keyword" column
        existing_keywords = row['keyword'].split(', ')
        new_keywords = existing_keywords + found_keywords
        selected_df.at[index, 'keyword'] = ', '.join(new_keywords)

# Dedupe the sheet again
print("Second deduping process started...")
selected_df['keyword'] = selected_df['keyword'].apply(lambda x: ', '.join(list(set(x.split(', ')))))
selected_df = selected_df.drop_duplicates(subset=['profileUrl', 'screenName'])

print("Sorting the DataFrame by the number of keywords...")
selected_df['keyword_count'] = selected_df['keyword'].apply(lambda x: x.count(','))
selected_df = selected_df.sort_values(by='keyword_count', ascending=False)
selected_df = selected_df.drop(columns=['keyword_count'])

print("Writing the selected rows to the output file...")
selected_df.to_csv('output.csv', index=False, quoting=csv.QUOTE_ALL, encoding='utf-8')

print("Keyword search process completed.")

