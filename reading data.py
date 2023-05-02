import zipfile
import fnmatch
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# specify the path to the zip file
zip_path = r"C:\Users\bella\Downloads\2017-04.zip"

# pattern for files to extract
pattern1 = "*metropolitan-street.csv"
pattern2 = "*metropolitan-outcomes.csv"
pattern3 = "*metropolitan-stop-and-search.csv"

# open the zip file for reading
with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    
    # iterate through all files in the archive
    for file_name in zip_ref.namelist():
        # print(file_name)
        # extract file if it matches the pattern
        if fnmatch.fnmatch(file_name, pattern1) or fnmatch.fnmatch(file_name, pattern2) or fnmatch.fnmatch(file_name, pattern3):
            # print(file_name)
            zip_ref.extract(file_name, r"C:\Users\bella\Downloads\Apr2017.zip")


# specify the directory containing the CSV files
directory = r"C:\Users\bella\Downloads\Y2Q4 JBG050 Data Challenge 2"

# initialize an empty list to store dataframes
df_list = []

# iterate through all subfolders and CSV files in the directory
for root, dirs, files in os.walk(directory):
    for filename in files:
        # print(filename)
        #Change csv names
        if filename.endswith("metropolitan-street.csv"): 
            # print(filename)
            # read the CSV file into a dataframe and append to the list
            filepath = os.path.join(root, filename)
            df = pd.read_csv(filepath)
            df_list.append(df)

# concatenate all dataframes into a single dataframe
concatenated_df = pd.concat(df_list, axis=0, ignore_index=True)
# write the concatenated dataframe to a new parquet file
output_filepath = r"C:\Users\bella\Downloads\Y2Q4 JBG050 Data Challenge 2\dataset.parquet"

# write the concatenated dataframe to a Parquet file
table = pa.Table.from_pandas(concatenated_df)
pq.write_table(table, output_filepath)