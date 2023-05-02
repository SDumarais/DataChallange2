import zipfile
import fnmatch
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
 
parquet_file = r"C:\Users\bella\Downloads\Y2Q4 JBG050 Data Challenge 2\dataset.parquet"
dataset = pd.read_parquet(parquet_file, engine='pyarrow')
pd.options.display.max_columns = None
dataset = dataset[dataset['LSOA name'].str.contains('Barnet')==True]
dataset = dataset[dataset['Crime type'].str.contains('Burglary')==True]
print(dataset)

output_filepath = r"C:\Users\bella\Downloads\Y2Q4 JBG050 Data Challenge 2\clean_df.parquet"
table = pa.Table.from_pandas(dataset)
pq.write_table(table, output_filepath)