# csv_to_parquet.py

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

csv_file = r"C:\Users\20212387\OneDrive - TU Eindhoven\Documents\Y2\Data challenge II\uncleaned_dataset.csv"
parquet_file = r"C:\Users\20212387\OneDrive - TU Eindhoven\Documents\Y2\Data challenge II\uncleaned_data_pq.csv"
chunksize = 100_000

csv_stream = pd.read_csv(csv_file, sep='\t', chunksize=chunksize, engine='python')

for i, chunk in enumerate(csv_stream):
    print("Chunk", i)
    if i == 0:
        # Guess the schema of the CSV file from the first chunk
        parquet_schema = pa.Table.from_pandas(df=chunk).schema
        # Open a Parquet file for writing
        parquet_writer = pq.ParquetWriter(parquet_file, parquet_schema, compression='snappy')
    # Write CSV chunk to the parquet file
    table = pa.Table.from_pandas(chunk, schema=parquet_schema)
    parquet_writer.write_table(table)

parquet_writer.close()