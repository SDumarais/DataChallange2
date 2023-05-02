import zipfile
import fnmatch
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

parquet_file = r"C:\Users\bella\Downloads\Y2Q4 JBG050 Data Challenge 2\clean_df.parquet"
dataset = pd.read_parquet(parquet_file, engine='pyarrow')
print(dataset)

import folium
from IPython.display import display
from IPython.core.display import HTML

# Create a map object
map = folium.Map(location=[dataset['Latitude'].mean(), dataset['Longitude'].mean()], zoom_start=10)

# Add a marker for each location in the dataframe
dataset.apply(lambda row: folium.Marker(location=[row['latitude'], row['longitude']], tooltip=row['name']).add_to(map), axis=1)

# Display the map
display(map)
HTML(map._repr_html_())
