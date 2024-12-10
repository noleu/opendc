from pyarrow import json
import pyarrow.parquet as pq

files = ["m7g_medium_spot_pricing_cleaned", "m7g_xlarge_spot_pricing_cleaned", "c7g_2xlarge_spot_pricing_cleaned", "r7g_2xlarge_spot_pricing_cleaned"]

for file in files:
    split = file.split('_')
    new_name = split[0] + '.' + split[1]
    table = json.read_json(f'pricing/{file}.json') 
    pq.write_table(table, f'pricing/{new_name}.parquet')  # save json/table as parquet
    print(f'Saved {new_name}.parquet')