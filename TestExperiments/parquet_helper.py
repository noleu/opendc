import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

def create_parquet_file(data, file_path, schema):
    if os.path.exists(file_path):
        os.remove(file_path)

    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df, schema=schema)
    pq.write_table(table, file_path)

STARTING_TIMESTAMP = 1735689600 # 2025-01-01 00:00:00
MILISECONDS = 1
# SECONDS = 1000 * MILISECONDS
SECONDS = 1
MINUTES = 60 * SECONDS
HOURS = 60 * MINUTES
DAYS = 24 * HOURS
WEEKS = 7 * DAYS

price_schema = pa.schema([
    ("timestamp", pa.int64(), False),
    ("spot_price", pa.float64(), False),
    ("on_demand_price", pa.float64(), False)
])

tasks_schema = pa.schema([
    ("id", pa.string(), False),
    ("submission_time", pa.int64(), False),
    ("duration", pa.int64(), False),
    ("cpu_count", pa.int32(), False),
    ("cpu_capacity", pa.float64(), False),
    ("mem_capacity", pa.int64(), False),
    ("deadline", pa.int64(), False),
])

fragments_schema = pa.schema([
    ("id", pa.string(), False),
    ("duration", pa.int64(), False),
    ("cpu_count", pa.int32(), False),
    ("cpu_usage", pa.float64(), False),
])

data = {
    'timestamp': [ STARTING_TIMESTAMP + i for i in range(0, 16 * WEEKS, DAYS) ],
    'spot_price': [ 2010 - i * 10 for i in range(16 * 7) ],
    'on_demand_price': [ 2010 - i * 10 for i in range(16 * 7) ],
}
file_path = 'traces/validate/decreasing_price_on_demand.parquet'
create_parquet_file(data, file_path, price_schema)

data = {
    'timestamp': [ STARTING_TIMESTAMP + i for i in range(0, 16 * WEEKS, DAYS) ],
    'spot_price': [ 2010 + i * 10 for i  in range(16 * 7) ],
    'on_demand_price': [ 2010 + i * 10 for i in range(16 * 7) ],
}
file_path = 'traces/validate/increasing_price_on_demand.parquet'
create_parquet_file(data, file_path, price_schema)

data = {
    'timestamp': [ STARTING_TIMESTAMP + i for i in range(0, 16 * WEEKS, DAYS) ],
    'spot_price': [ 1800 + (400 * (i % 2) ) for i in range(16 * 7) ],
    'on_demand_price': [ 2000 for i in range(16 * 7) ],
}
file_path = 'traces/validate/volatile_pricing.parquet'
create_parquet_file(data, file_path, price_schema)

data = {
    'timestamp': [ STARTING_TIMESTAMP + i for i in range(0, 16 * WEEKS, DAYS) ],
    'spot_price': [ 1800 if i < 1 * WEEKS // DAYS else 2200 for i in range(16 * WEEKS // DAYS) ],
    'on_demand_price': [ 2000 for i in range(16 * WEEKS // DAYS) ],
}
file_path = 'traces/validate/minimal_spot.parquet'
create_parquet_file(data, file_path, price_schema)

data = {
    'timestamp': [ STARTING_TIMESTAMP + i for i in range(0, 16 * WEEKS, MINUTES) ],
    'spot_price': [ 1800 if i > 15 * WEEKS // MINUTES else 2200 for i in range(16 * WEEKS // MINUTES) ],
    'on_demand_price': [ 2000 for i in range(16 * WEEKS // MINUTES) ],
}
file_path = 'traces/validate/late_spot.parquet'
create_parquet_file(data, file_path, price_schema)

data = {
    'timestamp': [ STARTING_TIMESTAMP - HOURS + i for i in range(0, 18 * WEEKS, DAYS) ],
    'spot_price': [ 1 for i in range(18 * WEEKS // DAYS) ],
    'on_demand_price': [ 3 for i in range(18 * WEEKS // DAYS) ],
}
file_path = 'traces/validate/high_spot_price.parquet'
create_parquet_file(data, file_path, price_schema)

NUM_TASKS = 1
data = {
    'id': [ str(i) for i in range(NUM_TASKS) ],
    'submission_time': [ STARTING_TIMESTAMP + i * WEEKS for i in range(NUM_TASKS) ],
    'duration': [ STARTING_TIMESTAMP + 4 * WEEKS + i * WEEKS for i in range(NUM_TASKS) ],
    'cpu_count': [ 12 for i in range(NUM_TASKS) ],
    'cpu_capacity': [ 2926.000135 for i in range(NUM_TASKS) ],
    'mem_capacity': [ 181352 for i in range(NUM_TASKS) ],
    'deadline': [ 16 * WEEKS for i in range(NUM_TASKS) ], # Deadline needs to be offset from submission time
}
file_path = 'traces/validate/tasks.parquet'
create_parquet_file(data, file_path, tasks_schema)

FRAGMENT_DURATION = MINUTES
TASK_DURATION = 4 * WEEKS // FRAGMENT_DURATION
data = {
    'id': [ str(i % NUM_TASKS) for i in range(0, TASK_DURATION * NUM_TASKS) ],
    'duration': [ FRAGMENT_DURATION for i in range(0, TASK_DURATION * NUM_TASKS) ],
    'cpu_count': [ 8 for i in range(0, TASK_DURATION * NUM_TASKS) ],
    'cpu_usage': [ 2400 for i in range(0, TASK_DURATION * NUM_TASKS) ],
}
file_path = 'traces/validate/fragments.parquet'
create_parquet_file(data, file_path, fragments_schema)