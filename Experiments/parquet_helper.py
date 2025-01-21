from os import cpu_count
from random import uniform

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import random

def create_parquet_file(data, file_path, schema):
    if os.path.exists(file_path):
        os.remove(file_path)
    if not os.path.exists(os.path.dirname(file_path)):
        os.makedirs(os.path.dirname(file_path))

    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df, schema=schema)
    pq.write_table(table, file_path)

# STARTING_TIMESTAMP = 1735689600 # 2025-01-01 00:00:00
STARTING_TIMESTAMP = 1704067200 # 2024-01-01 00:00:00
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

# min max values of aws instances from october 2024
instances = {"c7g.2xlarge":(0.0444,0.1891), "m7g.medium":(0.005, 0.0233), "m7g.xlarge": (0.0201, 0.1016), "r7g.2xlarge":(0.0682, 0.2928) }
workload_runtime = 52 * WEEKS # weeks, plus two weeks buffer
for instance, price_range in instances.items():

    spot_prices = []
    for i in range(0, workload_runtime, MINUTES):
            # every hour price change
            if i % 12*3600 == 0:
                # every even hour there is  spot instance
                if (i // 12*3600) % 2 == 0:
                    spot_prices.append(uniform(price_range[0], price_range[1]))
                else:
                    spot_prices.append(1.2 * price_range[1])
            else:
                spot_prices.append(spot_prices[-1])

    #switching between spot and on demand twice a day
    data = {
        'timestamp': [ STARTING_TIMESTAMP + i for i in range(0, workload_runtime, MINUTES ) ],
        'spot_price': spot_prices,
        'on_demand_price': [ 1.1 * price_range[1] for i in range(0, workload_runtime, MINUTES ) ],
    }
    file_path = f'traces/report/price_traces/{instance}.volatile_pricing.parquet'
    create_parquet_file(data, file_path, price_schema)

    spot_prices = []
    for i in range(0, workload_runtime, MINUTES):
        if i % 3600 == 0:
            if i < 46 * WEEKS:
                spot_prices.append(1.1* price_range[1])
            else:
                spot_prices.append(uniform(price_range[0], price_range[1]))
        else:
            spot_prices.append(spot_prices[-1])

    # also only the last 4 weeks, maybe 80 % of the time spot
    data = {
        'timestamp': [ STARTING_TIMESTAMP + i for i in range(0, workload_runtime, MINUTES ) ],
        'spot_price': spot_prices,
        'on_demand_price': [ 0.8 * price_range[1] for i in range(0, workload_runtime, MINUTES ) ],
    }
    file_path = f'traces/report/price_traces/{instance}.late_spot.parquet'
    create_parquet_file(data, file_path, price_schema)

    spot_prices = []
    for i in range(0, workload_runtime, MINUTES):
        if i % 3600 == 0:
            if i < 46 * WEEKS:
                spot_prices.append(1.2 * price_range[1])
            else:
                spot_prices.append(uniform(price_range[0], price_range[1]))
        else:
            spot_prices.append(spot_prices[-1])

    # guaranteed spot price in the last 4 weeks
    data = {
        'timestamp': [STARTING_TIMESTAMP + i for i in range(0, workload_runtime, MINUTES )],
        'spot_price': spot_prices,
        'on_demand_price': [1.1 * price_range[1] for i in
                            range(0, workload_runtime, MINUTES )],
    }
    file_path = f'traces/report/price_traces/{instance}.guaranteed_late_spot.parquet'
    create_parquet_file(data, file_path, price_schema)

    spot_prices = []
    for i in range(0, workload_runtime, MINUTES):
        if i % 3600 == 0:
                spot_prices.append(uniform(price_range[0], price_range[1]))
        else:
            spot_prices.append(spot_prices[-1])

    # 80 * of the time spot is available
    data = {
        'timestamp': [STARTING_TIMESTAMP + i for i in range(0, workload_runtime, MINUTES )],
        'spot_price': spot_prices,
        'on_demand_price': [0.8 * price_range[1] for i in range(0, workload_runtime, MINUTES )],
    }
    file_path = f'traces/report/price_traces/{instance}.normal.parquet'
    create_parquet_file(data, file_path, price_schema)

############ static WORKLOAD ############

# Number of tasks, duration in days, cpu_count, cpu_capacity, cpu_usage
dict = {'small-low-resources': (25, 14, 1,1000, 10.0), 'small-medium-resources': (25, 14,4,1600, 50.0), 'small-high-resources': (25, 14, 8, 3200, 100.0),
        'medium-low-resources': (50, 7, 1,1000, 10.0), 'medium-medium-resources': (50, 7, 4,1600, 50.0), 'medium-high-resources': (50, 7, 8, 3200, 100.0),
        'large-low-resources': (350, 1, 1,1000, 10.0), 'large-medium-resources': (350, 1, 4,1600, 50.0), 'large-high-resources': (350, 1, 8, 3200, 100.0)}

for key, value in dict.items():
    NUM_TASKS = value[0]
    FRAGMENT_DURATION = MINUTES
    TASK_DURATION = value[1] * DAYS // FRAGMENT_DURATION
    NUM_FRAGMENTS = NUM_TASKS * TASK_DURATION

    data = {
        'id': [str(i) for i in range(NUM_TASKS)],
        'submission_time': [STARTING_TIMESTAMP for i in range(NUM_TASKS)],
        'duration': [value[1] * DAYS for i in range(NUM_TASKS)],
        'cpu_count': [value[2] for i in range(NUM_TASKS)],
        'cpu_capacity': [value[3] * NUM_FRAGMENTS for i in range(NUM_TASKS)],
        'mem_capacity': [181352 for i in range(NUM_TASKS)],
        'deadline': [4 *value[1] * DAYS for i in range(NUM_TASKS)],  # Deadline needs to be offset from submission time
    }
    file_path = f'traces/report/workload/static-resources/{key}/tasks.parquet'
    create_parquet_file(data, file_path, tasks_schema)

    data = {
        'id': [str(i % NUM_TASKS) for i in range(0, NUM_FRAGMENTS)],
        'duration': [FRAGMENT_DURATION for i in range(0, NUM_FRAGMENTS)],
        'cpu_count': [value[2] for i in range(0, NUM_FRAGMENTS)],
        'cpu_usage': [value[4] for i in range(0, NUM_FRAGMENTS)],
    }
    file_path = f'traces/report/workload/static-resources/{key}/fragments.parquet'
    create_parquet_file(data, file_path, fragments_schema)

############ dynamic WORKLOAD ############
# number of task, duration in days
dict = {'small-varying-resources': (25, 14), 'medium-varying-resources': (50, 7), 'large-varying-resources': (350, 1)}

for key, value in dict.items():
    NUM_TASKS = value[0]
    FRAGMENT_DURATION = MINUTES
    TASK_DURATION = value[1] * DAYS // FRAGMENT_DURATION
    NUM_FRAGMENTS = NUM_TASKS * TASK_DURATION

    data = {
        'id': [str(i % NUM_TASKS) for i in range(0, NUM_FRAGMENTS)],
        'duration': [FRAGMENT_DURATION for i in range(0, NUM_FRAGMENTS)],
        'cpu_count': [random.randrange(1,9) for i in range(0, NUM_FRAGMENTS)],
        'cpu_usage': [random.uniform(0,101) for i in range(0, NUM_FRAGMENTS)],
    }
    file_path = f'traces/report/workload/static-resources/{key}/fragments.parquet'
    create_parquet_file(data, file_path, fragments_schema)

    df = pq.read_table(file_path).to_pandas()

    # Group by 'id' and sum the 'cpu_usage' for each 'id'
    cpu_usage_sum = df.groupby('id')['cpu_usage'].sum().reset_index()
    cpu_count_max = df.groupby('id')['cpu_count'].max().reset_index()

    data = {
        'id': [str(i) for i in range(NUM_TASKS)],
        'submission_time': [STARTING_TIMESTAMP for i in range(NUM_TASKS)],
        'duration': [value[1] * DAYS for i in range(NUM_TASKS)],
        # 'cpu_count': [random.randrange(1,8) for i in range(NUM_TASKS)],
        'cpu_count': cpu_count_max['cpu_count'],
        # 'cpu_capacity': [2926.000135 for i in range(NUM_TASKS)],
        'cpu_capacity': cpu_usage_sum['cpu_usage'],
        'mem_capacity': [181352 for i in range(NUM_TASKS)],
        'deadline': [4 *value[1] * DAYS for i in range(NUM_TASKS)],  # Deadline needs to be offset from submission time
    }
    file_path = f'traces/report/workload/static-resources/{key}/tasks.parquet'
    create_parquet_file(data, file_path, tasks_schema)

