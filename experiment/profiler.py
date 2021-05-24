import json
from datetime import datetime
from pyspark.sql import SparkSession

spark = SparkSession.builder \
            .master('local') \
            .appName('Profiler') \
            .getOrCreate()

with open('parameters.json', 'r') as f:
    parameters = json.load(f)

def reader(type, compression):
    if type == 'parquet':
        f = spark.read.parquet
    elif type == 'orc':
        f = spark.read.orc
    else:
        raise ValueError(f'Got {type} as format; Expected `parquet` or `orc`')
    
    f(f'data/experiment/users_{type}_{compression}').collect()

def profile(f, *args):
    tic = datetime.now()
    f(*args)
    toc = datetime.now()
    spark.sql("CLEAR CACHE").collect()
    return (toc - tic).total_seconds()

experiment = []
for type, compressions in parameters.items():
    for compression in compressions:
        experiment.extend(
            [
                {
                    'time_elapsed': profile(reader, type, compression),
                    'type': type,
                    'compression': compression
                }
                for i in range(30)])

spark.createDataFrame(experiment).coalesce(1).write.csv('data/experiment/results', header=True)