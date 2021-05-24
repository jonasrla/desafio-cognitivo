from random import randrange
from datetime import datetime
import json

from faker import Faker
from pyspark.sql import SparkSession

spark = SparkSession.builder \
            .master('local') \
            .appName('Experimento') \
            .getOrCreate()

fake = Faker()

data = [
           {
               'id': i,
               'name': fake.name(),
               'email': fake.email(),
               'phone': fake.phone_number(),
               'address': fake.street_address(),
               'age': randrange(20, 81),
               'create_date': fake.date_time_between('-30M', '-10d')
           }
           for i in range(1, 300001)
       ]

[d.update({
    'update_date': fake.date_time_between_dates(d['create_date'],
                                                datetime.now())})
        for d in data]

df = spark.createDataFrame(data)

with open('parameters.json', 'r') as f:
    parameters = json.load(f)

parquet_compression = parameters['parquet']
orc_compression = parameters['orc']

for compression in parquet_compression:
    df.coalesce(1).write.parquet(f'data/experiment/users_parquet_{compression}', compression=compression)

for compression in orc_compression:
    df.coalesce(1).write.orc(f'data/experiment/users_orc_{compression}', compression=compression)
