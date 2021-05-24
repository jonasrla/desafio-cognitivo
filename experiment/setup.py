from random import randrange
from datetime import datetime

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
           for i in range(1, 100001)
       ]

[d.update({
    'update_date': fake.date_time_between_dates(d['create_date'],
                                                datetime.now())})
        for d in data]

df = spark.createDataFrame(data)

df.coalesce(1).write.parquet('data/experiment/users')