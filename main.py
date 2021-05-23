from pyspark.sql import SparkSession

spark = SparkSession.builder \
            .master('local') \
            .appName('Limpeza') \
            .getOrCreate()

df = spark.read.csv('data/input/users/load.csv', header=True)

df.write.parquet('data/output/users')