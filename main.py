from pyspark.sql import SparkSession
from pyspark.sql.window import Window                                       
from pyspark.sql import functions as f

spark = SparkSession.builder \
            .master('local') \
            .appName('Limpeza') \
            .getOrCreate()

df = spark.read.csv('data/input/users/load.csv', header=True)

windowId = Window.partitionBy('id').orderBy(f.col('update_date').desc())
df = df.withColumn("row", f.row_number().over(windowId)).filter(f.col('row') == 1).drop('row')

df.write.parquet('data/output/users')