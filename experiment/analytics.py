from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession.builder \
            .master('local') \
            .appName('Analytics') \
            .getOrCreate()

df = spark.read.csv('data/experiment/results', header=True)

df = df.withColumn('time_elapsed', df.time_elapsed.astype('float'))

df_grouped = df.groupby('type', 'compression').agg(f.avg(df.time_elapsed), f.stddev(df.time_elapsed))

df = df.join(df_grouped, on=['type', 'compression'], how='left')

df = df.filter(
    f.col('time_elapsed') <
    (f.col('avg(time_elapsed)') + 3 * f.col('stddev_samp(time_elapsed)'))) \
    .filter(
    f.col('time_elapsed') >
    (f.col('avg(time_elapsed)') - 3 * f.col('stddev_samp(time_elapsed)'))) \
    .select('type', 'compression', 'time_elapsed')

df.groupby('type', 'compression') \
    .agg(f.avg(df.time_elapsed),
         f.stddev(df.time_elapsed)).sort('avg(time_elapsed)') \
    .select('type',
            'compression',
            f.col('avg(time_elapsed)').alias('time_elapsed'),
            f.col('stddev_samp(time_elapsed)').alias('standard_deviation'),) \
    .coalesce(1).write.csv('data/experiment/report', header=True)

