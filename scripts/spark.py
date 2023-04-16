import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('tepst') \
    .getOrCreate()

path = "./*.parquet"

df = spark.read \
    .option("header", "true") \
    .parquet(path)

df.show()