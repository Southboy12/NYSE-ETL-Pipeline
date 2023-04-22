import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, to_date
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from prefect import flow, task



credentials_location = '/home/menka/Downloads/nyc_taxi_creds.json'

conf = SparkConf() \
    .setMaster('spark://Southboy:7077') \
    .setAppName('test') \
    .set("spark.jars", "./lib/gcs-connector-hadoop3-2.2.5.jar") \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)


sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

@task(log_prints=True)
def create_spark_session():
    spark = SparkSession.builder \
        .config(conf=sc.getConf()) \
        .getOrCreate()
    return spark

@task(log_prints=True)
def transform_data(spark, input_data, output_data) -> None:
    """
    Description:
        Convert datatype for date column to datetime and extract year, month and day
    
    :param spark: a spark session instance
    :param input_data: input file path
    :param output_data: output file path

    """
    
    # read data files
    df = spark.read \
        .option("header", "true") \
        .parquet(input_data)

    # Convert date datatype to DateTime
    df = df.withColumn('date', F.lit(to_date(df.date)))

    # create new columns for year, month, and day
    df = df.withColumn('year', F.lit(year(df.date)))
    df = df.withColumn('month', F.lit(month(df.date)))
    df = df.withColumn('day', F.lit(dayofmonth(df.date)))

    # drop the __index_level_0__ column (if it exists)
    if '__index_level_0__' in df.columns:
        df = df.drop('__index_level_0__')

    df.show()
    df.count()

    # write transformed DataFrame to parquet file
    #df.coalesce(1).write.parquet(output_data, compression='gzip', mode='overwrite', partitionBy=['year', 'month'], sortBy='day')
    df.write \
        .mode('overwrite') \
        .partitionBy('date') \
        .orc(output_data, compression='snappy')

@flow(log_prints=True)
def main():
    input_data = "gs://dtc_data_lake_sincere-office-375210/data/split_parquet/"
    output_data = "gs://dtc_data_lake_sincere-office-375210/data/transformed_folder/"
    spark = create_spark_session()
    transform_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()