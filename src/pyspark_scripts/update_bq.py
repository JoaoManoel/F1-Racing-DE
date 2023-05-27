import argparse
from pyspark.sql import SparkSession


def save(dataframe, table):
    # Saving the data to BigQuery.
    dataframe\
        .write\
        .format('bigquery')\
        .option('table', table)\
        .option('writeMethod', 'direct')\
        .mode('overwrite')\
        .save()


def driver_standings(spark, source, dataset):
    driver_standings_df = spark.read\
        .format('parquet')\
        .load(f'{source}/driver_standings')

    save(driver_standings_df, f'{dataset}.driver_standings')


def constructor_standings(spark, source, dataset):
    constructor_standings_df = spark.read\
        .format('parquet')\
        .load(f'{source}/constructor_standings')

    save(constructor_standings_df, f'{dataset}.constructor_standings')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run the processing job')
    parser.add_argument('source', type=str, help='the source directory')
    parser.add_argument('dataset', type=str, help='the BigQuery dataset')
    args = parser.parse_args()

    spark = (SparkSession
                .builder
                .master('local[1]')
                .appName('F1-Racing-DE')
                .getOrCreate())

    spark.sparkContext.setLogLevel('WARN')
    spark.conf.set('viewsEnabled', 'true')
    if 'gs://' in args.source:
        spark.conf.set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
        spark.conf.set('fs.AbstractFileSystem.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS')

    driver_standings(spark, args.source, args.dataset)
    constructor_standings(spark, args.source, args.dataset)
