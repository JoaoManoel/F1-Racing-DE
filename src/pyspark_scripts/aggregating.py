import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    sum,
    when,
    count,
    col,
    current_timestamp
)


def race_results(spark, source, destination):
    races_df = spark.read.format('parquet').load(f'{source}/races')
    circuits_df = spark.read.format('parquet').load(f'{source}/circuits')
    drivers_df = spark.read.format('parquet').load(f'{source}/drivers')
    constructors_df = spark.read.format('parquet').load(f'{source}/constructors')
    results_df = spark.read.format('parquet').load(f'{source}/results')

    races_final_df = races_df.select(
        col('race_id'),
        col('circuit_id'),
        col('race_year'), 
        col('name').alias('race_name'), 
        col('race_timestamp').alias('race_date')
    )

    circuits_final_df = circuits_df.select(
        col('circuit_id'),
        col('location').alias('circuit_location')
    )

    drivers_final_df = drivers_df.select(
        col('driver_id'),
        col('number').alias('driver_number'),
        col('name').alias('driver_name'),
        col('nationality').alias('driver_nationality')
    )

    constructors_final_df = constructors_df.select(
        col('constructor_id'),
        col('name').alias('team')
    )

    results_final_df = results_df.select(
        col('result_id'),
        col('race_id'),
        col('driver_id'),
        col('constructor_id'),
        col('position'),
        col('grid'),
        col('fastest_lap'),
        col('points'),
        col('time').alias('race_time')
    )

    race_circuits_df = races_final_df.join(
        circuits_final_df,
        races_final_df.circuit_id == circuits_final_df.circuit_id,
        how='inner'
    )

    race_results_df = results_final_df\
        .join(
            race_circuits_df, results_final_df.race_id == race_circuits_df.race_id
        )\
        .join(
            drivers_final_df, results_final_df.driver_id == drivers_final_df.driver_id
        )\
        .join(
            constructors_final_df, results_final_df.constructor_id == constructors_final_df.constructor_id
        )

    race_results_final_df = race_results_df.select(
        'race_year',
        'race_name',
        'race_date',
        'circuit_location',
        'driver_name',
        'driver_number',
        'driver_nationality',
        'team',
        'grid',
        'position',
        'fastest_lap',
        'race_time',
        'points'
    )\
    .withColumn('created_date', current_timestamp())

    race_results_final_df.write.mode('overwrite').parquet(f'{destination}/race_results')

    return race_results_final_df


def driver_standings(race_results_df, destination):
    driver_standings_df = race_results_df\
        .groupBy('race_year', 'driver_name', 'driver_nationality', 'team')\
        .agg(
            sum('points').alias('total_points'),
            count(when(col('position') == 1, True)).alias('wins')
        )

    driver_standings_df.write.mode('overwrite').parquet(f'{destination}/driver_standings')


def constructor_standings(race_results_df, destination):
    constructor_standings_df = race_results_df\
        .groupBy('race_year', 'team')\
        .agg(
            sum('points').alias('total_points'),
            count(when(col('position') == 1, True)).alias('wins')
        )

    constructor_standings_df.write.mode('overwrite').parquet(f'{destination}/constructor_standings')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run the processing job')
    parser.add_argument('source', type=str, help='the source directory')
    parser.add_argument('destination', type=str, help='the destination directory')
    args = parser.parse_args()

    spark = (SparkSession
                .builder
                .master('local[1]')
                .appName('F1-Racing-DE')
                .getOrCreate())

    spark.sparkContext.setLogLevel('WARN')

    if 'gs://' in args.source:
        spark.conf.set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
        spark.conf.set('fs.AbstractFileSystem.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS')

    race_results_df = race_results(spark, args.source, args.destination)
    driver_standings(race_results_df, args.destination)
    constructor_standings(race_results_df, args.destination)

    spark.stop()
