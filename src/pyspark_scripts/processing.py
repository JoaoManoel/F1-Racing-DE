import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    IntegerType,
    DoubleType,
    DateType,
    FloatType
)
from pyspark.sql.functions import (
    current_timestamp,
    col,
    when,
    lit,
    concat,
    to_timestamp
)


def circuits(spark, source, destination):
    fields = [
        StructField('circuitId', IntegerType(), False),
        StructField('circuitRef', StringType(), True),
        StructField('name', StringType(), True),
        StructField('location', StringType(), True),
        StructField('country', StringType(), True),
        StructField('lat', DoubleType(), True),
        StructField('lng', DoubleType(), True),
        StructField('alt', IntegerType(), True),
        StructField('url', StringType(), True),
    ]

    circuits_schema = StructType(fields=fields)
    circuits_df = (spark
                    .read
                    .option('header', True)
                    .schema(circuits_schema)
                    .format('csv')
                    .load(f'{source}/circuits.csv'))

    circuits_renamed_df = circuits_df \
        .withColumnRenamed('circuitId', 'circuit_id') \
        .withColumnRenamed('circuitRef', 'circuit_ref') \
        .withColumnRenamed('lat', 'latitude') \
        .withColumnRenamed('lng', 'longitude') \
        .withColumnRenamed('alt', 'altitde')

    circuits_ingestion_df = circuits_renamed_df.withColumn('ingestion_date', current_timestamp())
    circuits_final_df = circuits_ingestion_df.drop(col('url'))

    circuits_final_df.write.mode('overwrite').parquet(f'{destination}/circuits')


def races(spark, source, destination):
    fields = [
        StructField('raceId', IntegerType(), False),
        StructField('year', IntegerType(), True),
        StructField('round', IntegerType(), True),
        StructField('circuitId', IntegerType(), True),
        StructField('name', StringType(), True),
        StructField('date', StringType(), True),
        StructField('time', StringType(), True),
        StructField('url', StringType(), True),
        StructField('fp1_date', DateType(), True),
        StructField('fp1_time', StringType(), True),
        StructField('fp2_date', DateType(), True),
        StructField('fp2_time', StringType(), True),
        StructField('fp3_date', DateType(), True),
        StructField('fp3_time', StringType(), True),
        StructField('quali_date', DateType(), True),
        StructField('quali_time', StringType(), True),
        StructField('sprint_date', DateType(), True),
        StructField('sprint_time', StringType(), True)
    ]

    races_schema = StructType(fields=fields)
    races_df = spark \
        .read \
        .option('header', True) \
        .schema(races_schema) \
        .format('csv') \
        .load(f'{source}/races.csv')

    for column in races_df.columns:
        races_df = races_df.withColumn(
            column,
            when(col(column) == '\\N', None) \
                .otherwise(col(column))
        )

    races_renamed_df = races_df \
        .withColumnRenamed('raceId', 'race_id') \
        .withColumnRenamed('year', 'race_year') \
        .withColumnRenamed('circuitId', 'circuit_id')

    ts_column = concat(col('date'), lit(' '), col('time'))
    race_final_df = races_renamed_df \
        .withColumn('race_timestamp', to_timestamp(ts_column, 'yyyy-MM-dd HH:mm:ss')) \
        .withColumn('ingestion_date', current_timestamp()) \
        .drop('date', 'time', 'url')

    race_final_df.write.partitionBy('race_year').mode('overwrite').parquet(f'{destination}/races')


def constructors(spark, source, destination):
    fields = [
        StructField('constructorId', IntegerType(), False),
        StructField('constructorRef', StringType(), True),
        StructField('name', StringType(), True),
        StructField('nationality', StringType(), True),
        StructField('url', StringType(), True),
    ]

    constructors_schema = StructType(fields=fields)
    constructors_df = spark \
        .read \
        .option('header', True) \
        .schema(constructors_schema) \
        .format('csv') \
        .load(f'{source}/constructors.csv')

    constructors_final_df = constructors_df \
        .withColumnRenamed('constructorId', 'constructor_id') \
        .withColumnRenamed('constructorref', 'constructo_ref') \
        .withColumn('ingestion_date', current_timestamp()) \
        .drop('url')

    constructors_final_df.write.mode('overwrite').parquet(f'{destination}/constructors')


def drivers(spark, source, destination):
    fields = [
        StructField('driverId', IntegerType(), False),
        StructField('driverRef', StringType(), True),
        StructField('number', IntegerType(), True),
        StructField('code', IntegerType(), True),
        StructField('forename', StringType(), True),
        StructField('surname', StringType(), True),
        StructField('dob', StringType(), True),
        StructField('nationality', StringType(), True),
        StructField('url', StringType(), True),
    ]

    drivers_schema = StructType(fields=fields)
    drivers_df = spark \
        .read \
        .option('header', True) \
        .schema(drivers_schema) \
        .format('csv') \
        .load(f'{source}/drivers.csv')

    drivers_renamed_df = drivers_df \
        .withColumnRenamed('driverId', 'driver_id') \
        .withColumnRenamed('driverRef', 'driver_ref')

    drivers_transformed_df = drivers_renamed_df \
        .withColumn('name', concat(col('forename'), lit(' '), col('surname'))) \
        .withColumn('ingestion_date', current_timestamp())

    drivers_final_df = drivers_transformed_df \
        .drop('forename', 'surname', 'url') \
        .select('driver_id', 'driver_ref', 'name', 'number', 'code', 'dob', 'nationality', 'ingestion_date')

    drivers_final_df.write.mode('overwrite').parquet(f'{destination}/drivers')


def results(spark, source, destination):
    fields = [
        StructField('resultId', IntegerType(), False),
        StructField('raceId', IntegerType(), True),
        StructField('driverId', IntegerType(), True),
        StructField('constructorId', IntegerType(), True),
        StructField('number', IntegerType(), True),
        StructField('grid', IntegerType(), True),
        StructField('position', IntegerType(), True),
        StructField('positionText', IntegerType(), True),
        StructField('positionOrder', IntegerType(), True),
        StructField('points', IntegerType(), True),
        StructField('laps', IntegerType(), True),
        StructField('time', StringType(), True),
        StructField('milliseconds', IntegerType(), True),
        StructField('fastestLap', IntegerType(), True),
        StructField('rank', IntegerType(), True),
        StructField('fastestLapTime', StringType(), True),
        StructField('fastestLapSpeed', FloatType(), True),
        StructField('statusId', IntegerType(), True)
    ]

    results_schema = StructType(fields=fields)
    results_df = spark \
        .read \
        .option('header', True) \
        .schema(results_schema) \
        .format('csv') \
        .load(f'{source}/results.csv')

    for column in results_df.columns:
        results_df = results_df.withColumn(
            column,
            when(col(column) == '\\N', None) \
                .otherwise(col(column))
        )

    results_renamed_df = results_df \
        .withColumnRenamed('resultId', 'result_id') \
        .withColumnRenamed('raceId', 'race_id') \
        .withColumnRenamed('driverId', 'driver_id') \
        .withColumnRenamed('constructorId', 'constructor_id') \
        .withColumnRenamed('positionText', 'position_text') \
        .withColumnRenamed('positionOrder', 'position_order') \
        .withColumnRenamed('fastestLap', 'fastest_lap') \
        .withColumnRenamed('fastestLapTime', 'fastest_lap_time') \
        .withColumnRenamed('fastestLapSpeed', 'fastest_lap_speed')

    results_final_df = results_renamed_df \
        .withColumn('ingestion_date', current_timestamp()) \
        .drop('statusId')

    results_final_df \
        .write \
        .mode('overwrite') \
        .partitionBy('race_id') \
        .parquet(f'{destination}/results')


def pitstops(spark, source, destination):
    fields = [
        StructField('raceId', IntegerType(), False),
        StructField('driverId', IntegerType(), True),
        StructField('stop', IntegerType(), True),
        StructField('lap', IntegerType(), True),
        StructField('time', StringType(), True),
        StructField('duration', FloatType(), True),
        StructField('milliseconds', IntegerType(), True)
    ]

    pitstops_schema = StructType(fields=fields)
    pitstops_df = spark\
        .read \
        .option('header', True) \
        .schema(pitstops_schema) \
        .format('csv') \
        .load(f'{source}/pit_stops.csv')

    pitstops_renamed_df = pitstops_df \
        .withColumnRenamed('raceId', 'race_id') \
        .withColumnRenamed('driverId', 'driver_id')

    pitstops_final_df = pitstops_renamed_df.withColumn('ingestion_date', current_timestamp())
    pitstops_final_df.write.mode('overwrite').parquet(f'{destination}/pitstops')


def lap_times(spark, source, destination):
    fields = [
        StructField('raceId', IntegerType(), False),
        StructField('driverId', IntegerType(), True),
        StructField('lap', IntegerType(), True),
        StructField('position', IntegerType(), True),
        StructField('time', StringType(), True),
        StructField('milliseconds', IntegerType(), True)
    ]

    laptimes_schema = StructType(fields=fields)
    laptimes_df = spark \
        .read \
        .option('header', True) \
        .schema(laptimes_schema) \
        .format('csv') \
        .load(f'{source}/lap_times.csv')

    laptimes_renamed_df = laptimes_df \
        .withColumnRenamed('raceId', 'race_id') \
        .withColumnRenamed('driverId', 'driver_id') 

    laptimes_final_df = laptimes_renamed_df.withColumn('ingestion_date', current_timestamp())
    laptimes_final_df.write.mode('overwrite').parquet(f'{destination}/laptimes')


def qualifying(spark, source, destination):
    fields = [
        StructField('qualifyId', IntegerType(), False),
        StructField('raceId', IntegerType(), True),
        StructField('driverId', IntegerType(), True),
        StructField('constructorId', IntegerType(), True),
        StructField('number', IntegerType(), True),
        StructField('position', IntegerType(), True),
        StructField('q1', StringType(), True),
        StructField('q2', StringType(), True),
        StructField('q3', StringType(), True),
    ]

    qualifying_schema = StructType(fields=fields)
    qualifying_df = spark \
        .read \
        .option('header', True) \
        .schema(qualifying_schema) \
        .format('csv') \
        .load(f'{source}/qualifying.csv')

    for column in qualifying_df.columns:
        qualifying_df = qualifying_df.withColumn(
            column,
            when(col(column) == '\\N', None).otherwise(col(column))
        )

    qualifying_renamed_df = qualifying_df \
        .withColumnRenamed('qualifyingId', 'qualifying_id') \
        .withColumnRenamed('raceId', 'race_id') \
        .withColumnRenamed('driverId', 'driver_id') \
        .withColumnRenamed('constructorId', 'constructor_id')

    qualifying_final_df = qualifying_renamed_df.withColumn('ingestion_date', current_timestamp())
    qualifying_final_df.write.mode('overwrite').parquet(f'{destination}/qualifying')


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

    circuits(spark, args.source, args.destination)
    races(spark, args.source, args.destination)
    constructors(spark, args.source, args.destination)
    drivers(spark, args.source, args.destination)
    results(spark, args.source, args.destination)
    pitstops(spark, args.source, args.destination)
    lap_times(spark, args.source, args.destination)
    qualifying(spark, args.source, args.destination)

    spark.stop()
