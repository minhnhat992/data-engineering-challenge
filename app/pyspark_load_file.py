import argparse

from pyspark.sql import SparkSession


def spark_load_file(data_source: str,
                    database_url: str,
                    database_schema: str,
                    database_table: str,
                    database_username: str,
                    database_password: str):
    """load cleaned file into a postgres database"""
    # os.environ[
    #     "PYSPARK_SUBMIT_ARGS"
    # ] = '--packages org.postgresql:postgresql:42.2.18 pyspark-shell'
    spark = SparkSession.builder.appName("Load Files").getOrCreate()
    clean_df = spark.read.csv(data_source, header=True, inferSchema="true")
    clean_df.write \
        .jdbc(url=database_url,
              table=f"{database_schema}.{database_table}",
              mode='overwrite',
              properties={"user": database_username,
                          "password": database_password,
                          'driver': 'org.postgresql.Driver'})


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--data_source',
        type=str,
        help="The s3 path to get data to load")
    parser.add_argument(
        '--database_url',
        type=str,
        help="database path")
    parser.add_argument(
        '--database_schema',
        type=str,
        help="database path")
    parser.add_argument(
        '--database_table',
        type=str,
        help="database path")
    parser.add_argument(
        '--database_username',
        type=str,
        help="username")
    parser.add_argument(
        '--database_password',
        type=str,
        help="passowrd")
    args = parser.parse_args()
    spark_load_file(args.data_source,
                    args.database_url,
                    args.database_schema,
                    args.database_table,
                    args.database_username,
                    args.database_password)
