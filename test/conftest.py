import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope='session')
def sql_context():
    spark = SparkSession.builder.appName("Test Files").getOrCreate()
    yield spark
    spark.stop()
