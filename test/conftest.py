import os
import subprocess

import boto3
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope='session')
def sql_context():
    spark = SparkSession.builder.appName("Test Files").getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture(autouse=True, scope="session")
def handle_server():
    process = subprocess.Popen(
        "moto_server s3", stdout=subprocess.PIPE,
        shell=True
    )
    # create an s3 connection that points to the moto server.
    s3_conn = boto3.resource(
        "s3", endpoint_url="http://127.0.0.1:5000"
    )
    # create an S3 bucket.
    s3_conn.create_bucket(Bucket="bucket")
    # configure pyspark to use hadoop-aws module.
    # notice that we reference the hadoop version we installed.
    os.environ[
        "PYSPARK_SUBMIT_ARGS"
    ] = '--packages "org.apache.hadoop:hadoop-aws:2.7.3" pyspark-shell'


@pytest.fixture(autouse=True, scope="session")
def spark_session(handle_server):
    spark = SparkSession.builder.getOrCreate()
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    # mock the aws credentials to access s3.
    hadoop_conf.set("fs.s3a.access.key", "dummy-value")
    hadoop_conf.set("fs.s3a.secret.key", "dummy-value")
    # we point s3a to our moto server.
    hadoop_conf.set("fs.s3a.endpoint", "http://127.0.0.1:5000")
    # we need to configure hadoop to use s3a.
    hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    yield spark
    spark.stop()
