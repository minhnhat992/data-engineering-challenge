from unittest.mock import patch

import boto3
import pandas as pd
from moto import mock_s3

from app.pyspark_load_file import spark_load_file


@mock_s3
def test_spark_clean_file(spark_session):
    # set up mock test s3
    bucket = 'test-bucket'
    s3_client = boto3.client('s3', region_name="us-east-1", endpoint_url="http://127.0.0.1:5000")
    s3_client.create_bucket(Bucket=bucket)
    s3_client.upload_file(Filename='sample/Sample Data.csv', Bucket=bucket, Key='input/Sample Data.csv')
    # run function
    with patch('app.pyspark_clean_file.SparkSession',
               spark_session):
        actual = spark_load_file(data_source=f's3a://{bucket}/input/*.csv',
                                 output_uri=f's3a://{bucket}/output.csv')

        # convert to df and sort
        col_list = ['id']
        actual = get_sorted_data_frame(data_frame=actual.toPandas(),
                                       columns_list=col_list)

        actual_id = actual['id'].tolist()

        expected = pd.read_csv('sample/expected_sample_data.csv')

        expected = get_sorted_data_frame(data_frame=expected,
                                         columns_list=col_list)

        expected_id = expected['id'].tolist()

        # assertion
        assert actual_id == expected_id


def get_sorted_data_frame(data_frame, columns_list):
    return data_frame.sort_values(columns_list).reset_index(drop=True)
