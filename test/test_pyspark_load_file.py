from unittest.mock import patch

import boto3
import sure  # noqa
import sure  # noqa
from moto import mock_rds2

from app.pyspark_load_file import spark_load_file


@mock_rds2
def test_create_database(spark_session):
    conn = boto3.client("rds", region_name="us-west-2")
    database = conn.create_db_instance(
        DBInstanceIdentifier="db-master-1",
        AllocatedStorage=10,
        Engine="postgres",
        DBName="myDatabase",
        DBInstanceClass="db.t2.micro",
        LicenseModel="postgresql-license",
        MasterUsername="root",
        MasterUserPassword="test",
        Port=5432,

    )
    db_instance = database["DBInstance"]
    db_instance["AllocatedStorage"].should.equal(10)
    db_instance["DBInstanceClass"].should.equal("db.t2.micro")

    # run function
    with patch('app.pyspark_load_file.SparkSession',
               spark_session):
        spark_load_file(data_source='sample/Sample Data.csv',
                        database_url='jdbc:postgresql://ddb-master-1.us-west-2.rds.amazonaws.com/myDatabase',
                        database_schema='public',
                        database_table='test_table',
                        database_username='root',
                        database_password='test')


def get_sorted_data_frame(data_frame, columns_list):
    return data_frame.sort_values(columns_list).reset_index(drop=True)
