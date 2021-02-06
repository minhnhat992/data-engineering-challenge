import pandas as pd
import pytest

from app.pyspark_clean_file import spark_clean_file

pytestmark = pytest.mark.usefixtures("sql_context")


def test_spark_clean_file(sql_context):
    # set up
    data_source = 'sample/Sample Data.csv'
    expected = pd.read_csv('sample/expected_sample_data.csv')
    col_list = ['id']
    # run function
    actual = spark_clean_file(data_source)

    # convert to df and sort
    actual = get_sorted_data_frame(data_frame=actual.toPandas(),
                                   columns_list=col_list)

    actual_id = actual['id'].tolist()

    expected = get_sorted_data_frame(data_frame=expected,
                                     columns_list=col_list)

    expected_id = expected['id'].tolist()

    # assertion
    assert actual_id == expected_id


def get_sorted_data_frame(data_frame, columns_list):
    return data_frame.sort_values(columns_list).reset_index(drop=True)
