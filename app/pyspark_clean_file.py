from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def spark_clean_file(data_source: str):
    """process sample file
    a. Amount should be rounded up to two digits after decimal.
    b. If loan_status is Charged Off filter out those records.
    c. If purpose is other filter out those records.
    d. If credit score is less than 700 filter out those records."""

    spark = SparkSession.builder.appName("Clean Files").getOrCreate()
    clean_df = spark.read.load(data_source,
                               format='csv',
                               sep=",",
                               inferSchema="true",
                               header="true")
    # conver int_rate to float
    clean_df = clean_df.withColumn("int_rate", F.regexp_replace("int_rate", "%", ""))
    # df = df.withColumn("int_rate",F.col("int_rate").cast(IntegerType)/100)

    col_list = ["int_rate", 'installment', 'dti', 'revol_util', 'total_pymnt', 'total_pymnt_inv',
                'total_rec_prncp', 'total_rec_int', 'recoveries', 'collection_recovery_fee', 'last_pymnt_amnt']
    # put cols as 2 digits after decimals
    for col in col_list:
        clean_df = clean_df.withColumn(col, F.round(clean_df[col], 2))

    # filter data
    # no credit score column so filter out any records with fico_range_low and last_fico_range_low
    clean_df = clean_df.filter((clean_df['loan_status'] != "Charged Off")
                               & (clean_df['purpose'] != 'other')
                               & (clean_df['purpose'] != 'other')
                               & (clean_df['last_fico_range_low'] >= 700)
                               )
    return clean_df
