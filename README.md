# **Minh Bui - PeerIQ Data Engineering Challenge**

The ETL job is written in python and requires the following packages (also available in requirements.txt file)

- boto3==1.17.0
- botocore==1.20.0
- moto==1.3.16
- pandas==1.2.1
- pyspark==3.0.1
- pytest==6.2.2

For sample of a complete run, please look into **_sample/sample_run_emr_workflow.py_**.

The flow will start in this particular order

1. Spin up an EMR cluster, taking necessary credentials from **_config.yml_ file**. During the spinning up period, the
   job will also submit 2 main steps
2. First step is  **_spark_clean_file_** : To load any csv files in **_input_** folder and clean them based on these
   criteria:
   - Amount should be rounded up to two digits after decimal.
   - If loan_status is Charged Off filter out those records
   - If purpose is other filter out those records.
   - If credit score is less than 700 filter out those records.
3. **_spark_clean_file_** will then save cleaned files in **_output_** folder
4. Second step is **_spark_load_file_**: To load cleaned csv files in **_output_** to a PostgresSQL table
5. Cluster will automatically terminate once both steps finish,

**_test_** folder contains unitest for **_spark_clean_file_** and **_spark_load_file_**

