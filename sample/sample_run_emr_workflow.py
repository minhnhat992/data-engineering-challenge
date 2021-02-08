import boto3
import yaml

from app.emr import EMRJob
from app.helpers import status_poller


def main():
    # read from config credentials file
    with open('../config.yml') as file:
        credentials = yaml.load(file, Loader=yaml.FullLoader)['amazon_creds']

    # setup resource
    emr_client = boto3.client('emr')
    cluster_name = "test_cluster"
    bucket_name = credentials['S3_Bucket_Name']
    log_uri = f"s3://{bucket_name}/logs"
    keep_alive = False
    applications = ['Spark']
    job_flow_role = 'EMR_EC2_DefaultRole'
    service_role = 'EMR_DefaultRole'

    # creating steps
    steps = [{
        'name': 'spark_clean_file',
        f'script_uri': f"s3://{bucket_name}/pyspark_clean_file.py",
        'script_args':
            ['--data_source',
             f's3://{bucket_name}/input/*.csv',
             '--output_uri',
             f's3://{bucket_name}/output/']
    }
        , {
            'name': 'spark_load_file',
            f'script_uri': f"s3://{bucket_name}/pyspark_load_file.py",
            'script_args':
                ['--data_source',
                 f's3://{bucket_name}/output/*.csv',

                 '--database_url',
                 credentials['Database_Path'],

                 '--database_schema',
                 credentials['Database_Schema'],

                 '--database_table',
                 credentials['Database_Table'],

                 '--database_username',
                 credentials['Database_Username'],

                 '--database_password',
                 credentials['Database_Password'],
                 ]
        }]

    # spin up cluster and submit
    emr_job = EMRJob(
        name=cluster_name,
        log_uri=log_uri,
        keep_alive=keep_alive,
        applications=applications,
        job_flow_role=job_flow_role,
        service_role=service_role,
        steps=steps,
        emr_client=emr_client,
        # security_groups=security_groups,
        bucket_name=credentials['S3_Bucket_Name'])

    # initiate cluster to run job
    cluster_id = emr_job.run_job_flow()
    print("generated job steps with cluster id {}".format(cluster_id))

    status_poller(
        "Waiting for cluster, this typically takes several minutes...",
        'RUNNING',
        lambda: emr_job.describe_cluster()['Status']['State'],
    )
    status_poller(
        "Waiting for step to complete...",
        'PENDING',
        lambda: emr_job.list_steps()[0]['Status']['State'])

    status_poller(
        "Waiting for cluster to terminate.",
        'TERMINATED',
        lambda: emr_job.describe_cluster()['Status']['State'],
    )


if __name__ == '__main__':
    main()
