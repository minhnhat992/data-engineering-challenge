from unittest.mock import MagicMock

import boto3

from app.emr import EMRJob
from app.helpers import status_poller


# # from emr_usage_demo import create_roles, create_security_groups
# logger = logging.getLogger('example_logger')
#
#
# def create_security_groups(prefix, ec2_resource):
#     """
#     Creates Amazon EC2 security groups for the instances contained in the cluster.
#
#     When the cluster is created, Amazon EMRJob adds all required rules to these
#     security groups. Because this demo needs only the default rules, it creates
#     empty security groups and lets Amazon EMRJob fill them in.
#
#     :param prefix: The name prefix for the security groups.
#     :param ec2_resource: The Boto3 Amazon EC2 resource object.
#     :return: The newly created security groups.
#     """
#     try:
#         default_vpc = list(ec2_resource.vpcs.filter(
#             Filters=[{'Name': 'isDefault', 'Values': ['true']}]))[0]
#         logger.info("Got default VPC %s.", default_vpc.id)
#     except ClientError:
#         logger.exception("Couldn't get VPCs.")
#         raise
#     except IndexError:
#         logger.exception("No default VPC in the list.")
#         raise
#
#     groups = {'manager': None, 'worker': None}
#     for group in groups.keys():
#         try:
#             groups[group] = default_vpc.create_security_group(
#                 GroupName=f'{prefix}-{group}', Description=f"EMRJob {group} group.")
#             logger.info(
#                 "Created security group %s in VPC %s.",
#                 groups[group].id, default_vpc.id)
#         except ClientError:
#             logger.exception("Couldn't create security group.")
#             raise
#
#     return groups
#
#
# ec2_resource = boto3.resource('ec2')
# security_groups = create_security_groups(prefix='tg', ec2_resource=ec2_resource)


def main():
    global job_flow_role
    ec2_resource = boto3.resource('ec2')
    emr_client = boto3.client('emr')
    cluster_name = "test_cluster"
    log_uri = "s3://peeriq-project/logs"
    keep_alive = False
    applications = ['Spark']
    job_flow_role = MagicMock()
    job_flow_role.name = 'EMR_EC2_DefaultRole'
    service_role = MagicMock()
    service_role.name = 'EMR_DefaultRole'
    security_groups = {'manager': ec2_resource.SecurityGroup(id='sg-01f1b29f04ab2d48c'),
                       'worker': ec2_resource.SecurityGroup(id='sg-0ed3992a09c297012')}
    steps = [{
        'name': 'spark_clean_file',
        'script_uri': "s3://peeriq-project/pyspark_clean_file.py",
        'script_args':
            ['--data_source', 's3://peeriq-project/Sample Data.csv', '--output_uri',
             's3://peeriq-project/spark_clean_file.csv']
    }]
    emr_job = EMRJob(
        name=cluster_name,
        log_uri=log_uri,
        keep_alive=keep_alive,
        applications=applications,
        job_flow_role=job_flow_role,
        service_role=service_role,
        steps=steps,
        emr_client=emr_client,
        security_groups=security_groups)

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
