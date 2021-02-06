# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Purpose

Shows how to use the AWS SDK for Python (Boto3) with the Amazon EMRJob API to create
and manage clusters and job steps.
"""
import logging

from botocore.exceptions import ClientError

from app.s3 import logger

logger = logging.getLogger(__name__)


class EMRJob:
    """Class to handle spinning up EMR cluster and run jobs"""

    def __init__(self, name, log_uri, keep_alive, applications, job_flow_role, service_role,
                 security_groups, steps, emr_client):
        self.security_groups = security_groups
        self.steps = steps
        self.emr_client = emr_client
        self.service_role = service_role
        self.job_flow_role = job_flow_role
        self.applications = applications
        self.keep_alive = keep_alive
        self.log_uri = log_uri
        self.cluster_id = None
        self.name = name

    def run_job_flow(self):
        """
        Runs a job flow with the specified steps. A job flow creates a cluster of
        instances and adds steps to be run on the cluster. Steps added to the cluster
        are run as soon as the cluster is ready.

        This example uses the 'emr-5.30.1' release. A list of recent releases can be
        found here:
            https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-release-components.html.

        :param name: The name of the cluster.
        :param log_uri: The URI where logs are stored. This can be an Amazon S3 bucket URL,
                        such as 's3://my-log-bucket'.
        :param keep_alive: When True, the cluster is put into a Waiting state after all
                           steps are run. When False, the cluster terminates itself when
                           the step queue is empty.
        :param applications: The applications to install on each instance in the cluster,
                             such as Hive or Spark.
        :param job_flow_role: The IAM role assumed by the cluster.
        :param service_role: The IAM role assumed by the service.
        :param security_groups: The security groups to assign to the cluster instances.
                                Amazon EMRJob adds all needed rules to these groups, so
                                they can be empty if you require only the default rules.
        :param steps: The job flow steps to add to the cluster. These are run in order
                      when the cluster is ready.
        :param emr_client: The Boto3 EMRJob client object.
        :return: The ID of the newly created cluster.
        """
        try:
            response = self.emr_client.run_job_flow(
                Name=self.name,
                LogUri=self.log_uri,
                ReleaseLabel='emr-5.30.1',
                Instances={
                    'MasterInstanceType': 'm5.xlarge',
                    'SlaveInstanceType': 'm5.xlarge',
                    'InstanceCount': 3,
                    'KeepJobFlowAliveWhenNoSteps': self.keep_alive,
                    'EmrManagedMasterSecurityGroup': self.security_groups['manager'].id,
                    'EmrManagedSlaveSecurityGroup': self.security_groups['worker'].id,
                },
                Steps=[{
                    'Name': step['name'],
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['spark-submit', '--deploy-mode', 'cluster',
                                 step['script_uri'], *step['script_args']]
                    }
                } for step in self.steps],
                Applications=[{
                    'Name': app
                } for app in self.applications],
                JobFlowRole=self.job_flow_role.name,
                ServiceRole=self.service_role.name,
                EbsRootVolumeSize=10,
                VisibleToAllUsers=True
            )
            self.cluster_id = response['JobFlowId']
            logger.info("Created cluster %s.", self.cluster_id)
        except ClientError:
            logger.exception("Couldn't create cluster.")
            raise
        else:
            return self.cluster_id

    def describe_cluster(self):
        """
        Gets detailed information about a cluster.

        :param cluster_id: The ID of the cluster to describe.
        :param emr_client: The Boto3 EMRJob client object.
        :return: The retrieved cluster information.
        """
        try:
            response = self.emr_client.describe_cluster(ClusterId=self.cluster_id)
            cluster = response['Cluster']
            logger.info("Got data for cluster %s.", cluster['Name'])
        except ClientError:
            logger.exception("Couldn't get data for cluster %s.", self.cluster_id)
            raise
        else:
            return cluster

    def terminate_cluster(self):
        """
        Terminates a cluster. This terminates all instances in the cluster and cannot
        be undone. Any data not saved elsewhere, such as in an Amazon S3 bucket, is lost.

        :param cluster_id: The ID of the cluster to terminate.
        :param emr_client: The Boto3 EMRJob client object.
        """
        try:
            self.emr_client.terminate_job_flows(JobFlowIds=[self.cluster_id])
            logger.info("Terminated cluster %s.", self.cluster_id)
        except ClientError:
            logger.exception("Couldn't terminate cluster %s.", self.cluster_id)
            raise

    def add_step(self, name, script_uri, script_args):
        """
        Adds a job step to the specified cluster. This example adds a Spark
        step, which is run by the cluster as soon as it is added.

        :param cluster_id: The ID of the cluster.
        :param name: The name of the step.
        :param script_uri: The URI where the Python script is stored.
        :param script_args: Arguments to pass to the Python script.
        :param emr_client: The Boto3 EMRJob client object.
        :return: The ID of the newly added step.
        """
        try:
            response = self.emr_client.add_job_flow_steps(
                JobFlowId=self.cluster_id,
                Steps=[{
                    'Name': name,
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['spark-submit', '--deploy-mode', 'cluster',
                                 script_uri, *script_args]
                    }
                }])
            step_id = response['StepIds'][0]
            logger.info("Started step with ID %s", step_id)
        except ClientError:
            logger.exception("Couldn't start step %s with URI %s.", name, script_uri)
            raise
        else:
            return step_id

    def list_steps(self):
        """
        Gets a list of steps for the specified cluster. In this example, all steps are
        returned, including completed and failed steps.

        :param cluster_id: The ID of the cluster.
        :param emr_client: The Boto3 EMRJob client object.
        :return: The list of steps for the specified cluster.
        """
        try:
            response = self.emr_client.list_steps(ClusterId=self.cluster_id)
            steps = response['Steps']
            logger.info("Got %s steps for cluster %s.", len(steps), self.cluster_id)
        except ClientError:
            logger.exception("Couldn't get steps for cluster %s.", self.cluster_id)
            raise
        else:
            return steps

    def describe_step(self, step_id=None):
        """
        Gets detailed information about the specified step, including the current state of
        the step.

        :param cluster_id: The ID of the cluster.
        :param step_id: The ID of the step.
        :param emr_client: The Boto3 EMRJob client object.
        :return: The retrieved information about the specified step.
        """
        try:
            response = self.emr_client.describe_step(ClusterId=self.cluster_id, StepId=step_id)
            step = response['Step']
            logger.info("Got data for step %s.", step_id)
        except ClientError:
            logger.exception("Couldn't get data for step %s.", step_id)
            raise
        else:
            return step
