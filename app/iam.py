import json

from botocore.exceptions import ClientError

from app.emr import logger


class IAM:
    """Class to handle creating roles and security groups"""

    def __init__(self, iam_resource):
        self.iam_resource = iam_resource

    def create_roles(self, job_flow_role_name, service_role_name):
        """
        Creates IAM roles for the job flow and for the service.

        The job flow role is assumed by the cluster's Amazon EC2 instances and grants
        them broad permission to use services like Amazon DynamoDB and Amazon S3.

        The service role is assumed by Amazon EMRJob and grants it permission to use various
        Amazon EC2, Amazon S3, and other actions.

        For demo purposes, these roles are fairly permissive. In practice, it's more
        secure to restrict permissions to the minimum needed to perform the required
        tasks.

        :param job_flow_role_name: The name of the job flow role.
        :param service_role_name: The name of the service role.
        :param iam_resource: The Boto3 IAM resource object.
        :return: The newly created roles.
        """
        try:
            job_flow_role = self.iam_resource.create_role(
                RoleName=job_flow_role_name,
                AssumeRolePolicyDocument=json.dumps({
                    "Version": "2008-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "ec2.amazonaws.com"
                        },
                        "Action": "sts:AssumeRole"
                    }]
                })
            )
            waiter = self.iam_resource.meta.client.get_waiter('role_exists')
            waiter.wait(RoleName=job_flow_role_name)
            logger.info("Created job flow role %s.", job_flow_role_name)
        except ClientError:
            logger.exception("Couldn't create job flow role %s.", job_flow_role_name)
            raise

        try:
            job_flow_role.attach_policy(
                PolicyArn=
                "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role"
            )
            logger.info("Attached policy to role %s.", job_flow_role_name)
        except ClientError:
            logger.exception("Couldn't attach policy to role %s.", job_flow_role_name)
            raise

        try:
            job_flow_inst_profile = self.iam_resource.create_instance_profile(
                InstanceProfileName=job_flow_role_name)
            job_flow_inst_profile.add_role(RoleName=job_flow_role_name)
            logger.info(
                "Created instance profile %s and added job flow role.", job_flow_role_name)
        except ClientError:
            logger.exception("Couldn't create instance profile %s.", job_flow_role_name)
            raise

        try:
            service_role = self.iam_resource.create_role(
                RoleName=service_role_name,
                AssumeRolePolicyDocument=json.dumps({
                    "Version": "2008-10-17",
                    "Statement": [{
                        "Sid": "",
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "elasticmapreduce.amazonaws.com"
                        },
                        "Action": "sts:AssumeRole"
                    }]
                })
            )
            waiter = self.iam_resource.meta.client.get_waiter('role_exists')
            waiter.wait(RoleName=service_role_name)
            logger.info("Created service role %s.", service_role_name)
        except ClientError:
            logger.exception("Couldn't create service role %s.", service_role_name)
            raise

        try:
            service_role.attach_policy(
                PolicyArn='arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole'
            )
            logger.info("Attached policy to service role %s.", service_role_name)
        except ClientError:
            logger.exception(
                "Couldn't attach policy to service role %s.", service_role_name)
            raise

        return job_flow_role, service_role

    def delete_roles(self, roles):
        """
        Deletes the roles created for this demo.

        :param roles: The roles to delete.
        """
        try:
            for role in roles:
                for policy in role.attached_policies.all():
                    role.detach_policy(PolicyArn=policy.arn)
                for inst_profile in role.instance_profiles.all():
                    inst_profile.remove_role(RoleName=role.name)
                    inst_profile.delete()
                role.delete()
                logger.info("Detached policies and deleted role %s.", role.name)
        except ClientError:
            logger.exception("Couldn't delete roles %s.", [role.name for role in roles])
            raise
