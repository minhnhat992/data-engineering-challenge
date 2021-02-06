import time

from botocore.exceptions import ClientError

from app.emr import logger


class EC2:
    """class to handle EC2 related methods"""

    def __init__(self, ec2_resource):
        self.ec2_resource = ec2_resource

    def create_security_groups(self, prefix):
        """
        Creates Amazon EC2 security groups for the instances contained in the cluster.

        When the cluster is created, Amazon EMRJob adds all required rules to these
        security groups. Because this demo needs only the default rules, it creates
        empty security groups and lets Amazon EMRJob fill them in.

        :param prefix: The name prefix for the security groups.
        :param ec2_resource: The Boto3 Amazon EC2 resource object.
        :return: The newly created security groups.
        """
        try:
            default_vpc = list(self.ec2_resource.vpcs.filter(
                Filters=[{'Name': 'isDefault', 'Values': ['true']}]))[0]
            logger.info("Got default VPC %s.", default_vpc.id)
        except ClientError:
            logger.exception("Couldn't get VPCs.")
            raise
        except IndexError:
            logger.exception("No default VPC in the list.")
            raise

        groups = {'manager': None, 'worker': None}
        for group in groups.keys():
            try:
                groups[group] = default_vpc.create_security_group(
                    GroupName=f'{prefix}-{group}', Description=f"EMRJob {group} group.")
                logger.info(
                    "Created security group %s in VPC %s.",
                    groups[group].id, default_vpc.id)
            except ClientError:
                logger.exception("Couldn't create security group.")
                raise

        return groups

    def delete_security_groups(self, security_groups):
        """
        Deletes the security groups used by the demo. When there are dependencies
        on a security group, it cannot be deleted. Because it can take some time
        to release all dependencies after a cluster is terminated, this function retries
        the delete until it succeeds.

        :param security_groups: The security groups to delete.
        """
        try:
            for sg in security_groups.values():
                sg.revoke_ingress(IpPermissions=sg.ip_permissions)
            max_tries = 5
            while True:
                try:
                    for sg in security_groups.values():
                        sg.delete()
                    break
                except ClientError as error:
                    max_tries -= 1
                    if max_tries > 0 and \
                            error.response['Error']['Code'] == 'DependencyViolation':
                        logger.warning(
                            "Attempt to delete security group got DependencyViolation. "
                            "Waiting for 10 seconds to let things propagate.")
                        time.sleep(10)
                    else:
                        raise
            logger.info("Deleted security groups %s.", security_groups)
        except ClientError:
            logger.exception("Couldn't delete security groups %s.", security_groups)
            raise
