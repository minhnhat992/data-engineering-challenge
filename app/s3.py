# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging

from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class S3:
    """Class to handle S3 service related methods"""

    def __init__(self, bucket_name, s3_resource):
        self.s3_resource = s3_resource
        self.bucket_name = bucket_name
        self.bucket = None

    def create_bucket(self):
        """
        Creates an Amazon S3 bucket

        :param bucket_name: The name of the bucket to create.
        :param s3_resource: The Boto3 Amazon S3 resource object.
        :return: The newly created bucket.
        """
        try:
            self.bucket = self.s3_resource.create_bucket(
                Bucket=self.bucket_name,
                CreateBucketConfiguration={
                    'LocationConstraint': self.s3_resource.meta.client.meta.region_name
                }
            )
            self.bucket.wait_until_exists()
            logger.info("Created bucket %s.", self.bucket_name)
        except ClientError:
            logger.exception("Couldn't create bucket %s.", self.bucket_name)
            raise

        return self.bucket

    def add_object(self, object_name, object_key):
        try:
            self.bucket.upload_file(object_name, object_key)
            logger.info(
                "Uploaded script %s to %s.", object_name,
                f'{object_name}/{object_key}')
        except ClientError:
            logger.exception("Couldn't upload %s to %s.", object_name, self.bucket_name)
            raise

        return self.bucket

    def delete_bucket(self):
        """
           Deletes all objects in the specified bucket and deletes the bucket.

           :param bucket: The bucket to delete.
           """
        try:
            self.bucket.objects.delete()
            self.bucket.delete()
            logger.info("Emptied and removed bucket %s.", self.bucket.name)
        except ClientError:
            logger.exception("Couldn't remove bucket %s.", self.bucket.name)
            raise
