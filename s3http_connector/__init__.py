import boto3
import botocore
import botocore.exceptions

from loguru import logger
from s3http_connector.models.base import S3ConnectionMeta


class S3HttpConnector:
    def __init__(self, meta: S3ConnectionMeta) -> None:
        self.meta: S3ConnectionMeta = meta
        self.client = None
        self.bucket = meta.bucket

    def connect(self, **kwargs):
        try:
            self.client = boto3.client(
                "s3",
                endpoint_url=f"http://{self.meta.host}:{self.meta.port}",
                aws_access_key_id=self.meta.access_key,
                aws_secret_access_key=self.meta.secret_key,
            )
            logger.success("S3HTTP is connected")
        except Exception:
            raise ValueError("Failed to connect to S3HTTP")

    def check_key(self, key):
        if self.client:
            try:
                self.client.head_object(Bucket=self.bucket, Key=key)
                return True
            except botocore.exceptions.ClientError as e:
                if e.response["Error"]["Code"] == "404":
                    logger.warning("Key not found")
                    return False
                else:
                    logger.exception(e)
                    raise e
        else:
            raise ValueError("S3HTTP is not connected")

    def close(self):
        if self.client:
            self.client.close()
        logger.success("S3HTTP is closed")
