import boto3
import botocore
import botocore.exceptions

from loguru import logger
from s3http_connector.models.base import S3ConnectionMeta
from s3http_connector.utils import client_detector


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


    @client_detector
    def dir_list(self, prefix: str, full_path=False):
        try:
            if not prefix.endswith("/") and prefix != "":
                prefix = f"{prefix}/"
            req_s3http = self.client.list_objects(
                Bucket=self.bucket, Prefix=f"{prefix}", Delimiter="/"
            )

            folders = []
            for folder in req_s3http.get("CommonPrefixes", []):
                if full_path:
                    folders.append(folder.get("Prefix"))
                else:
                    if folder.get("Prefix") != prefix:
                        folders.append(folder.get("Prefix"))

            files = []
            for file in req_s3http.get("Contents", []):
                if file.get("Size", 0) > 0:
                    if full_path:
                        files.append(file.get("Key"))
                    else:
                        files.append(file.get("Key").split("/")[-1])
            return {
                "prefix": prefix,
                "delimiter": "/",
                "folders": folders,
                "files": files,
            }
        except botocore.exceptions.ClientError as e:
            logger.exception(e)
            raise ValueError("Something went wrong")

    @client_detector
    def check_key(self, key):
        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
            return True
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                logger.warning("Key not found")
                return False
            else:
                logger.exception(e)
                raise ValueError("Something went wrong")

    def close(self):
        if self.client:
            self.client.close()
        logger.success("S3HTTP is closed")
