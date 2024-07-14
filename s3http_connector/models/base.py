from typing import Optional

from pydantic import BaseModel, Field, field_validator


class HostMeta(BaseModel):
    host: Optional[str] = Field("localhost", description="Connection host")
    port: Optional[str | int] = Field(8000, description="Connection port")

    @field_validator("port", mode="before")
    def transform_port(cls, v):
        if isinstance(v, int):
            return v
        return int(v)

    @property
    def uri(self) -> str:
        return f"{self.host}:{self.port}"


class S3ConnectionMeta(HostMeta):
    access_key: str
    secret_key: str
    bucket: str

    @property
    def s3_meta(self):
        return {
            "endpoint_url": f"http://{self.host}:{self.port}",
            "aws_access_key_id": self.access_key,
            "aws_secret_access_key": self.secret_key,
        }
