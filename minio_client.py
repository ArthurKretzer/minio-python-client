import io
import os
import traceback

import ratelimit
import urllib3
from backoff import expo, on_exception
from minio import Minio, S3Error

from logger.logger import log

# Disable SSL check warning.
urllib3.disable_warnings()
logger = log(f"{__file__}")


def str_to_bool(s):
    return s.lower() in ["true", "1", "t", "y", "yes"]


def giveup_if_no_such_key(e):
    # Return True to give up retrying if the error code is 'NoSuchKey'
    return isinstance(e, S3Error) and e.code == "NoSuchKey"


class MinioClient:
    """
    Client class for interacting with MinIO.
    """

    def __init__(self) -> None:
        """
        Initialize the Client instance.
        """
        secure = str_to_bool(os.getenv("MINIO_SECURE_ACCESS", "True"))
        check_ssl = str_to_bool(os.getenv("MINIO_VERIFY_SSL", "False"))
        self._minio_client = Minio(
            endpoint=os.environ["MINIO_ENDPOINT"],
            secure=secure,
            access_key=os.environ["MINIO_ACCESS_KEY"],
            secret_key=os.environ["MINIO_SECRET_KEY"],
            cert_check=check_ssl,
        )

        self._bucket_name = os.environ["MINIO_BUCKET"]
        self._destination_repo = os.environ["MINIO_DESTINATION_REPO"]

    @on_exception(
        expo, (ratelimit.exception.RateLimitException, S3Error), max_tries=10
    )
    @ratelimit.limits(calls=3, period=1)
    def save_object(self, object: any, object_path: str) -> None:
        """
        Put an object on MinIO bucket.
        :param object_path: The object to be stored.
        :param object_path: The name of the object.
        """
        try:
            self._minio_client.put_object(
                bucket_name=self._bucket_name,
                object_name=f"{self._destination_repo}/{object_path}",
                data=io.BytesIO(object),
                length=len(object),
            )
        except Exception as e:
            logger.info(f"Erro ao salvar o arquivo: {str(e)}")
            traceback.print_exc()
            raise

    @on_exception(
        expo,
        (ratelimit.exception.RateLimitException, S3Error),
        max_tries=10,
        giveup=giveup_if_no_such_key,
    )
    @ratelimit.limits(calls=3, period=1)
    def get_object(self, object_path: str) -> io.BytesIO:
        """
        Get an object from MinIO bucket as a BytesIO object.
        :param object_path: The name of the object to retrieve.
        :return: The BytesIO object containing the retrieved data.
        """
        try:
            return io.BytesIO(
                self._minio_client.get_object(
                    bucket_name=self._bucket_name,
                    object_name=f"{self._destination_repo}/{object_path}",
                ).data
            )
        except Exception as e:
            logger.info(f"Erro ao obter o arquivo: {str(e)}")
            traceback.print_exc()
            raise

    @on_exception(
        expo, (ratelimit.exception.RateLimitException, S3Error), max_tries=10
    )
    @ratelimit.limits(calls=3, period=1)
    def save_file_as_object(self, object_path: str, file_path: str) -> None:
        """
        Put a file from the local system into the MinIO bucket.
        :param object_path: The name of the object in the MinIO bucket.
        :param file_path: The path to the file in the local system.
        """
        try:
            self._minio_client.fput_object(
                self._bucket_name,
                f"{self._destination_repo}/{object_path}",
                file_path,
            )
        except Exception as e:
            logger.info(f"Erro ao salvar o arquivo: {str(e)}")
            traceback.print_exc()
            raise

    @on_exception(
        expo,
        (ratelimit.exception.RateLimitException, S3Error),
        max_tries=10,
        giveup=giveup_if_no_such_key,
    )
    @ratelimit.limits(calls=3, period=1)
    def get_object_as_file(self, object_path: str, file_path: str) -> None:
        """
        Get an object from MinIO bucket and
        save it as a file on the local system.

        :param object_path: The name of the object in the MinIO bucket.
        :param file_path: The path to save the file in the local system.
        """
        try:
            self._minio_client.fget_object(
                self._bucket_name,
                f"{self._destination_repo}/{object_path}",
                file_path,
            )
        except Exception as e:
            logger.info(f"Erro ao obter o arquivo: {str(e)}")
            traceback.print_exc()
            raise

    @on_exception(
        expo,
        (ratelimit.exception.RateLimitException, S3Error),
        max_tries=10,
        giveup=giveup_if_no_such_key,
    )
    @ratelimit.limits(calls=3, period=1)
    def remove_object(self, object_name: str) -> None:
        """
        Remove a file from the MinIO bucket.
        :param object_name: The name of the object in the MinIO bucket.
        """
        try:
            self._minio_client.remove_object(
                self._bucket_name, f"{self._destination_repo}/{object_name}"
            )
        except Exception as e:
            logger.info(f"Erro ao salvar remover o arquivo: {str(e)}")
            traceback.print_exc()
            raise
