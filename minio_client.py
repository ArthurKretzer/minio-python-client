import io
import os

from minio import Minio


class MinioClient:
    """
    Client class for interacting with MinIO.
    """

    def __init__(self) -> None:
        """
        Initialize the Client instance.
        """
        self._minio_client = Minio(
            endpoint=os.environ["MINIO_ENDPOINT"],
            secure=False,
            access_key=os.environ["MINIO_ACCESS_KEY"],
            secret_key=os.environ["MINIO_SECRET_KEY"],
        )

        self._bucket_name = os.environ["MINIO_BUCKET"]

    def save_object(self, object: any, object_path: str) -> None:
        """
        Put an object on MinIO bucket.
        :param object_path: The object to be stored.
        :param object_path: The name of the object.
        """
        self._minio_client.put_object(
            bucket_name=self._bucket_name,
            object_name=f"{object_path}",
            data=io.BytesIO(object),
            length=len(object),
        )

    def get_object(self, object_path: str) -> io.BytesIO:
        """
        Get an object from MinIO bucket as a BytesIO object.
        :param object_path: The name of the object to retrieve.
        :return: The BytesIO object containing the retrieved data.
        """
        return io.BytesIO(
            self._minio_client.get_object(
                bucket_name=self._bucket_name,
                object_name=f"{object_path}",
            ).data
        )

    def save_file_as_object(self, object_path: str, file_path: str) -> None:
        """
        Put a file from the local system into the MinIO bucket.
        :param object_path: The name of the object in the MinIO bucket.
        :param file_path: The path to the file in the local system.
        """
        self._minio_client.fput_object(self._bucket_name, f"{object_path}", file_path)

    def get_object_as_file(self, object_path: str, file_path: str) -> None:
        """
        Get an object from MinIO bucket and save it as a file on the local system.
        :param object_path: The name of the object in the MinIO bucket.
        :param file_path: The path to save the file in the local system.
        """
        self._minio_client.fget_object(self._bucket_name, f"{object_path}", file_path)

    def remove_object(self, object_name: str) -> None:
        """
        Remove a file from the MinIO bucket.
        :param object_name: The name of the object in the MinIO bucket.
        """
        self._minio_client.remove_object(self._bucket_name, f"{object_name}")
