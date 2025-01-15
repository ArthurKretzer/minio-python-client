from dotenv import load_dotenv

from minio_client import MinioClient

load_dotenv()

# Initialize the MinioClient
client = MinioClient()

# 1. Upload a file to MinIO
file_content = b"Hello, MinIO!"
object_name = "hello_minio.txt"
file_name = "local_hello_minio.txt"

try:
    print("Uploading file...")
    client.save_object(file_content, object_name)
    print("Upload successful!")
except Exception as e:
    print(f"Failed to upload: {e}")

input("Press any key to download the object to a local file.")
# 2. Download the file back
try:
    print("Downloading file...")
    downloaded_file = client.get_object_as_file(object_name, file_name)
    print("Downloaded content:", downloaded_file.getvalue().decode())
except Exception as e:
    print(f"Failed to download: {e}")

input("Press any key to delete the object in minio.")
# 3. Delete the file
try:
    print("Deleting file...")
    client.remove_object(object_name)
    print("Deletion successful!")
except Exception as e:
    print(f"Failed to delete: {e}")
