from datetime import datetime
import os
import re
import subprocess
import tempfile
from azure.storage.blob import BlockBlobService

import post_blob


def get_blobs():
    collection = os.environ.get("MONGO_COLLECTION", "trip-stops")
    account_name, account_key = post_blob.get_credentials()
    container_name = os.environ.get("AZURE_BUCKET", "mbtafyi")

    service = BlockBlobService(
        account_name=account_name, account_key=account_key)
    return service.list_blobs(container_name)


def get_dump_blobs():
    return (blob for blob in get_blobs()
            if re.match(r"trip-stops_\d+_\d+\.gz", blob.name))


def import_file():
    hostname, db = post_blob.get_mongo()

    for blob in get_dump_blobs():
        print("Restoring from blob: " + blob.name)
        filename = tempfile.mktemp(suffix=".gz")
        blob.get_blob_to_path(filename)
        subprocess.check_output([
            "mongorestore", "--host", hostname, "--db", db, "--collection",
            "trip-stops", "--gzip", "--archive=" + filename
        ])

        os.unlink(filename)
