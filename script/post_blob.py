"""
Script that dumps the contents of the Mongo database's trip-stops
collection, then uploads the dump to Blob Storage.
"""
from datetime import datetime
import json
import os
import re
import subprocess
import sys
import tempfile

from azure.storage.blob import BlockBlobService, ContentSettings, PublicAccess


def parse_uri(mongo_uri):
    m = re.match(r"mongodb://([^/]+)/(\w+)", mongo_uri)
    return (m.group(1), m.group(2))


def get_mongo():
    mongo_uri = os.environ.get("MONGO_URI")
    if mongo_uri:
        return parse_uri(mongo_uri)
    else:
        return ("localhost", "mbta")


def mongo_dump(since_stamp=None, before_stamp=None):
    """
    Call the 'mongodump' utility to generate a zipped archive file.
    """
    if since_stamp:
        since_dt = datetime.fromtimestamp(since_stamp)
    else:
        # NOTE: Should really be using local midnight on the first of the
        # month--but agency local, not server local.
        dt = datetime.utcnow()
        since_dt = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        # Calculate the stamp:
        since_stamp = int(since_dt.timestamp())

    collection = os.environ.get("MONGO_COLLECTION", "trip-stops")
    (hostname, db) = get_mongo()
    subquery = {"$gte": since_stamp}
    if before_stamp:
        subquery["$lt"] = before_stamp
    query = {"arrival-time": subquery}
    outfile = "{collection}_{dt}.gz".format(
        collection=collection, dt=since_dt.strftime("%Y_%m"))
    subprocess.check_output(
        [
            "mongodump", "--host", hostname, "--db", db, "--collection",
            "trip-stops", "--query", json.dumps(query), "--gzip",
            "--archive=" + outfile
        ],
        timeout=6000)

    return outfile


def get_credentials():
    name = os.environ.get("AZURE_ACCOUNT")
    key = os.environ.get("AZURE_KEY")
    creds = {}
    if not name or not key:
        creds_file = os.environ.get("AZURE_CREDENTIALS") or \
                     os.path.join(os.path.dirname(__file__), "credentials.json")
        try:
            with open(creds_file) as f:
                creds = json.load(f)

        except:
            pass

    return (name or creds.get("account"), key or creds.get("key"))


def upload_blob(outfile: str):
    account_name, account_key = get_credentials()
    container_name = os.environ.get("AZURE_BUCKET", "mbtafyi")

    service = BlockBlobService(
        account_name=account_name, account_key=account_key)
    # Ensure that the container exists:
    service.create_container(
        container_name, public_access=PublicAccess.Container)

    service.create_blob_from_path(container_name,
                                  os.path.basename(outfile), outfile)
    os.unlink(outfile)


def do_main(args):
    try:
        outfile = mongo_dump()
    except subprocess.TimeoutExpired as _exc:
        sys.stderr.write("mongodump took more than 10 minutes to run\n")
        sys.stderr.flush()
        sys.exit(1)

    # Just show the exception if there's an error in the subprocess.
    # except subprocess.CalledProcessError as cpe:

    upload_blob(outfile)


if __name__ == "__main__":
    do_main(sys.argv)
