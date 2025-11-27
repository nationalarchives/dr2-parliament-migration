from operator import itemgetter

import boto3
import os
import json
from urllib.parse import urlparse
import io
import sqlite3
import subprocess
from natsort import natsorted

s3_client = boto3.client("s3")
conn = sqlite3.connect("indexes")
cur = conn.cursor()

pa_bucket = os.environ["PA_BUCKET"]


def lambda_handler(event, context):
    metadata_bucket = os.environ["METADATA_BUCKET"]
    for record in event["Records"]:
        body: dict[str, str] = json.loads(record["body"])
        metadata_location = body['metadataLocation']
        metadata_uri = urlparse(metadata_location)
        bucket = metadata_uri.netloc
        key = metadata_uri.path[1:]

        files_bucket = os.environ["FILES_BUCKET"]
        json_metadata = get_json_metadata(bucket, key)
        asset_id = json_metadata[0]['UUID']
        asset_source = json_metadata[0].get('digitalAssetSource', 'Born Digital')
        record_id = get_record_id(asset_id)
        replica_files = []
        for metadata in json_metadata:
            file_id = metadata['fileId']
            file_name = metadata['Filename']
            file_extension = metadata['Filename'].split(".")[-1]
            if file_extension == "tif" or file_extension == "tiff":
                jpg_bytes = convert_file(file_id)
                # s3_client.upload_fileobj(io.BytesIO(jpg_bytes), files_bucket, f"files/{record_id}/{file_id}.jpg")
                file_extension = "jpg"
                file_size = int(len(jpg_bytes) / 1000)
            else:
                # files_copy_source = {"Bucket": pa_bucket, "Key": file_id}
                # s3_client.copy(files_copy_source, files_bucket, f"files/{record_id}/{file_id}.{file_extension}")
                head_object_response = s3_client.head_object(Bucket=pa_bucket, Key=file_id)
                file_size = int(head_object_response['ContentLength'] / 1000)

            replica_files.append({
                "checkSum": metadata['checksum_sha1'],
                "format": file_extension,
                "name": file_id,
                "originalName": file_name,
                "size": file_size
            })

        sorted_replica_files = natsorted(replica_files, key=itemgetter(*['originalName']))
        replica_metadata = {
            'files': sorted_replica_files,
            'replicaId': asset_id,
            'origination': asset_source,
            'totalSize': sum([x['size'] for x in replica_files])
        }
        metadata_body = io.BytesIO(json.dumps(replica_metadata).encode("utf-8"))
        s3_client.upload_fileobj(metadata_body, metadata_bucket, f"metadata/{record_id}.json")


def convert_file(key):
    tiff_obj = io.BytesIO()
    s3_client.download_fileobj(pa_bucket, key, tiff_obj)
    tiff_obj.seek(0)

    process = subprocess.Popen(
        ["/opt/bin/convert", "tiff:-", "jpg:-"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    jpg_bytes, err = process.communicate(input=tiff_obj.read())

    if process.returncode != 0:
        raise RuntimeError(f"Convert failed: {err.decode()}")
    return jpg_bytes


def get_record_id(sdb_id):
    cur.execute("select record_id from indexes where sdb_ref = ?", (sdb_id,))
    row = cur.fetchone()
    return row[0]


def get_json_metadata(bucket, key):
    response = s3_client.get_object(Bucket=bucket, Key=key)
    json_metadata = json.loads(response['Body'].read().decode('utf-8'))
    return json_metadata
