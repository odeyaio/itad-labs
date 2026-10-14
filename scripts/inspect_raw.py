import argparse
import hashlib
import json
import os
from urllib.parse import urlsplit

import boto3


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", required=True)
    args = parser.parse_args()
    path = urlsplit(args.path)
    if path.scheme != "s3" or not path.netloc or not path.path.strip("/"):
        parser.error("Передайте путь s3://bucket/key")
    bucket = path.netloc
    key = path.path.lstrip("/")
    client = boto3.client(
        "s3",
        endpoint_url="http://" + os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        region_name="us-east-1",
    )
    result = client.get_object(Bucket=bucket, Key=key)
    with result["Body"] as body:
        payload = body.read()
    partition = key.rsplit("/", 1)[0] + "/"
    keys = [
        item["Key"]
        for page in client.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=partition
        )
        for item in page.get("Contents", [])
    ]
    print(
        json.dumps(
            {
                "path": args.path,
                "size_bytes": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
                "etag": result["ETag"],
                "last_modified": result["LastModified"].isoformat(),
                "partition_object_count": len(keys),
                "partition_keys": sorted(keys),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
