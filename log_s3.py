"""Xuất metadata object S3; không phải lịch sử truy cập S3/CloudTrail."""
import sys
from aws_config import run_export, setting, positive_int


def configure(parser):
    parser.add_argument("--bucket", default=setting("S3_BUCKET"))
    parser.add_argument("--prefix", default=setting("S3_PREFIX", ""))
    parser.add_argument("--limit", type=positive_int, default=1000)


def collect(client, args):
    if not args.bucket:
        raise ValueError("Điền S3_BUCKET trong .env hoặc dùng --bucket.")
    bucket = args.bucket.removeprefix("s3://").rstrip("/")
    objects = []
    more = False
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=args.prefix):
        for obj in page.get("Contents", []):
            if len(objects) == args.limit:
                more = True
                break
            objects.append(obj)
        if more:
            break
    return {"bucket": bucket, "prefix": args.prefix, "scope": "object metadata, not access audit logs", "truncated": more, "count": len(objects), "listed_bytes": sum(obj["Size"] for obj in objects), "objects": objects}


def main():
    return run_export("s3", __doc__, configure, collect)


if __name__ == "__main__":
    sys.exit(main())
