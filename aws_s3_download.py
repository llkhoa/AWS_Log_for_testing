"""Đọc danh sách và thông tin object trong bucket S3 có sẵn."""

import argparse
import sys
from contextlib import closing
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError
from botocore.stub import Stubber
from aws_config import load_config, setting


def show_bucket(client, bucket, prefix=""):
    print(f"Bucket: s3://{bucket}")
    print(f"Prefix: {prefix or '(toàn bộ)'}")
    print("Kích thước (byte) | Cập nhật lần cuối | Storage class | Object key")
    count = total = 0
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            modified = obj.get("LastModified")
            modified = modified.isoformat() if modified else "Không rõ"
            print(f"{obj['Size']} | {modified} | {obj.get('StorageClass', 'Không rõ')} | {obj['Key']}")
            count += 1
            total += obj["Size"]
    if count == 0:
        print("Không có object phù hợp.")
    print(f"Tổng: {count} object, {total:,} byte (theo prefix đã chọn).")
    return count, total


def main():
    load_config()
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", default=setting("S3_BUCKET"), help="Tên bucket có sẵn")
    parser.add_argument("--prefix", default=setting("S3_PREFIX", ""), help="Chỉ liệt kê key bắt đầu bằng prefix, ví dụ data/")
    parser.add_argument("--region", default=setting("AWS_DEFAULT_REGION"), help="Region đã cấu hình hoặc ap-southeast-2")
    parser.add_argument("--profile", default=setting("AWS_PROFILE"), help="AWS profile")
    parser.add_argument("--dry-run", action="store_true", help="Chạy với dữ liệu mô phỏng, không gọi AWS")
    args = parser.parse_args()
    bucket = args.bucket
    if not bucket:
        if args.dry_run:
            bucket = "demo-existing-bucket"
        else:
            try:
                bucket = input("Nhập tên bucket có sẵn: ").strip()
            except (EOFError, KeyboardInterrupt):
                print("\nHãy truyền tên bucket bằng --bucket TEN_BUCKET.", file=sys.stderr)
                return 2
    bucket = bucket.strip()
    if bucket.startswith("s3://"):
        bucket = bucket[5:].rstrip("/")
    if not bucket or "/" in bucket:
        parser.error("Chỉ nhập tên bucket; dùng --prefix để chọn thư mục.")

    try:
        if args.dry_run:
            session = boto3.Session(aws_access_key_id="testing", aws_secret_access_key="testing", region_name=args.region or "ap-southeast-2")
        else:
            session = boto3.Session(profile_name=args.profile, region_name=args.region)
        config = Config(connect_timeout=5, read_timeout=10, retries={"total_max_attempts": 2, "mode": "standard"})
        with closing(session.client("s3", region_name=session.region_name or "ap-southeast-2", config=config)) as client:
            if args.dry_run:
                with Stubber(client) as stubber:
                    for index in range(2):
                        expected = {"Bucket": bucket, "Prefix": args.prefix}
                        response = {"IsTruncated": index == 0, "Contents": [{
                            "Key": f"{args.prefix}example-{index + 1}.csv", "Size": 128,
                            "LastModified": datetime(2026, 1, 1, tzinfo=timezone.utc), "StorageClass": "STANDARD",
                        }]}
                        if index == 0:
                            response["NextContinuationToken"] = "demo-next-page"
                        else:
                            expected["ContinuationToken"] = "demo-next-page"
                        stubber.add_response("list_objects_v2", response, expected)
                    assert show_bucket(client, bucket, args.prefix) == (2, 256)
                    stubber.assert_no_pending_responses()
                print("Chạy thử thành công với 2 trang dữ liệu mô phỏng; không gọi AWS.")
            else:
                show_bucket(client, bucket, args.prefix)
        return 0
    except NoCredentialsError:
        print("Chưa có thông tin xác thực AWS. Chọn profile đã cấu hình hoặc dùng --dry-run.", file=sys.stderr)
        return 1
    except ClientError as exc:
        error = exc.response.get("Error", {})
        print(f"Không đọc được bucket: {error.get('Code')}: {error.get('Message')}", file=sys.stderr)
        if error.get("Code") == "AccessDenied":
            print(f"Cần quyền s3:ListBucket trên arn:aws:s3:::{bucket}; kiểm tra cả giới hạn prefix trong policy.", file=sys.stderr)
        return 1
    except BotoCoreError as exc:
        print(f"Lỗi kết nối/cấu hình AWS: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
