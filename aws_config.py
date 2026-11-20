"""Nạp .env cạnh script; biến môi trường hiện có được ưu tiên."""
import os
from pathlib import Path

from dotenv import dotenv_values


def load_config():
    values = dotenv_values(Path(__file__).resolve().parent / ".env", encoding="utf-8-sig", interpolate=False)
    for name, value in values.items():
        if value:
            os.environ.setdefault(name, value)


def setting(name, default=None):
    return os.environ.get(name) or default


def run_export(service, description, configure, collect):
    """CLI chung: cấu hình, đóng client, lưu kết quả và báo lỗi."""
    import argparse
    from contextlib import closing
    from datetime import datetime, timezone
    import json
    import sys
    import uuid
    import boto3
    from botocore.config import Config
    from botocore.exceptions import BotoCoreError, ClientError

    load_config()
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--region", default=setting("AWS_DEFAULT_REGION", "ap-southeast-2"))
    parser.add_argument("--profile", default=setting("AWS_PROFILE"))
    parser.add_argument("--output", default=setting("PIPELINE_LOG_OUTPUT", "pipeline_logs"))
    configure(parser)
    args = parser.parse_args()
    try:
        session = boto3.Session(profile_name=args.profile, region_name=args.region)
        config = Config(connect_timeout=5, read_timeout=15, retries={"mode": "standard", "total_max_attempts": 2})
        with closing(session.client(service, config=config)) as client:
            result = collect(client, args)
        folder = Path(args.output).resolve()
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / f"{service}-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}.json"
        path.write_text(json.dumps({"service": service, "captured_at": datetime.now(timezone.utc).isoformat(), "region": args.region, "data": result}, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print(f"Đã lưu: {path}")
        print("Dữ liệu là snapshot theo phạm vi đã chọn; không tự xác nhận thành công toàn pipeline.")
        return 0
    except (BotoCoreError, ClientError, ValueError, OSError, TimeoutError, RuntimeError) as exc:
        print(f"Không xuất được log: {exc}", file=sys.stderr)
        return 1


def positive_int(value):
    import argparse
    try:
        result = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("Cần nhập số nguyên dương.")
    if result <= 0:
        raise argparse.ArgumentTypeError("Cần nhập số nguyên dương.")
    return result
