"""Lưu snapshot Glue Job Run và log CloudWatch về máy (chỉ đọc AWS)."""
import argparse
from contextlib import ExitStack, closing
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import uuid

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from aws_config import load_config, setting


def export_logs(glue, logs, job, run_id, groups, stream_prefix, destination):
    if run_id:
        run = glue.get_job_run(JobName=job, RunId=run_id)["JobRun"]
    else:
        runs = glue.get_job_runs(JobName=job, MaxResults=1).get("JobRuns", [])
        if not runs:
            raise ValueError("Glue Job chưa có lần chạy nào.")
        run = runs[0]
    run_id = run["Id"]
    prefix = stream_prefix or run_id
    start = int(run["StartedOn"].timestamp() * 1000)
    snapshot = datetime.now(timezone.utc)
    end = int(snapshot.timestamp() * 1000)
    destination.mkdir(parents=True, exist_ok=False)
    summary = {key: run[key] for key in (
        "Id", "JobName", "JobRunState", "StartedOn", "CompletedOn",
        "ExecutionTime", "ErrorMessage", "GlueVersion", "LogGroupName"
    ) if key in run}
    summary.update(snapshot_at=snapshot.isoformat(), stream_prefix=prefix,
                   scope="Glue metadata and CloudWatch logs only; no direct S3 audit or Redshift query logs",
                   groups=[], events=0)
    (destination / "run.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    try:
        with (destination / "events.jsonl").open("w", encoding="utf-8") as output:
            for group in groups:
                status = {"name": group, "events": 0, "status": "reading"}
                summary["groups"].append(status)
                try:
                    pages = logs.get_paginator("filter_log_events").paginate(
                        logGroupName=group, logStreamNamePrefix=prefix,
                        startTime=start, endTime=end,
                    )
                    for page in pages:
                        for event in page.get("events", []):
                            output.write(json.dumps({"logGroup": group, **event}, ensure_ascii=False) + "\n")
                            status["events"] += 1
                            summary["events"] += 1
                    status["status"] = "read" if status["events"] else "no_events"
                except logs.exceptions.ResourceNotFoundException:
                    status["status"] = "group_not_found"
                except (ClientError, BotoCoreError):
                    status["status"] = "read_failed"
                    raise
    finally:
        (destination / "run.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return summary


def main():
    load_config()
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--job", default=setting("GLUE_JOB_NAME"), help="Tên Glue Job")
    parser.add_argument("--run-id", default=setting("GLUE_RUN_ID"), help="Bỏ trống để lấy lần chạy gần nhất")
    parser.add_argument("--region", default=setting("AWS_DEFAULT_REGION", "ap-southeast-2"))
    parser.add_argument("--profile", default=setting("AWS_PROFILE"))
    parser.add_argument("--output", default=setting("PIPELINE_LOG_OUTPUT", "pipeline_logs"), help="Thư mục lưu trên máy")
    parser.add_argument("--log-group", action="append", help="Log group chính xác; có thể truyền nhiều lần")
    parser.add_argument("--stream-prefix", default=setting("CLOUDWATCH_LOG_STREAM_PREFIX"), help="Prefix stream tùy chỉnh; mặc định là Glue run ID")
    args = parser.parse_args()
    if not args.job:
        parser.error("Điền GLUE_JOB_NAME trong .env hoặc truyền --job TEN_JOB.")
    destination = Path(args.output).resolve() / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8])
    groups = args.log_group or [group.strip() for group in setting("CLOUDWATCH_LOG_GROUPS", "/aws-glue/jobs/error,/aws-glue/jobs/output").split(",") if group.strip()]
    if not groups:
        parser.error("CLOUDWATCH_LOG_GROUPS phải có ít nhất một log group.")
    try:
        session = boto3.Session(profile_name=args.profile, region_name=args.region)
        config = Config(connect_timeout=5, read_timeout=15, retries={"mode": "standard", "total_max_attempts": 2})
        with ExitStack() as stack:
            glue = stack.enter_context(closing(session.client("glue", config=config)))
            logs = stack.enter_context(closing(session.client("logs", config=config)))
            result = export_logs(glue, logs, args.job, args.run_id, groups, args.stream_prefix, destination)
        print(f"Run: {result['Id']} | Trạng thái: {result.get('JobRunState')}")
        print(f"Đã lưu {result['events']} sự kiện tại: {destination}")
        print("Đây là snapshot log đã có tại thời điểm lấy; log đến trễ cần lấy lại.")
        if any(group["status"] != "read" for group in result["groups"]):
            print("Có nguồn không tìm thấy log. Kiểm tra run.json, log group và stream prefix; chưa thể kết luận pipeline không có lỗi.")
        return 0
    except (ClientError, BotoCoreError, ValueError, OSError) as exc:
        print(f"Không lấy đủ log: {exc}\nNếu có dữ liệu đã đọc, được lưu tại: {destination}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
