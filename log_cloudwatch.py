"""Xuất sự kiện CloudWatch Logs trong khoảng thời gian chọn."""
from datetime import datetime, timedelta, timezone
import sys
from aws_config import run_export, setting, positive_int


def configure(parser):
    parser.add_argument("--log-group", action="append")
    parser.add_argument("--stream-prefix", default=setting("CLOUDWATCH_LOG_STREAM_PREFIX") or setting("GLUE_RUN_ID"))
    parser.add_argument("--hours", type=positive_int, default=24)
    parser.add_argument("--limit", type=positive_int, default=10000, help="Số sự kiện tối đa mỗi group")


def collect(client, args):
    groups = args.log_group or [v.strip() for v in setting("CLOUDWATCH_LOG_GROUPS", "/aws-glue/jobs/error,/aws-glue/jobs/output").split(",") if v.strip()]
    if not groups:
        raise ValueError("Cần ít nhất một log group.")
    end = datetime.now(timezone.utc)
    start = end - timedelta(hours=args.hours)
    results = []
    for group in groups:
        params = {"logGroupName": group, "startTime": int(start.timestamp()*1000), "endTime": int(end.timestamp()*1000)}
        if args.stream_prefix:
            params["logStreamNamePrefix"] = args.stream_prefix
        events = []
        more = False
        for page in client.get_paginator("filter_log_events").paginate(**params):
            for event in page.get("events", []):
                if len(events) == args.limit:
                    more = True
                    break
                events.append(event)
            if more:
                break
        results.append({"log_group": group, "truncated": more, "events": events})
    return {"start": start, "end": end, "stream_prefix": args.stream_prefix, "scope": "all streams in selected groups" if not args.stream_prefix else "selected stream prefix", "groups": results}


def main():
    return run_export("logs", __doc__, configure, collect)


if __name__ == "__main__":
    sys.exit(main())
