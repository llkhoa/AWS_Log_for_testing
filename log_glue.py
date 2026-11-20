"""Xuất lịch sử trạng thái Glue Job; log chi tiết dùng log_cloudwatch.py."""
import sys
from aws_config import run_export, setting, positive_int


def configure(parser):
    parser.add_argument("--job", default=setting("GLUE_JOB_NAME"))
    parser.add_argument("--run-id", default=setting("GLUE_RUN_ID"))
    parser.add_argument("--limit", type=positive_int, default=20)


def collect(client, args):
    if not args.job:
        raise ValueError("Điền GLUE_JOB_NAME trong .env hoặc dùng --job.")
    more = False
    if args.run_id:
        runs = [client.get_job_run(JobName=args.job, RunId=args.run_id)["JobRun"]]
    else:
        runs = []
        for page in client.get_paginator("get_job_runs").paginate(JobName=args.job):
            for run in page.get("JobRuns", []):
                if len(runs) == args.limit:
                    more = True
                    break
                runs.append(run)
            if more:
                break
    fields = ("Id", "JobName", "JobRunState", "StartedOn", "CompletedOn", "LastModifiedOn", "ExecutionTime", "ErrorMessage", "GlueVersion", "LogGroupName", "Attempt", "DPUSeconds")
    return {"job": args.job, "truncated": more, "runs": [{k: run[k] for k in fields if k in run} for run in runs]}


def main():
    return run_export("glue", __doc__, configure, collect)


if __name__ == "__main__":
    sys.exit(main())
