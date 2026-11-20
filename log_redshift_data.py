"""Đọc SYS monitoring views bằng Redshift Data API và lưu kết quả."""
import sys
import time
from aws_config import run_export, setting, positive_int

VIEWS = {"query": "sys_query_history", "load": "sys_load_history", "load-errors": "sys_load_error_detail"}


def configure(parser):
    parser.add_argument("--cluster", default=setting("REDSHIFT_CLUSTER_IDENTIFIER"))
    parser.add_argument("--workgroup", default=setting("REDSHIFT_WORKGROUP_NAME"))
    parser.add_argument("--database", default=setting("REDSHIFT_DATABASE"))
    parser.add_argument("--db-user", default=setting("REDSHIFT_DB_USER"))
    parser.add_argument("--secret-arn", default=setting("REDSHIFT_SECRET_ARN"))
    parser.add_argument("--view", choices=VIEWS, default="load")
    parser.add_argument("--hours", type=positive_int, default=24)
    parser.add_argument("--limit", type=positive_int, default=1000)
    parser.add_argument("--timeout", type=positive_int, default=120)


def collect(client, args):
    if not args.database or bool(args.cluster) == bool(args.workgroup):
        raise ValueError("Điền REDSHIFT_DATABASE và đúng một trong CLUSTER_IDENTIFIER hoặc WORKGROUP_NAME.")
    if args.workgroup and args.db_user:
        raise ValueError("Serverless không dùng DB_USER; để trống REDSHIFT_DB_USER.")
    if args.secret_arn and args.db_user:
        raise ValueError("Chọn SECRET_ARN hoặc DB_USER, không dùng đồng thời.")
    params = {"Database": args.database}
    params["ClusterIdentifier" if args.cluster else "WorkgroupName"] = args.cluster or args.workgroup
    if args.secret_arn:
        params["SecretArn"] = args.secret_arn
    elif args.db_user:
        params["DbUser"] = args.db_user
    view = VIEWS[args.view]
    sql = f"SELECT * FROM {view} WHERE start_time >= dateadd(hour, -{int(args.hours)}, getdate()) AND database_name = current_database() ORDER BY start_time DESC LIMIT {int(args.limit) + 1}"
    statement = client.execute_statement(Sql=sql, **params)["Id"]
    print(f"Đang đọc Redshift, statement ID: {statement}", flush=True)
    deadline = time.monotonic() + args.timeout
    while True:
        info = client.describe_statement(Id=statement)
        if info["Status"] == "FINISHED":
            break
        if info["Status"] in ("FAILED", "ABORTED"):
            raise RuntimeError(f"Statement {statement}: {info.get('Error', info['Status'])}")
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Hết thời gian chờ statement {statement}; truy vấn có thể vẫn đang chạy trên AWS.")
        time.sleep(1)
    rows = []
    columns = []
    for page in client.get_paginator("get_statement_result").paginate(Id=statement):
        if page.get("ColumnMetadata"):
            columns = [column.get("name", str(i)) for i, column in enumerate(page["ColumnMetadata"])]
        for record in page.get("Records", []):
            rows.append({name: None if cell.get("isNull") else next(iter(cell.values())) for name, cell in zip(columns, record)})
    return {"statement_id": statement, "view": view, "database": args.database, "hours": args.hours, "scope": "visible database history, not correlated to a Glue run", "truncated": len(rows) > args.limit, "rows": rows[:args.limit]}


def main():
    return run_export("redshift-data", __doc__, configure, collect)


if __name__ == "__main__":
    sys.exit(main())
