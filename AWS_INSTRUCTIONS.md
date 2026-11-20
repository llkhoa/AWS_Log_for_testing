# AWS Pipeline & `boto3` Log Access Setup (`S3 -> AWS Glue -> Redshift`)

This document captures the AWS infrastructure, IAM Roles, IAM User, IAM Policies, and monitoring setup for the **Olist Data Pipeline** and local **`boto3`** log/telemetry retrieval.

---

## 1. Environment & Resource Overview

| Resource | Value |
| :--- | :--- |
| **AWS Account ID** | `997018780514` |
| **AWS Region** | `ap-southeast-2` |
| **S3 Data Lake Bucket** | `dev-dataops-order-datalake-997018780514-ap-southeast-2-an` |
| **S3 Prefixes** | `raw/` (input CSV/JSON), `curated/` (Parquet output), `temp/`, `scripts/` |
| **AWS Glue Script** | `pipelines/aws_glue_to_redshift/glue_olist_raw_to_curated.py` |
| **Redshift Serverless Workgroup** | `dataops-workgroup` |
| **Redshift Serverless Namespace** | `dataops-namespace` |
| **Redshift Database** | `dev` |
| **Redshift Target Tables** | `public.olist_orders`, `public.olist_order_items` |
| **Redshift Auto-Copy DDL** | `pipelines/aws_glue_to_redshift/redshift_autocopy.sql` |

---

## 2. End-to-End Pipeline Architecture

```text
S3 (raw/*.csv, *.json)
   │
   ▼
AWS Glue PySpark ETL Job (glue_olist_raw_to_curated.py)
   │  ├── Writes execution & error logs -> Amazon CloudWatch Logs
   │  │     (/aws-glue/jobs/output, /aws-glue/jobs/error, /aws-glue/jobs/logs-v2)
   │  └── Writes deduplicated Parquet   -> S3 (curated/olist_orders/, curated/olist_order_items/)
   ▼
Amazon Redshift Serverless (dataops-workgroup / dev)
   │  ├── Auto-Copy jobs (autocopy_olist_orders, autocopy_olist_order_items)
   │  └── Load logs & status in sys_copy_job, sys_copy_job_detail, sys_copy_job_info, sys_load_history
   ▼
Local Python (`boto3`) Client
      ├── Glue API (get_job_runs, get_job_run)
      ├── CloudWatch Logs API (filter_log_events, get_log_events)
      └── Redshift Data API (execute_statement, get_statement_result)
```

---

## 3. IAM Roles (Service Execution)

### A. AWS Glue Job Execution Role
Attached to the AWS Glue ETL job in **Job details**:
- **Managed Policy:** `AWSGlueServiceRole` (grants Glue Catalog access and `logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents` on `/aws-glue/jobs/*`).
- **Custom S3 Policy:** `pipelines/aws_glue_to_redshift/iam_glue_s3_policy.json`
  - `s3:ListBucket`, `s3:GetBucketLocation` on `dev-dataops-order-datalake-997018780514-ap-southeast-2-an`
  - `s3:GetObject` on `raw/*`
  - `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` on `curated/*`, `temp/*`, `scripts/*`

### B. Redshift Serverless Namespace IAM Role
Attached to `dataops-namespace` (**Security and encryption** → **Associated IAM roles**):
- **Role ARN format:** `arn:aws:iam::997018780514:role/<YOUR_REDSHIFT_ROLE_NAME>`
- **Required Permissions:** Read access (`s3:ListBucket`, `s3:GetObject`) on `dev-dataops-order-datalake-997018780514-ap-southeast-2-an/curated/*` so Redshift Auto-Copy (`COPY ... JOB CREATE ... AUTO ON`) can ingest Parquet files automatically.
- **Note:** `COPY ... JOB CREATE` requires the explicit quoted IAM Role ARN in `pipelines/aws_glue_to_redshift/redshift_autocopy.sql` (`IAM_ROLE default` is not supported for Auto-Copy).

---

## 4. AWS Glue Job Configuration

Recommended properties for the Glue job running `glue_olist_raw_to_curated.py`:
- **Glue version:** Glue 4.0 / 5.0 / 6.0
- **Worker type / count:** `G.1X` | `2` workers
- **Job bookmark:** Enabled
- **Job parameters:**
  - `--BUCKET_NAME`: `dev-dataops-order-datalake-997018780514-ap-southeast-2-an`
- **CloudWatch Logging:**
  - In **Job details** → **Advanced properties** → **Continuous logging**, enable **Enable logs in CloudWatch** (if shown for your Glue version; Glue 5.0+ writes directly to `/aws-glue/jobs/output` and `/aws-glue/jobs/error` by default).

---

## 5. Redshift Serverless Setup

Run `pipelines/aws_glue_to_redshift/redshift_autocopy.sql` in **Redshift Query Editor v2** (workgroup `dataops-workgroup`, database `dev`, **Auto-commit ON**):
1. Creates `public.olist_orders` and `public.olist_order_items`.
2. Creates Auto-Copy jobs `autocopy_olist_orders` and `autocopy_olist_order_items` watching `curated/olist_orders/` and `curated/olist_order_items/`.
3. Monitoring tables for pipeline status & errors:
   - `sys_copy_job` — registered Auto-Copy jobs
   - `sys_copy_job_detail` — discovered S3 files (`P` = Pending, `I` = Ingested, `E` = Error)
   - `sys_copy_job_info` — detailed Auto-Copy error messages
   - `sys_load_history` — recent `COPY` execution metrics (`loaded_rows`, `loaded_bytes`, `status`)

---

## 6. IAM User & Policy for Local `boto3` Log Access

### A. IAM User
- **User name:** `dataops-boto3-reader` (or your chosen IAM user name)
- **Console access:** Disabled (programmatic access only)
- **Credentials:** Create an **Access Key** (**Local code** / **CLI**) under **Security credentials** for local `boto3` authentication.

### B. IAM Policy (`dataops-pipeline-logs-readonly`)
Attach this policy to the IAM user to allow read-only access to Glue job runs, CloudWatch Logs, Redshift Data API, and (optionally) the S3 data lake bucket:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ReadGlueCloudWatchLogs",
      "Effect": "Allow",
      "Action": [
        "logs:DescribeLogGroups",
        "logs:DescribeLogStreams",
        "logs:GetLogEvents",
        "logs:FilterLogEvents"
      ],
      "Resource": [
        "arn:aws:logs:ap-southeast-2:997018780514:log-group:/aws-glue/jobs/output:*",
        "arn:aws:logs:ap-southeast-2:997018780514:log-group:/aws-glue/jobs/error:*",
        "arn:aws:logs:ap-southeast-2:997018780514:log-group:/aws-glue/jobs/logs-v2:*"
      ]
    },
    {
      "Sid": "ReadGlueJobRunsAndMetadata",
      "Effect": "Allow",
      "Action": [
        "glue:GetJob",
        "glue:GetJobs",
        "glue:GetJobRun",
        "glue:GetJobRuns",
        "glue:GetTable",
        "glue:GetTables"
      ],
      "Resource": "*"
    },
    {
      "Sid": "QueryRedshiftDataAPI",
      "Effect": "Allow",
      "Action": [
        "redshift-data:ExecuteStatement",
        "redshift-data:DescribeStatement",
        "redshift-data:GetStatementResult",
        "redshift-serverless:GetWorkgroup"
      ],
      "Resource": "*"
    },
    {
      "Sid": "ReadDataLakeBucketOptional",
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetObject"
      ],
      "Resource": [
        "arn:aws:s3:::dev-dataops-order-datalake-997018780514-ap-southeast-2-an",
        "arn:aws:s3:::dev-dataops-order-datalake-997018780514-ap-southeast-2-an/*"
      ]
    }
  ]
}
```

### C. Redshift Database Permissions for the IAM User
If `boto3` (`redshift-data`) queries Redshift Serverless using IAM authentication, run the following in **Query Editor v2** (`dataops-workgroup` / `dev`):

```sql
CREATE USER "IAM:dataops-boto3-reader" PASSWORD DISABLE;
GRANT SELECT ON ALL TABLES IN SCHEMA pg_catalog TO "IAM:dataops-boto3-reader";

-- Optional: allow reading target tables in public schema
GRANT USAGE ON SCHEMA public TO "IAM:dataops-boto3-reader";
GRANT SELECT ON ALL TABLES IN SCHEMA public TO "IAM:dataops-boto3-reader";
```
