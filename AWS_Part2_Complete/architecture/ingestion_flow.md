# Part 2A — Data Ingestion

```text
EventBridge → Step Functions → Fargate in private subnet
                            ↘ Secrets Manager (SFTP credential)
Fargate → NAT Gateway → Internet Gateway → Client SFTP endpoint
        ↓ download CSV batches
S3 raw/staged/
        ↓
Glue Spark SQL ETL
        ↓
S3 cleaned / failed / review → Glue Data Catalog
```

Glue starts only after the downloader succeeds.
