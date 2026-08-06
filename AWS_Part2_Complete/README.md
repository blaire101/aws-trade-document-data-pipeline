# AWS Part 2 — Trade Invoice Data Pipeline

Production-style AWS design for the same post-extraction CINV rules implemented in Part 1.

```text
EventBridge Scheduler
        ↓
Step Functions
        ↓
ECS Fargate SFTP Downloader ← Secrets Manager (SSH key)
        ↓
Client secure SFTP endpoint
        ↓
S3 raw/staged/*.csv
        ↓
AWS Glue (PySpark + Spark SQL)
        ↓
S3 processed/cleaned + failed + review
        ↓
Glue Data Catalog → Athena → Tableau
```

SFTP is a portfolio architecture assumption for secure batch CSV exchange; it is not a statement about any specific client's actual production interface.
