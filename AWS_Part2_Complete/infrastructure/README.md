# CloudFormation Infrastructure

The AWS infrastructure is decomposed into **nested CloudFormation stacks** so that networking, storage, monitoring, ingestion, processing, and analytics can evolve independently.

```text
infrastructure/
├── main.yaml
└── templates/
    ├── network.yaml
    ├── storage.yaml
    ├── monitoring.yaml
    ├── processing.yaml
    ├── ingestion.yaml
    └── analytics.yaml
```

## Responsibility by template

| Template | Main resources |
|---|---|
| `network.yaml` | VPC, subnets, Internet Gateway, NAT Gateway, route tables, S3 Gateway VPC Endpoint |
| `storage.yaml` | KMS key, encrypted/versioned S3 data bucket |
| `monitoring.yaml` | CloudWatch log groups, SNS failure topic |
| `processing.yaml` | Glue Database, Glue IAM role, Glue ETL job |
| `ingestion.yaml` | ECS/Fargate SFTP downloader, IAM roles, Step Functions, EventBridge Scheduler |
| `analytics.yaml` | Athena WorkGroup, Athena Interface VPC Endpoint, private Tableau EC2 |
| `main.yaml` | Nested-stack orchestration and parameter wiring |

## Why nested stacks

The original infrastructure was split across large CloudFormation templates. Nested stacks keep each file focused on one domain while preserving AWS-native Infrastructure as Code.

Cross-stack values are passed through child stack `Outputs` and parent stack `Parameters`, for example:

```yaml
DataBucketName: !GetAtt StorageStack.Outputs.DataBucketName
```

## Deployment model

CloudFormation nested-stack `TemplateURL` values must point to templates stored in Amazon S3.

1. Upload the child templates under `templates/` to an S3 deployment bucket.
2. Pass that S3 HTTPS prefix as `TemplateBaseUrl` to `main.yaml`.
3. Deploy only `main.yaml` as the root stack.

Example:

```bash
aws cloudformation deploy \
  --template-file main.yaml \
  --stack-name trade-data \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides \
    TemplateBaseUrl=https://YOUR-BUCKET.s3.YOUR-REGION.amazonaws.com/cloudformation \
    SftpHost=sftp.example.com \
    SftpSecretId=arn:aws:secretsmanager:REGION:ACCOUNT:secret:trade-sftp \
    DownloaderImageUri=ACCOUNT.dkr.ecr.REGION.amazonaws.com/trade-downloader:latest \
    GlueScriptS3Uri=s3://YOUR-ASSET-BUCKET/glue/trade_etl.py \
    GlueSqlBaseUri=s3://YOUR-ASSET-BUCKET/glue/sql \
    TableauAmiId=ami-xxxxxxxxxxxxxxxxx
```

The schedule remains **disabled by default** until the ingestion and Glue paths are manually validated.
