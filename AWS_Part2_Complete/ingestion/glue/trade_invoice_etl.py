"""AWS Glue ETL for trade invoice data.

Python controls the job; Spark SQL implements the business transformations.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from urllib.parse import urlparse

import boto3
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F


SQL_FILES = [
    ("01_staging.sql", "staging"),
    ("02_validation.sql", "validated"),
    ("03_dedup.sql", "dedup_ranked"),
    ("04_enrich.sql", "enriched"),
    ("05_iqr_stats.sql", "iqr_stats"),
    ("06_cleaned.sql", "cleaned"),
    ("07_invalid_failed.sql", "invalid_failed"),
    ("08_duplicate_failed.sql", "duplicate_failed"),
    ("09_review.sql", "review"),
]


def parse_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"Expected S3 URI, got: {uri}")
    return parsed.netloc, parsed.path.lstrip("/")


def load_sql_from_s3(s3, base_uri: str, filename: str) -> str:
    bucket, prefix = parse_s3_uri(base_uri.rstrip("/") + "/" + filename)
    body = s3.get_object(Bucket=bucket, Key=prefix)["Body"].read()
    return body.decode("utf-8")


def render_sql(sql: str, params: dict[str, str]) -> str:
    for key, value in params.items():
        sql = sql.replace("{{" + key + "}}", str(value))
    if "{{" in sql:
        raise ValueError("Unresolved SQL template parameter")
    return sql


def write_catalog_table(
    glue_context: GlueContext,
    df,
    path: str,
    database: str,
    table: str,
    partition_keys: list[str] | None = None,
) -> None:
    frame = DynamicFrame.fromDF(df, glue_context, table)
    sink = glue_context.getSink(
        connection_type="s3",
        path=path,
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=partition_keys or [],
    )
    sink.setFormat("glueparquet")
    sink.setCatalogInfo(catalogDatabase=database, catalogTableName=table)
    sink.writeFrame(frame)


def put_json(s3, uri: str, payload: dict) -> None:
    bucket, key = parse_s3_uri(uri)
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, indent=2, default=str).encode("utf-8"),
        ContentType="application/json",
    )


def main() -> None:
    arg_names = [
        "JOB_NAME",
        "SOURCE_PATH",
        "TARGET_PATH",
        "PROFILE_PATH",
        "MANIFEST_PATH",
        "DATABASE_NAME",
        "CLEANED_TABLE",
        "FAILED_TABLE",
        "REVIEW_TABLE",
        "SQL_BASE_URI",
        "START_DATE",
        "END_DATE",
        "TARGET_DOCUMENT_TYPE",
        "MIN_CATEGORY_FREQUENCY",
        "IQR_MULTIPLIER",
    ]
    args = getResolvedOptions(sys.argv, arg_names)

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    s3 = boto3.client("s3")
    started_at = datetime.now(timezone.utc)
    run_id = started_at.strftime("%Y%m%dT%H%M%SZ")

    # Read each staged CSV separately, then union by column name.
    # This preserves schema evolution when one client export omits an optional column.
    source_bucket, source_prefix = parse_s3_uri(args["SOURCE_PATH"])
    objects = s3.list_objects_v2(Bucket=source_bucket, Prefix=source_prefix).get("Contents", [])
    csv_uris = [f"s3://{source_bucket}/{obj['Key']}" for obj in objects if obj["Key"].lower().endswith(".csv")]
    if not csv_uris:
        raise RuntimeError("No staged CSV files found")

    frames = []
    for uri in csv_uris:
        frame = (spark.read.option("header", "true").option("inferSchema", "true").csv(uri)
                 .withColumn("source_file", F.regexp_extract(F.input_file_name(), r"([^/]+)$", 1)))
        frames.append(frame)
    raw_df = frames[0]
    for frame in frames[1:]:
        raw_df = raw_df.unionByName(frame, allowMissingColumns=True)
    raw_df.createOrReplaceTempView("raw_data")

    sql_params = {
        "START_DATE": args["START_DATE"],
        "END_DATE": args["END_DATE"],
        "TARGET_DOCUMENT_TYPE": args["TARGET_DOCUMENT_TYPE"],
        "MIN_CATEGORY_FREQUENCY": args["MIN_CATEGORY_FREQUENCY"],
        "IQR_MULTIPLIER": args["IQR_MULTIPLIER"],
    }

    views = {}
    for filename, view_name in SQL_FILES:
        sql_text = load_sql_from_s3(s3, args["SQL_BASE_URI"], filename)
        sql_text = render_sql(sql_text, sql_params)
        df = spark.sql(sql_text)
        df.createOrReplaceTempView(view_name)
        views[view_name] = df

    # Failed = invalid records + lower-amount duplicates.
    failed_df = views["invalid_failed"].unionByName(
        views["duplicate_failed"],
        allowMissingColumns=True,
    )

    cleaned_df = views["cleaned"]
    review_df = views["review"]

    target = args["TARGET_PATH"].rstrip("/")
    write_catalog_table(
        glue_context,
        cleaned_df,
        f"{target}/cleaned/",
        args["DATABASE_NAME"],
        args["CLEANED_TABLE"],
        ["year", "transaction_month"],
    )
    write_catalog_table(
        glue_context,
        failed_df,
        f"{target}/failed/",
        args["DATABASE_NAME"],
        args["FAILED_TABLE"],
    )
    write_catalog_table(
        glue_context,
        review_df,
        f"{target}/review/",
        args["DATABASE_NAME"],
        args["REVIEW_TABLE"],
        ["year", "transaction_month"],
    )

    # Compact profiling outputs.
    profile_path = args["PROFILE_PATH"].rstrip("/") + f"/run_id={run_id}"
    row_counts = [
        ("raw", raw_df.count()),
        ("cleaned", cleaned_df.count()),
        ("failed", failed_df.count()),
        ("review", review_df.count()),
    ]
    spark.createDataFrame(row_counts, ["dataset", "row_count"]).coalesce(1).write.mode("overwrite").json(
        f"{profile_path}/row_counts"
    )

    for column in ["buyer_name", "document_type", "product_category", "seller_name"]:
        (
            cleaned_df.groupBy(column)
            .count()
            .orderBy(F.desc("count"))
            .coalesce(1)
            .write.mode("overwrite")
            .json(f"{profile_path}/{column}_distribution")
        )

    finished_at = datetime.now(timezone.utc)
    manifest_uri = args["MANIFEST_PATH"].rstrip("/") + f"/run_id={run_id}/run_manifest.json"
    put_json(
        s3,
        manifest_uri,
        {
            "run_id": run_id,
            "job_name": args["JOB_NAME"],
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "source_path": args["SOURCE_PATH"],
            "target_path": args["TARGET_PATH"],
            "row_counts": dict(row_counts),
            "rules": {
                "start_date": args["START_DATE"],
                "end_date": args["END_DATE"],
                "target_document_type": args["TARGET_DOCUMENT_TYPE"],
                "minimum_category_frequency": int(args["MIN_CATEGORY_FREQUENCY"]),
                "iqr_multiplier": float(args["IQR_MULTIPLIER"]),
            },
        },
    )

    job.commit()


if __name__ == "__main__":
    main()
