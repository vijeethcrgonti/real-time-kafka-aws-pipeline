"""
glue_job.py
===========
AWS Glue ETL job for batch processing of Kafka-ingested data.
Reads from S3 raw zone, transforms, validates, writes to curated Delta Lake.

Deploy: aws glue create-job --cli-input-json file://aws/glue_job_config.json
Run   : aws glue start-job-run --job-name kafka-etl-pipeline
"""

import sys
import logging
from datetime import datetime, timedelta

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

from processing.partition_optimizer import PartitionOptimizer
from processing.data_validator import DataValidator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ── Job Parameters ────────────────────────────────────────────────────────────

def get_job_args():
    """Parse Glue job arguments."""
    args = getResolvedOptions(sys.argv, [
        "JOB_NAME",
        "input_path",
        "output_path",
        "database_name",
        "table_name",
        "processing_date",     # YYYY-MM-DD format
        "enable_validation",   # "true" or "false"
    ])
    return args


# ── Main ETL Logic ────────────────────────────────────────────────────────────

def run_etl(
    glue_context: GlueContext,
    args: dict,
) -> dict:
    """
    Main ETL pipeline:
    1. Read raw JSON from S3
    2. Apply transformations
    3. Validate data quality
    4. Write to Delta Lake (partitioned)
    5. Update Glue catalog

    Returns: job stats dictionary
    """
    spark = glue_context.spark_session
    optimizer = PartitionOptimizer(spark)
    stats = {"job_start": datetime.utcnow().isoformat(), "status": "running"}

    processing_date = args.get("processing_date", datetime.utcnow().strftime("%Y-%m-%d"))
    logger.info(f"Processing date: {processing_date}")

    # ── Step 1: Read raw data ─────────────────────────────────────────────────
    logger.info(f"Reading from: {args['input_path']}")
    raw_df = (
        spark.read
        .option("basePath", args["input_path"])
        .json(f"{args['input_path']}/processing_date={processing_date}/")
    )
    stats["raw_count"] = raw_df.count()
    logger.info(f"Raw records: {stats['raw_count']:,}")

    # ── Step 2: Transform ─────────────────────────────────────────────────────
    logger.info("Applying transformations...")
    transformed_df = (
        raw_df
        # Standardize timestamp format
        .withColumn("event_ts", F.to_timestamp(F.col("timestamp")))
        # Extract payload fields
        .withColumn("member_id", F.col("payload.member_id"))
        .withColumn("record_value", F.col("payload.value").cast(DoubleType()))
        .withColumn("status", F.col("payload.status"))
        .withColumn("region", F.col("payload.region"))
        .withColumn("facility_id", F.col("payload.facility_id"))
        # Add ETL metadata
        .withColumn("etl_date", F.lit(processing_date))
        .withColumn("etl_timestamp", F.current_timestamp())
        .withColumn("etl_version", F.lit("2.1.0"))
        # Standardize nulls
        .fillna({"status": "unknown", "region": "US-UNKNOWN"})
        # Drop raw payload column (already extracted)
        .drop("payload", "timestamp")
        # Rename for clarity
        .withColumnRenamed("event_ts", "event_timestamp")
    )

    # ── Step 3: Optimize partitions ───────────────────────────────────────────
    logger.info("Applying partition optimization...")
    optimized_df = optimizer.optimize(
        transformed_df,
        partition_cols=["etl_date", "source_system", "region"],
        filter_conditions=[
            F.col("event_id").isNotNull(),
            F.col("source_system").isNotNull(),
        ],
        cache=True,
        coalesce_output=True,
    )

    # ── Step 4: Data validation ───────────────────────────────────────────────
    if args.get("enable_validation", "true").lower() == "true":
        logger.info("Running data validation...")
        validator = DataValidator(spark)
        validation_result = validator.validate(
            optimized_df,
            suite_name="glue_etl_suite",
        )
        stats["validation_passed"] = validation_result["passed"]
        stats["validation_failures"] = validation_result.get("failure_count", 0)
        logger.info(f"Validation: passed={validation_result['passed']}")

        if not validation_result["passed"] and validation_result.get("critical", False):
            raise ValueError(f"Critical validation failure: {validation_result['failures']}")

    # ── Step 5: Write to Delta Lake ───────────────────────────────────────────
    output_path = args["output_path"]
    logger.info(f"Writing to Delta Lake: {output_path}")

    (
        optimized_df
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("replaceWhere", f"etl_date = '{processing_date}'")
        .partitionBy("etl_date", "source_system", "region")
        .save(output_path)
    )

    stats["output_count"] = optimized_df.count()
    stats["status"] = "completed"
    stats["job_end"] = datetime.utcnow().isoformat()
    logger.info(f"ETL complete | written={stats['output_count']:,}")

    return stats


# ── Glue Job Entrypoint ───────────────────────────────────────────────────────

def main():
    args = get_job_args()

    # Initialize Glue context
    sc = SparkContext()
    glue_context = GlueContext(sc)
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    try:
        stats = run_etl(glue_context, args)
        logger.info(f"Job stats: {stats}")
        job.commit()
        logger.info("Glue job committed successfully")

    except Exception as e:
        logger.error(f"Glue job failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
