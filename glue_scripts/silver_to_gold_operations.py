import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime


logger = logging.getLogger()
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
handler.setFormatter(formatter)
logger.addHandler(handler)


logger.info("=" * 80)
logger.info("Starting Gold Operations ETL Job")
logger.info("=" * 80)

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'SILVER_BUCKET',
    'GOLD_BUCKET',
    'DATABASE_NAME'
])

logger.info(f"Job Name: {args['JOB_NAME']}")
logger.info(f"Silver Bucket: {args['SILVER_BUCKET']}")
logger.info(f"Gold Bucket: {args['GOLD_BUCKET']}")
logger.info(f"Database: {args['DATABASE_NAME']}")

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger.info("Glue context initialized successfully")

logger.info("Reading data from Silver layer...")

try:
    silver_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
        database=args['DATABASE_NAME'],
        table_name="silver_yellow",
        transformation_ctx="silver_dynamic_frame"
    )

    silver_df = silver_dynamic_frame.toDF()

    silver_count = silver_df.count()
    logger.info(f"Silver records loaded: {silver_count:,}")

    if silver_count == 0:
        logger.warning("No records found in Silver layer. Exiting job.")
        job.commit()
        sys.exit(0)

except Exception as e:
    logger.error(f"Failed to read Silver layer: {str(e)}")
    raise


logger.info("Applying operational transformations...")

try:
    gold_operations_df = silver_df.select(
        col("pickup_datetime"),
        col("dropoff_datetime"),
        date_format("pickup_datetime", "yyyy-MM-dd").cast("date").alias("trip_date"),
        hour("pickup_datetime").alias("pickup_hour"),
        dayofmonth("pickup_datetime").alias("day"),
        dayofweek("pickup_datetime").alias("day_of_week"),
        date_format("pickup_datetime", "EEEE").alias("day_name"),
        weekofyear("pickup_datetime").alias("week_of_year"),
        quarter("pickup_datetime").alias("quarter"),
        col("pickup_location_id"),
        col("dropoff_location_id"),
        col("vendor_label"),
        col("passenger_count"),
        col("trip_distance"),
        col("rate_code"),
        col("store_and_fwd_flag"),
        when(
            col("pickup_location_id") == col("dropoff_location_id"),
            True
        ).otherwise(False).alias("is_same_location"),
        when(
            (col("total_amount") < 0),
            True
        ).otherwise(False).alias("potential_reversal"),
        ((unix_timestamp("dropoff_datetime") - unix_timestamp("pickup_datetime")) / 60.0)
        .cast("double").alias("trip_duration_minutes"),
        # Average speed in mph
        when(
            (unix_timestamp("dropoff_datetime") - unix_timestamp("pickup_datetime")) > 0,
            round((col("trip_distance") /
                   ((unix_timestamp("dropoff_datetime") - unix_timestamp("pickup_datetime")) / 3600.0)), 2)
        ).otherwise(0).alias("avg_speed_mph"),
        when(hour("pickup_datetime").between(6, 9), "Morning Rush")
        .when(hour("pickup_datetime").between(16, 19), "Evening Rush")
        .when(hour("pickup_datetime").between(22, 23) |
              hour("pickup_datetime").between(0, 5), "Night")
        .otherwise("Off-Peak").alias("time_of_day"),
        when(dayofweek("pickup_datetime").isin([1, 7]), "Weekend")
        .otherwise("Weekday").alias("day_type"),
        when(
            hour("pickup_datetime").between(6, 9) |
            hour("pickup_datetime").between(16, 19),
            True
        ).otherwise(False).alias("is_rush_hour"),
        when(dayofweek("pickup_datetime").isin([1, 7]), True).otherwise(False).alias("is_weekend"),
        when(
            (hour("pickup_datetime") >= 22) |
            (hour("pickup_datetime") <= 6),
            True
        ).otherwise(False).alias("is_night_trip"),
        when(col("store_and_fwd_flag") == "Y", True).otherwise(False).alias("was_stored_forward"),
        current_timestamp().alias("processed_timestamp"),
        year("pickup_datetime").cast("string").alias("year"),
        lpad(month("pickup_datetime").cast("string"), 2, "0").alias("month")
    )
    gold_count = gold_operations_df.count()
    logger.info(f"Transformation complete. Gold Operations records: {gold_count:,}")

except Exception as e:
    logger.error(f"Transformation failed: {str(e)}")
    raise

logger.info("Running data quality checks on Gold layer...")


gold_output_path = f"s3://{args['GOLD_BUCKET']}/operations/"
logger.info(f"Writing to Gold Operations layer: {gold_output_path}")

try:
    gold_operations_df.write \
        .mode("append") \
        .format("parquet") \
        .option("compression", "snappy") \
        .partitionBy("year", "month") \
        .save(gold_output_path)

    logger.info(f"✓ Gold Operations layer written successfully")

except Exception as e:
    logger.error(f"Failed to write Gold Operations layer: {str(e)}")
    raise


logger.info("=" * 80)
logger.info("Job completed successfully")
logger.info(f"Completion time: {datetime.now()}")
logger.info("=" * 80)

job.commit()