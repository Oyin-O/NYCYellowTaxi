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


# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create console handler with formatting
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
handler.setFormatter(formatter)
logger.addHandler(handler)


logger.info("=" * 80)
logger.info("Starting Gold Finance ETL Job")
logger.info("=" * 80)

# Get job parameters
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

# Initialize Glue context
logger.info("Initializing Glue context and Spark session...")
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger.info("Glue context initialized successfully")

logger.info("Reading data from Silver layer...")
logger.info(f"Source: {args['DATABASE_NAME']}.silver_yellow")

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

# ==================== TRANSFORMATIONS ====================

logger.info("Applying business transformations...")

try:
    gold_finance_df = silver_df.select(
        col("pickup_datetime"),
        col("dropoff_datetime"),
        date_format("pickup_datetime", "yyyy-MM-dd").cast("date").alias("trip_date"),
        hour("pickup_datetime").alias("pickup_hour"),
        dayofmonth("pickup_datetime").alias("day"),
        dayofweek("pickup_datetime").alias("day_of_week"),
        date_format("pickup_datetime", "EEEE").alias("day_name"),
        weekofyear("pickup_datetime").alias("week_of_year"),
        quarter("pickup_datetime").alias("quarter"),
        col("payment_type"),
        col("fare_amount").cast("decimal(10,2)"),
        col("extra").cast("decimal(10,2)"),
        col("mta_tax").cast("decimal(10,2)"),
        col("tip_amount").cast("decimal(10,2)"),
        col("tolls_amount").cast("decimal(10,2)"),
        col("improvement_surcharge").cast("decimal(10,2)"),
        col("total_amount").cast("decimal(10,2)"),
        coalesce(col("congestion_surcharge"), lit(0.0)).cast("decimal(10,2)").alias("congestion_surcharge"),
        coalesce(col("airport_fee"), lit(0.0)).cast("decimal(10,2)").alias("airport_fee"),
        (col("total_amount") - coalesce(col("tip_amount"), lit(0))).cast("decimal(10,2)").alias("fare_without_tip"),
        when(col("total_amount") > 0,
             round((col("tip_amount") / col("total_amount")) * 100, 2))
        .otherwise(0).alias("tip_percentage"),
        (coalesce(col("extra"), lit(0)) +
         coalesce(col("mta_tax"), lit(0)) +
         coalesce(col("improvement_surcharge"), lit(0)) +
         coalesce(col("congestion_surcharge"), lit(0)) +
         coalesce(col("airport_fee"), lit(0))
         ).cast("decimal(10,2)").alias("total_surcharges"),
        when(hour("pickup_datetime").between(6, 9), "Morning Rush")
        .when(hour("pickup_datetime").between(16, 19), "Evening Rush")
        .when(hour("pickup_datetime").between(22, 23) |
              hour("pickup_datetime").between(0, 5), "Night")
        .otherwise("Off-Peak").alias("time_of_day"),
        when(dayofweek("pickup_datetime").isin([1, 7]), "Weekend")
        .otherwise("Weekday").alias("day_type"),
        current_timestamp().alias("processed_timestamp"),
        year("pickup_datetime").cast("string").alias("year"),
        lpad(month("pickup_datetime").cast("string"), 2, "0").alias("month")
    )

    gold_count = gold_finance_df.count()
    logger.info(f"Transformation complete. Gold Finance records: {gold_count:,}")

except Exception as e:
    logger.error(f"Transformation failed: {str(e)}")
    raise


logger.info("Running data quality checks on Gold layer...")

try:
    # Check for nulls in critical fields
    null_checks = gold_finance_df.select(
        sum(when(col("total_amount").isNull(), 1).otherwise(0)).alias("null_total_amount"),
        sum(when(col("fare_amount").isNull(), 1).otherwise(0)).alias("null_fare_amount"),
        sum(when(col("payment_type").isNull(), 1).otherwise(0)).alias("null_payment_type")
    ).collect()[0]

    if null_checks.null_total_amount > 0:
        logger.error(f"Data quality check failed: {null_checks.null_total_amount} null values in total_amount")
        raise Exception("Critical field validation failed: null values in total_amount")

    if null_checks.null_fare_amount > 0:
        logger.error(f"Data quality check failed: {null_checks.null_fare_amount} null values in fare_amount")
        raise Exception("Critical field validation failed: null values in fare_amount")

    logger.info("Data quality checks passed - no nulls in critical fields")

except Exception as e:
    logger.error(f"Data quality check failed: {str(e)}")
    raise

# ==================== WRITE TO GOLD LAYER ====================

gold_output_path = f"s3://{args['GOLD_BUCKET']}/finance/"
logger.info(f"Writing to Gold Finance layer: {gold_output_path}")

try:
    # Write as Parquet with Snappy compression, partitioned by year and month
    gold_finance_df.write \
        .mode("append") \
        .format("parquet") \
        .option("compression", "snappy") \
        .partitionBy("year", "month") \
        .save(gold_output_path)

    logger.info(f"Gold Finance layer written successfully to {gold_output_path}")

except Exception as e:
    logger.error(f"Failed to write Gold Finance layer: {str(e)}")
    raise

# ==================== STATISTICS ====================

logger.info("=" * 80)
logger.info("JOB STATISTICS")
logger.info("=" * 80)

try:
    # Get partition statistics
    partition_stats = gold_finance_df.groupBy("year", "month") \
        .count() \
        .orderBy("year", "month") \
        .collect()

    logger.info("Records by partition:")
    for row in partition_stats:
        logger.info(f"  {row.year}-{row.month}: {row['count']:,} records")

    # Revenue statistics
    revenue_stats = gold_finance_df.select(
        round(sum("total_amount"), 2).alias("total_revenue"),
        round(avg("total_amount"), 2).alias("avg_fare"),
        round(sum("tip_amount"), 2).alias("total_tips"),
        round(avg("tip_percentage"), 2).alias("avg_tip_percentage")
    ).collect()[0]

    logger.info("")
    logger.info("Revenue Metrics:")
    logger.info(f"  Total Revenue: ${revenue_stats.total_revenue:,.2f}")
    logger.info(f"  Average Fare: ${revenue_stats.avg_fare:,.2f}")
    logger.info(f"  Total Tips: ${revenue_stats.total_tips:,.2f}")
    logger.info(f"  Average Tip %: {revenue_stats.avg_tip_percentage}%")

    # Payment type breakdown
    payment_breakdown = gold_finance_df.groupBy("payment_type_name") \
        .agg(count("*").alias("trip_count")) \
        .orderBy(desc("trip_count")) \
        .collect()

    logger.info("")
    logger.info("Payment Type Distribution:")
    for row in payment_breakdown:
        logger.info(f"  {row.payment_type_name}: {row.trip_count:,} trips")

except Exception as e:
    logger.warning(f"Failed to generate statistics: {str(e)}")

# ==================== JOB COMPLETION ====================

logger.info("=" * 80)
logger.info("Job completed successfully")
logger.info(f"Completion time: {datetime.now()}")
logger.info("=" * 80)

job.commit()