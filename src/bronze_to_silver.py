import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import last_day, date_add, to_date, concat, lit

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("bronze_to_silver_ETL")

args = getResolvedOptions(sys.argv, ["BRONZE_BUCKET", "SILVER_BUCKET", "DATABASE_NAME", "JOB_NAME"])
bronze_bucket = args["BRONZE_BUCKET"]
silver_bucket = args["SILVER_BUCKET"]

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

try:
    bronze_df = glueContext.create_dynamic_frame.from_catalog(
        database=args['DATABASE_NAME'],
        table_name="bronze_yellow"
    ).toDF()

    # Build date range filter
    source_date = to_date(concat(F.col("year"), lit("-"), F.col("month"), lit("-01")))
    range_start = date_add(source_date, -2)
    range_end = date_add(last_day(source_date), 2)

    # Create a filter condition column
    date_filter_condition = (
            (to_date(F.col("tpep_dropoff_datetime")) >= range_start) &
            (to_date(F.col("tpep_dropoff_datetime")) <= range_end)
    )

    # Split into valid and invalid data
    valid_data = bronze_df.filter(date_filter_condition)
    invalid_data = bronze_df.filter(~date_filter_condition)  # ~ means NOT

    # Log counts
    valid_count = valid_data.count()
    invalid_count = invalid_data.count()
    logger.info(f"Valid records: {valid_count}, Invalid records: {invalid_count}")

    # Save invalid data to quarantine location
    if invalid_count > 0:
        invalid_dyf = DynamicFrame.fromDF(invalid_data, glueContext, "invalid_dyf")
        glueContext.write_dynamic_frame.from_options(
            frame=invalid_dyf,
            connection_type="s3",
            connection_options={
                "path": f"s3://{args['SILVER_BUCKET']}/quarantine/",
                "partitionKeys": ["year", "month"]
            },
            format="parquet"
        )
        logger.info(f"Saved {invalid_count} invalid records to quarantine")


    null_check_subset = ["VendorID", "tpep_pickup_datetime", "trip_distance", "tpep_dropoff_datetime",
                         "total_amount","PULocationID", "DOLocationID"]

    null_counts = bronze_df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in null_check_subset])
    null_counts.show()


    cleaned_df = valid_data


    cleaned_df = cleaned_df.dropna(subset=null_check_subset)

    cleaning_rules = """
        trip_distance > 0
        AND tpep_dropoff_datetime > tpep_pickup_datetime
    """

    cleaned_df = cleaned_df.filter(cleaning_rules)



    # Define the mappings based on NYC TLC official documentation
    cleaned_df = cleaned_df.withColumn("payment_type_label",
                                       F.when(F.col("payment_type") == 0, "Flex Fare trip")
                                       .when(F.col("payment_type") == 1, "Credit Card")
                                       .when(F.col("payment_type") == 2, "Cash")
                                       .when(F.col("payment_type") == 3, "No Charge")
                                       .when(F.col("payment_type") == 4, "Dispute")
                                       .when(F.col("payment_type") == 6, "Voided trip")
                                       .otherwise("Unknown")
                                       )

    cleaned_df = cleaned_df.withColumn("rate_code_label",
                                       F.when(F.col("RatecodeID") == 1, "Standard rate")
                                       .when(F.col("RatecodeID") == 2, "JFK")
                                       .when(F.col("RatecodeID") == 3, "Newark")
                                       .when(F.col("RatecodeID") == 4, "Nassau or Westchester")
                                       .when(F.col("RatecodeID") == 5, "Negotiated fare")
                                       .when(F.col("RatecodeID") == 6, "Group ride")
                                       .otherwise("Null / unknown")
                                       )

    cleaned_df = cleaned_df.withColumn("VendorID_label",
                                       F.when(F.col("VendorID") == 1, "Creative Mobile Technologies, LLC")
                                       .when(F.col("VendorID") == 2, "Curb Mobility, LLC")
                                       .when(F.col("VendorID") == 6, "Myle Technologies Inc")
                                       .when(F.col("VendorID") == 7, "Helix")
                                       .otherwise("Unknown")
                                       )



    cleaned_df = cleaned_df.drop("payment_type", "RatecodeID", "VendorID")

    logger.info(f"Cleaned {cleaned_df.count()} records")

    silver_DF = DynamicFrame.fromDF(cleaned_df, glueContext, "silver_dyf")

    glueContext.write_dynamic_frame.from_options(
        frame=silver_DF,
        connection_type="s3",
        connection_options={
            "path": f"s3://{args['SILVER_BUCKET']}/yellow/",
            "partitionKeys": ["year", "month"]
        },
        format="parquet"
    )


    job.commit()
    logger.info("Job completed successfully")

except Exception as e:
    logger.error(f"Job failed: {str(e)}")
    raise
