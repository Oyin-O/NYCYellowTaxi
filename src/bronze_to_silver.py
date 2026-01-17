import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F

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
        table_name="nyc_yellow_bronze"
    ).toDF()

    null_check_subset = ["vendor_id", "tpep_pickup_datetime", "trip_distance", "tpep_dropoff_datetime"]

    null_counts = bronze_df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in null_check_subset])
    null_counts.show()

    cleaned_df = bronze_df.dropna(subset=null_check_subset)

    cleaning_rules = """
        trip_distance > 0 AND trip_distance < 100 
        AND passenger_count >= 1 AND passenger_count <= 6
        AND fare_amount >= 2.50
        AND total_amount > 0
        AND tpep_dropoff_datetime > tpep_pickup_datetime
        AND tip_amount >= 0
    """

    cleaned_df = cleaned_df.filter(cleaning_rules)

    cleaned_df = cleaned_df \
        .withColumn("year", F.year(F.col("tpep_pickup_datetime"))) \
        .withColumn("month", F.month(F.col("tpep_pickup_datetime")))

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
