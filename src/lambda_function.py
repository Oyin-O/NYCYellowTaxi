import os
import boto3
import logging
from ingest_nyc_taxi_to_bronze import TaxiDownloader

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """
        Lambda handler that:
        1. Downloads NYC taxi data to Bronze S3
        2. Triggers Bronze crawler upon success
        """

    downloader = TaxiDownloader(
        bucket_name=os.getenv("BUCKET"),
        base_url_template=os.getenv("BASE_URL"),
        save_root="/tmp",
        max_retries=3,
        retry_delay=5
    )

    try:
        logger.info("Starting Bronze ingestion ...")
        result = downloader.run_bronze_landing_job()

        logger.info(f"Ingestion result: {result}")
        return {
            'statusCode': 200,
            'body': result
        }

    except Exception as e:
        logger.error(f"[FAILED] Lambda execution failed: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': {
                'error': str(e),
                'crawler_triggered': False
            }
        }
