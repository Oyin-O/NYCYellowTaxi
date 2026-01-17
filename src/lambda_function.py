import os
from ingest_nyc_taxi_to_bronze import TaxiDownloader


def lambda_handler(event, context):

    downloader = TaxiDownloader(
        bucket_name=os.getenv("BUCKET"),
        base_url_template=os.getenv("BASE_URL"),
        save_root="/tmp",
        max_retries=3,
        retry_delay=5
    )

    result = downloader.run_bronze_landing_job()

    return result