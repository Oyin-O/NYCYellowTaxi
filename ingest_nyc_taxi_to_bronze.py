import os
import hashlib
import time
import boto3
import requests
from datetime import datetime, date, timedelta
from pathlib import Path
import logging
from typing import Optional, Dict
from abc import ABC, abstractmethod

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class TaxiDownloader:
    def __init__(self, bucket_name: str, base_url_template: str, save_root: str = '/tmp',
                 max_retries: int = 3, retry_delay: int = 5):
        self.save_root = save_root
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.s3_client = boto3.client('s3')
        self.bucket = bucket_name
        self.base_url_template = base_url_template
        os.makedirs(self.save_root, exist_ok=True)

    @staticmethod
    def get_previous_year_month() -> str:
        """
        Get the previous month-year that would be passed into the default url for data download
        :return: previous month-year e.g '2025-01'
        """
        prev_year_month = (date.today().replace(day=1) - timedelta(days=32)).strftime("%Y-%m")
        return prev_year_month

    @staticmethod
    def md5_compute(file_path: str) -> str:
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    @staticmethod
    def get_file_size_mb(file_path: str) -> float:
        """
        :param file_path:
        :return: file size in mb
        """
        return os.path.getsize(file_path) / (1024 * 1024)

    def download_file(self, url: str, local_path: str):
        attempt = 0

        while attempt < self.max_retries:
            try:
                attempt += 1
                logger.info(f'Attempt {attempt} Downloading: {url}')
                r = requests.get(url, stream=True, timeout=60)
                r.raise_for_status()

                with open(local_path, "wb") as f:
                    for chunk in r.iter_content(8192):
                        f.write(chunk)

                logger.info(f'[SUCCESS] Downloaded {local_path}')
                return local_path

            except Exception as e:
                logger.warning(f'[RETRY] Download failed')
                time.sleep(self.retry_delay)

        logger.error(f'[FAIL] Unable to download after {self.max_retries} attempts')
        raise Exception('Download Failed')

    def upload_to_bronze(self, local_file: str, year_month: str):
        filename = os.path.basename(local_file)
        s3_key = f'bronze/yellow/year={year_month.split("-")[0]}/month={year_month.split("-")[1]}/{filename}'

        attempt = 0
        while attempt < self.max_retries:
            try:
                attempt += 1
                logger.info(f'[ATTEMPT {attempt}] Uploading to S3: {s3_key}')
                self.s3_client.upload_file(local_file, self.bucket, s3_key)
                logger.info(f'[SUCCESS] Uploaded {filename} to s3://{self.bucket}/{s3_key}')
                return True

            except Exception as e:
                logger.warning(f'[RETRY] Upload failed: {e}')
                time.sleep(self.retry_delay)

    def run_bronze_landing_job(self):
        year_month = self.get_previous_year_month()
        url = self.base_url_template.format(year_month=year_month)
        local_file_name = os.path.basename(url)
        local_path = os.path.join(self.save_root, local_file_name)

        file_path = self.download_file(url, local_path)

        # Metadata
        size_mb = round(self.get_file_size_mb(file_path), 2)
        md5_checksum = self.md5_compute(file_path)
        logger.info(f"[METADATA] {year_month} → {size_mb} MB | MD5: {md5_checksum}")

        # Upload to Bronze
        self.upload_to_bronze(file_path, year_month)

        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"[CLEANUP] Deleted {file_path} from local storage")

        return {"year_month": year_month, "size_mb": size_mb, "md5": md5_checksum}



