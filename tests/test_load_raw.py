import gzip
import unittest
from unittest.mock import MagicMock, patch

import requests

from airflow.exceptions import AirflowException
from ingestion.load_raw import load_to_raw


class LoadRawTest(unittest.TestCase):
    def setUp(self):
        self.hook_patch = patch("ingestion.load_raw.S3Hook")
        self.hook = self.hook_patch.start().return_value
        self.addCleanup(self.hook_patch.stop)
        self.http_patch = patch("ingestion.load_raw.requests.get")
        self.http = self.http_patch.start()
        self.addCleanup(self.http_patch.stop)
        self.hook.check_for_bucket.return_value = True
        self.hook.check_for_key.return_value = False
        self.kwargs = {
            "source_url": "https://example.com/data.csv",
            "source_filename": "data.csv",
            "dataset_slug": "nyc311",
            "s3_conn_id": "s3_conn",
            "ds": "2025-01-01",
        }

    def test_existing_object_skips_http_and_upload(self):
        self.hook.check_for_key.return_value = True
        path = load_to_raw(**self.kwargs)
        self.assertEqual(path, "s3://raw/nyc311/ingested_on=2025-01-01/data.csv")
        self.http.assert_not_called()
        self.hook.load_bytes.assert_not_called()

    def test_missing_bucket_fails_without_creating_it(self):
        self.hook.check_for_bucket.return_value = False
        with self.assertRaisesRegex(AirflowException, "docker compose up -d"):
            load_to_raw(**self.kwargs)
        self.hook.create_bucket.assert_not_called()
        self.http.assert_not_called()
        self.hook.load_bytes.assert_not_called()

    def test_compressed_response_preserves_bytes_and_logical_date(self):
        payload = gzip.compress(b"key,value\r\n1,2\r\n")
        response = MagicMock()
        response.raw.read.return_value = payload
        self.http.return_value.__enter__.return_value = response
        load_to_raw(**self.kwargs)
        response.raw.read.assert_called_once_with(decode_content=False)
        self.hook.load_bytes.assert_called_once_with(
            bytes_data=payload,
            key="nyc311/ingested_on=2025-01-01/data.csv",
            bucket_name="raw",
            replace=False,
        )

    def test_http_error_does_not_upload(self):
        response = self.http.return_value.__enter__.return_value
        response.raise_for_status.side_effect = requests.HTTPError("503")
        with self.assertRaises(requests.HTTPError):
            load_to_raw(**self.kwargs)
        self.hook.load_bytes.assert_not_called()

    def test_interrupted_download_does_not_upload(self):
        response = self.http.return_value.__enter__.return_value
        response.raw.read.side_effect = requests.ConnectionError("connection closed")
        with self.assertRaises(requests.ConnectionError):
            load_to_raw(**self.kwargs)
        self.hook.load_bytes.assert_not_called()


if __name__ == "__main__":
    unittest.main()
