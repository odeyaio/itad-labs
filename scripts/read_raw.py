import argparse
import os
from pathlib import PurePosixPath

import duckdb


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", required=True)
    args = parser.parse_args()
    extension = PurePosixPath(args.path).suffix.lower()
    readers = {
        ".csv": "read_csv_auto",
        ".json": "read_json_auto",
        ".jsonl": "read_json_auto",
        ".ndjson": "read_json_auto",
        ".parquet": "read_parquet",
    }
    if extension not in readers:
        parser.error(f"Неподдерживаемый формат: {extension}")

    with duckdb.connect(":memory:") as connection:
        connection.execute("INSTALL httpfs FROM 'https://extensions.duckdb.org'")
        connection.execute("LOAD httpfs")
        settings = {
            "s3_endpoint": os.environ["S3_ENDPOINT"],
            "s3_access_key_id": os.environ["S3_ACCESS_KEY"],
            "s3_secret_access_key": os.environ["S3_SECRET_KEY"],
            "s3_region": "us-east-1",
            "s3_url_style": "path",
            "s3_use_ssl": False,
        }
        for name, value in settings.items():
            connection.execute(f"SET {name} = ?", [value])

        print(f"Чтение {args.path}")
        reader = readers[extension]
        connection.sql(f"SELECT * FROM {reader}(?) LIMIT 5", params=[args.path]).show()
        count = connection.execute(
            f"SELECT count(*) FROM {reader}(?)", [args.path]
        ).fetchone()[0]
        print(f"Число строк: {count}")


if __name__ == "__main__":
    main()
