# utils/bq_client.py
from __future__ import annotations

import os
from typing import Optional

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account


def get_bq_client(
    key_path: Optional[str] = None,
    project_id: Optional[str] = None,
) -> bigquery.Client:
    """
    Tạo và trả về một BigQuery Client dùng service account.

    Parameters
    ----------
    key_path : str, optional
        Đường dẫn tới file JSON của service account.
        - Nếu None: mặc định lấy file service_account.json trong thư mục utils.
    project_id : str, optional
        Gán project_id cho client.
        - Nếu None: dùng project_id trong file service account.

    Returns
    -------
    bigquery.Client
    """
    if key_path is None:
        # Đường dẫn tới service_account.json ngay trong thư mục utils
        key_path = os.path.join(os.path.dirname(__file__), "service_account.json")

    credentials = service_account.Credentials.from_service_account_file(key_path)
    client = bigquery.Client(
        credentials=credentials,
        project=project_id or credentials.project_id,
    )
    return client


def query_to_dataframe(
    sql: str,
    client: Optional[bigquery.Client] = None,
) -> pd.DataFrame:
    """
    Chạy câu lệnh SQL và trả về DataFrame.
    Dùng REST API thuần (create_bqstorage_client=False) để tránh lỗi Arrow/Storage.
    """
    if client is None:
        client = get_bq_client()

    job = client.query(sql)
    df = job.to_dataframe(create_bqstorage_client=False)
    return df


def load_sql_file(filename: str) -> str:
    """
    Đọc file .sql trong thư mục /sql và trả về nội dung query.
    """
    base_dir = os.path.dirname(os.path.dirname(__file__))   # thư mục BIGQUERY
    sql_path = os.path.join(base_dir, "sql", filename)

    with open(sql_path, "r", encoding="utf-8") as f:
        return f.read()
