# -*- coding: utf-8 -*-
# data_ingestion/bigquery.py
import os
from typing import List, Optional, Sequence

import pandas as pd
from google.cloud import bigquery


def get_bq_client(project_id: Optional[str] = None) -> bigquery.Client:
    """
    建立 BigQuery Client。會使用 GOOGLE_APPLICATION_CREDENTIALS 指向的服務金鑰。
    """
    project = project_id or os.getenv("GCP_PROJECT_ID")
    if not project:
        raise RuntimeError("GCP_PROJECT_ID 未設定")
    return bigquery.Client(project=project)


def ensure_dataset(client: bigquery.Client, dataset_id: str, location: str = "asia-east1") -> bigquery.Dataset:
    """
    若 dataset 不存在就建立；存在則直接回傳。
    """
    ds_ref = bigquery.Dataset(client.dataset(dataset_id))
    ds_ref.location = location
    try:
        return client.get_dataset(ds_ref)
    except Exception:
        return client.create_dataset(ds_ref, exists_ok=True)


def load_dataframe(
    client: bigquery.Client,
    df: pd.DataFrame,
    table: str,
    dataset_id: str,
    write_mode: str = "replace",
    partition_field: Optional[str] = None,
    clustering_fields: Optional[Sequence[str]] = None,
) -> None:
    """
    將 DataFrame 上傳到 BigQuery。
    write_mode: "replace" | "append"（預設 replace）
    """
    table_id = f"{client.project}.{dataset_id}.{table}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=(
            bigquery.WriteDisposition.WRITE_TRUNCATE
            if write_mode == "replace" else bigquery.WriteDisposition.WRITE_APPEND
        )
    )
    if partition_field:
        job_config.time_partitioning = bigquery.TimePartitioning(field=partition_field)
    if clustering_fields:
        job_config.clustering_fields = list(clustering_fields)

    load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    load_job.result()  # wait
    # 建好後再抓一次 schema（確保表存在）
    client.get_table(table_id)


def create_or_replace_view(client: bigquery.Client, dataset_id: str, view_name: str, sql: str) -> None:
    table_id = f"{client.project}.{dataset_id}.{view_name}"
    view = bigquery.Table(table_id)
    view.view_query = sql
    client.delete_table(table_id, not_found_ok=True)
    client.create_table(view)


def create_or_replace_table_as_select(client: bigquery.Client, dataset_id: str, table_name: str, sql_select: str) -> None:
    """
    用 CTAS 方式建立/覆蓋實體表：CREATE OR REPLACE TABLE ... AS SELECT ...
    """
    table_id = f"`{client.project}.{dataset_id}.{table_name}`"
    q = f"CREATE OR REPLACE TABLE {table_id} AS {sql_select}"
    client.query(q).result()
