import boto3
import datetime
import os
import pandas as pd
import logging


def upload_df_to_s3(bucket_name: str, df: pd.DataFrame, key: str) -> None:
    """Upload any file to s3.

    Args:
        bucket_name (str): bucket name
        df (dataFrame) : dataframe
        key (str): file name in s3
    """
    
    username = boto3.client("sts").get_caller_identity()["Arn"].split("/")[-1]
    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket_name, Body = df.to_csv(index=False), Key=key, Tagging=f"owner={username}")

def export_source(
    df: pd.DataFrame,
    source_name: str,
    base_path: str = "app_data/asset_mapping/input_data/",
    bucket_name: str = "vuong",
):
    df.to_csv("tmp.csv", index=False)
    path = os.path.join(
        base_path, source_name, f"{source_name}_{str(datetime.date.today())}.csv"
    )
    try:
        upload_df_to_s3(bucket_name=bucket_name, df=df, key=path)
    except Exception as e:
        logging.error(e)
    finally:
        os.remove("tmp.csv")
