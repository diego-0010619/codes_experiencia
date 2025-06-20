import sys
from datetime import datetime
from pyspark.sql import DataFrame
from product_logs_consultas_agentes.extra.utils.utils import create_custom_logger

import sys
from pyspark.context import SparkContext

logger = create_custom_logger(__name__)

def read_data_s3(glue_context, execution_timestamp: str, bucket_source: str) -> DataFrame:
    """
    Returns a DataFrame by reading JSON files from S3 in the specified partition.

    Args:
        glue_context: GlueContext object for AWS Glue
        execution_timestamp (str or datetime): Execution timestamp in 'YYYY-MM-DD HH:MM:SS' format or datetime
        bucket_source (str): S3 bucket prefix, e.g., 's3://my-bucket/path/'

    Returns:
        DataFrame: Spark DataFrame loaded from the partitioned path
    """
    if isinstance(execution_timestamp, str):
        execution_timestamp = datetime.strptime(execution_timestamp, "%Y-%m-%d %H:%M:%S")

    year = execution_timestamp.strftime('%Y')
    month = execution_timestamp.strftime('%m')
    day = execution_timestamp.strftime('%d')
    hour = execution_timestamp.strftime('%H')

    s3_path = f"{bucket_source}year={year}/month={month}/day={day}/hour={hour}/"

    dyf = glue_context.create_dynamic_frame_from_options(
        connection_type="s3",
        connection_options={"paths": [s3_path], "recurse": True},
        format="json",
        format_options={"multiLine": "true"}
    )
    df = dyf.toDF()
    return df
