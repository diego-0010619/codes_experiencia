import pytest
from datetime import datetime
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession, DataFrame
from src.product_logs_consultas_agentes.extra.config.sparkconfig import spark_config
from src.product_logs_consultas_agentes.extra.utils.extract import (read_data_s3)

spark_config: SparkConf = spark_config
spark_session: SparkSession = (
    SparkSession.builder.appName("test_glue_job").enableHiveSupport().config(conf=spark_config).getOrCreate()
)

execution_timestamp = datetime.strptime("2025-04-28 15:00:00", "%Y-%m-%d %H:%M:%S")

columns = ["col1", "col2"]
data = [("data1", 10), ("data2", 20)]
df_expected: DataFrame = spark_session.createDataFrame(data=data, schema=columns)

class GlueContextMock:
    def __init__(self, df_mock):
        self.df_mock = df_mock

    def create_dynamic_frame_from_options(self, connection_type, connection_options, format, format_options):
        class DynamicFrameMock:
            def toDF(inner_self):
                return self.df_mock
        return DynamicFrameMock()

# Variables de prueba
bucket_source = "s3://bucket-source/path/"
execution_timestamp = "2025-04-28 15:00:00"

glue_context = GlueContextMock(df_expected)

def test_read_data_s3():
    df_final = read_data_s3(
        glue_context,
        execution_timestamp,
        bucket_source,
    )

    assert df_final.schema == df_expected.schema
    assert df_final.collect() == df_expected.collect()
