"""
Owner: Gestión de Fraude
Description: Este Glue Job realiza el curado de los logs
            generados por las consultas de los agendes de Zendesk
            a usuarios Nequi por medio de la vista 360.
"""

import sys

import logging
from datetime import datetime
from typing import Dict, Any, overload, Literal

# Glue modules
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

# spark modules
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType, StructField

# Custom modules
from product_logs_consultas_agentes.extra.sources import table_sources
from product_logs_consultas_agentes.extra.utils.load import load_data
from product_logs_consultas_agentes.extra.utils.utils import create_custom_logger
from product_logs_consultas_agentes.extra.utils.extract import read_data_s3
from product_logs_consultas_agentes.extra.config.hudiconfig import get_hudi_config
from product_logs_consultas_agentes.extra.config.sparkconfig import spark_config
from product_logs_consultas_agentes.extra.constants import (
    CAPACITY,
    DATA_PRODUCT,
    DOMAIN,
    COUNTRY,
    SUBDOMAIN,
    PRODUCT_SOURCE,
    PRODUCT
)

from product_logs_consultas_agentes.extra.utils.transform import (
    transform_data,
    add_motivo_consulta,
    add_usuario_zendesk,
    add_fecha_hora_unix,
    add_search_at,
    add_fecha,
    add_hora,
    add_validacion_cuenta,
    add_primary_key,
    add_execution_timestamp
)

# Config Logger
logger = create_custom_logger(__name__)

# Spark config
spark_config: SparkConf = spark_config

# Input Job Parameters
args = getResolvedOptions(sys.argv, ["JOB_NAME", "account_id", "ENV", "execution_timestamp"])
ENV = args.get("ENV", "")
sources: dict = table_sources(ENV)
ACCOUNT = args.get("account_id", "")
execution_timestamp = args.get("execution_timestamp", "")
execution_timestamp = datetime.strptime(execution_timestamp, '%Y-%m-%d %H:%M:%S')
bucket_source = f"s3://{COUNTRY}-{CAPACITY}-{DOMAIN}-raw-{ACCOUNT}-{ENV}/{SUBDOMAIN}/{PRODUCT_SOURCE}/{PRODUCT}/".lower()

# -------------------------------------------------------------------
# -------------------- Métodos de Transformación --------------------
# -------------------------------------------------------------------

def transform_product_logs_consultas_agentes(df: DataFrame, execution_timestamp: str) -> DataFrame:

    # 1. tabulación de la información de los logs.
    df = transform_data(df=df)

    # 2. Rename columns
    df = add_motivo_consulta(df=df)

    # 3. Trim apply for columns of DataFrame
    df = add_usuario_zendesk(df=df)

    # 4. Specific cast columns
    df = add_fecha_hora_unix(df=df)

    # 5. Agregando columna combinada para el guardado del DF
    df = add_search_at(df=df)

    # 6. Validar que el campo numero_cuenta no sea nulo, sea un número y empiece por 870
    df = add_fecha(df=df)

    # 7. Convirtiendo columna combinada en formato Unix
    df = add_hora(df=df)

    # 8. Add field date_execution
    df = add_validacion_cuenta(df=df)

    # 9. Delete columns unique_id, text and channel
    df = add_primary_key(df=df)

    # 10. Add field execution_timestamp
    add_execution_timestamp

    return df

# -------------------------------------------------------------------
# ------------------------ Método Main ------------------------------
# -------------------------------------------------------------------

def main():
    # -------------------------------------------------------------------
    # ------------------------ Inicia Spark -----------------------------
    # -------------------------------------------------------------------
    APP_NAME: str = DATA_PRODUCT

    spark: SparkSession = (
        SparkSession.builder.appName(APP_NAME).enableHiveSupport().config(conf=spark_config).getOrCreate()
    )
    glue_context = GlueContext(spark.sparkContext)
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    logger.info("iniciando Glue job...")

    # ------------------------------------------------------------------------------
    # --------------- Variables inmutables productos de datos ----------------------
    # ------------------------------------------------------------------------------

    # Databases
    database_gestionfraude_curated = f"{COUNTRY}_{CAPACITY}_{DOMAIN}_curated_{ENV}_rl".lower()
    # Buckets
    bucket_gestionfraude_curated = f"{COUNTRY}-{CAPACITY}-{DOMAIN}-curated-{ACCOUNT}-{ENV}".lower()

    # Variables producto datos:
    dp_table_name = f"{COUNTRY}_{DATA_PRODUCT}"
    dp_yaml_name = dp_table_name
    dp_table_type = "hudi"
    dp_post_stage_path = f"s3://{bucket_gestionfraude_curated}/{SUBDOMAIN}/{COUNTRY}_{PRODUCT_SOURCE}/{dp_table_name}/"
    dp_table_type = "hudi"
    dp_write_mode = "append"
    dp_primary_key = "primary_key"
    dp_precombine_key = "primary_key"
    dp_partition = "year,month,day,hour"

    # -------------------------------------------------------------------
    # ------------------------ Proceso main -----------------------------
    # -------------------------------------------------------------------

    try:
        """ PRODUCTO DE DATOS: co_delfos_gestionfraude_fri_curado_logs_consultas_agentes """
        # Extract raw data
        logger.info(f"Leyendo data de: {sources.get('logs_consultas_agentes').get('databasename')}")

        # 0. Extracción de la información desde el bucket de S3.
        df: DataFrame = read_data_s3(glue_context, execution_timestamp, bucket_source)

        logger.info(f"Lectura completada de: {sources.get('logs_consultas_agentes').get('databasename')}, count df: {df.count()}")

        if df.count() > 0:

            """ PRODUCTO DE DATOS: co_logs_consultas_agentes """
            # Transform data
            logger.info(f"Iniciando Transformacion en: {dp_table_name}")
            df_dp_final = transform_product_logs_consultas_agentes(df, execution_timestamp)
            logger.info(f"Finalizando Transformacion en: {dp_table_name}, count: {df_dp_final.count()}")

            # Parametros configuración Hudi
            logs_hudi_config = get_hudi_config(
                database=database_gestionfraude_curated,
                table_name=dp_table_name,
                primary_key=dp_primary_key,
                precombine_field=dp_precombine_key,
                partition_field=dp_partition
            )

            # Loading data
            logger.info(f"Cargando datos con Hudi config: {logs_hudi_config}", )
            logger.info(f"Cargando datos DP ... en: {dp_table_name}")
            load_data(data=df_dp_final,
                yaml_name=dp_yaml_name,
                table_type=dp_table_type,
                options=logs_hudi_config,
                mode=dp_write_mode,
                target_path=dp_post_stage_path
            )
            job.commit()
            logger.info("Data Product guardado exitosamente!!")
        else:
            logger.info(f"Sin info nueva en el prefix s3: {dp_post_stage_path}")
    except Exception as error:
        logger.error(f"Falla el proceso en main {error}")
        raise error

if __name__ == '__main__':
    main()
    # este comentario esta afectando al Main.py..
