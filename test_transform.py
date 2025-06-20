import pytest
import datetime
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType, StructField, StructType, DateType, TimestampType, LongType, ArrayType, MapType, BooleanType, IntegerType

from pyspark.sql.functions import (
    col, coalesce, lit, unix_timestamp, to_timestamp, concat_ws,
    date_format, when, transform, map_from_entries, trim, lower, expr
)

from src.product_logs_consultas_agentes.extra.utils.transform import (
    transform_data, add_motivo_consulta, add_usuario_zendesk,
    add_fecha_hora_unix, add_search_at, add_fecha, add_hora, add_validacion_cuenta,
    add_primary_key, add_execution_timestamp
)


@pytest.fixture
def spark():
    spark: SparkSession = SparkSession.builder.appName("test_glue_job").getOrCreate()
    return spark

@pytest.fixture
def df_sample_data_raw(spark: SparkSession):
    # Definir esquema del DataFrame
    schema = StructType([
        StructField("text", StringType(), True),
        StructField("attachments", ArrayType(
            StructType([
                StructField("color", StringType(), True),
                StructField("fields", ArrayType(
                    StructType([
                        StructField("short", BooleanType(), True),
                        StructField("value", StringType(), True),
                        StructField("title", StringType(), True),
                    ])
                ), True),
                StructField("attachment_type", StringType(), True)
            ])
        ), True),
        StructField("channel", StringType(), True)
    ])

    # Crear datos de ejemplo
    data = [
        (
            "Validación de auditoria",
            [
                {
                    "color": "#519872",
                    "fields": [
                        {"short": True, "value": "2025-03-31", "title": "Fecha"},
                        {"short": True, "value": "19:07:52.970Z", "title": "Hora"},
                        {"short": True, "value": "3003992564", "title": "Numero_Nequi"},
                        {"short": True, "value": "87042338313", "title": "Numero_870"},
                        {"short": True, "value": "usuario@empresa.com", "title": "Usuario_Zendesk"},
                        {"short": True, "value": "Consulta de persona", "title": "Modulos"}
                    ],
                    "attachment_type": "default"
                }
            ],
            "#validacion_auditoria"
        ),
        (
            "Validación de auditoria",
            [
                {
                    "color": "#519872",
                    "fields": [
                        {"short": True, "value": "2025-03-31", "title": "Fecha"},
                        {"short": True, "value": "19:08:36.318Z", "title": "Hora"},
                        {"short": True, "value": "3146214892", "title": "Numero_Nequi"},
                        {"short": True, "value": "87051369232", "title": "Numero_870"},
                        {"short": True, "value": "usuario2@empresa.com", "title": "Usuario_Zendesk"},
                        {"short": True, "value": "Consulta de persona", "title": "Modulos"}
                    ],
                    "attachment_type": "default"
                }
            ],
            "#validacion_auditoria"
        )
    ]

    return spark.createDataFrame(data=data, schema=schema)
@pytest.fixture
def df_sample_data_base(spark: SparkSession):
    schema = StructType([
        StructField("text", StringType(), True),
        StructField("attachments_fields", MapType(StringType(), StringType(), True), True),
        StructField("channel", StringType(), True)
    ])

    data = [
        (
            "Validación de auditoria",
            {
                "fecha": "2025-03-31",
                "hora": "19:07:52.970Z",
                "numero_nequi": "3003992564",
                "numero_870": "87042338313",
                "usuario_zendesk": "usuario@empresa.com",
                "modulos": "Consulta de persona"
            },
            "#validacion_auditoria"
        ),
        (
            "Validación de auditoria",
            {
                "fecha": "2025-03-31",
                "hora": "19:08:36.318Z",
                "numero_nequi": "3146214892",
                "numero_870": "87051369232",
                "usuario_zendesk": "usuario2@empresa.com",
                "modulos": "Consulta de persona"
            },
            "#validacion_auditoria"
        )
    ]

    return spark.createDataFrame(data=data, schema=schema)
@pytest.fixture
def df_with_motivo(spark: SparkSession):
    schema = StructType([
        StructField("text", StringType(), True),
        StructField("attachments_fields", MapType(StringType(), StringType(), True), True),
        StructField("channel", StringType(), True),
        StructField("motivo_consulta", StringType(), False)
    ])

    data = [
        (
            "Validación de auditoria",
            {
                "fecha": "2025-03-31",
                "hora": "19:07:52.970Z",
                "numero_nequi": "3003992564",
                "numero_870": "87042338313",
                "usuario_zendesk": "usuario@empresa.com",
                "modulos": "Consulta de persona"
            },
            "#validacion_auditoria",
            "87042338313"
        ),
        (
            "Validación de auditoria",
            {
                "fecha": "2025-03-31",
                "hora": "19:08:36.318Z",
                "numero_nequi": "3146214892",
                "numero_870": "87051369232",
                "usuario_zendesk": "usuario2@empresa.com",
                "modulos": "Consulta de persona"
            },
            "#validacion_auditoria",
            "87051369232"
        )
    ]

    return spark.createDataFrame(data=data, schema=schema)
@pytest.fixture
def df_fecha_hora_unix(spark: SparkSession):
    schema = StructType([
        StructField("text", StringType(), True),
        StructField("attachments_fields", MapType(StringType(), StringType(), True), True),
        StructField("channel", StringType(), True),
        StructField("motivo_consulta", StringType(), False),
        StructField("usuario_zendesk", StringType(), True)
    ])

    data = [
        (
            "Validación de auditoria",
            {
                "fecha": "2025-03-31",
                "hora": "19:07:52.970Z",
                "numero_nequi": "3003992564",
                "numero_870": "87042338313",
                "usuario_zendesk": "usuario@empresa.com",
                "modulos": "Consulta de persona"
            },
            "#validacion_auditoria",
            "87042338313",
            "usuario@empresa.com"
        ),
        (
            "Validación de auditoria",
            {
                "fecha": "2025-03-31",
                "hora": "19:08:36.318Z",
                "numero_nequi": "3146214892",
                "numero_870": "87051369232",
                "usuario_zendesk": "usuario2@empresa.com",
                "modulos": "Consulta de persona"
            },
            "#validacion_auditoria",
            "87051369232",
            "usuario2@empresa.com"
        )
    ]


    return spark.createDataFrame(data=data, schema=schema)
@pytest.fixture
def df_search_at(spark: SparkSession):
    schema = StructType([
        StructField("text", StringType(), True),
        StructField("attachments_fields", MapType(StringType(), StringType(), True), True),
        StructField("channel", StringType(), True),
        StructField("motivo_consulta", StringType(), True),
        StructField("usuario_zendesk", StringType(), True),
        StructField("fecha_hora_unix", IntegerType(), True)
    ])

    data = [
        (
            "Validación de auditoria",
            {
                "fecha": "2025-03-31",
                "hora": "19:07:52.970Z",
                "numero_nequi": "3003992564",
                "numero_870": "87042338313",
                "usuario_zendesk": "usuario@empresa.com",
                "modulos": "Consulta de persona"
            },
            "#validacion_auditoria",
            "87042338313",
            "usuario@empresa.com",
            1743448072
        ),
        (
            "Validación de auditoria",
            {
                "fecha": "2025-03-31",
                "hora": "19:08:36.318Z",
                "numero_nequi": "3146214892",
                "numero_870": "87051369232",
                "usuario_zendesk": "usuario2@empresa.com",
                "modulos": "Consulta de persona"
            },
            "#validacion_auditoria",
            "87051369232",
            "usuario2@empresa.com",
            1743448116
        )
    ]

    return spark.createDataFrame(data=data, schema=schema)
@pytest.fixture
def df_add_fecha(spark: SparkSession):
    schema = StructType([
        StructField("text", StringType(), True),
        StructField("attachments_fields", MapType(StringType(), StringType(), True), True),
        StructField("channel", StringType(), True),
        StructField("motivo_consulta", StringType(), True),
        StructField("usuario_zendesk", StringType(), True),
        StructField("fecha_hora_unix", IntegerType(), True),
        StructField("search_at", TimestampType(), True)
    ])

    data = [
        (
            "Validación de auditoria",
            {
                "fecha": "2025-03-31",
                "hora": "19:07:52.970Z",
                "numero_nequi": "3003992564",
                "numero_870": "87042338313",
                "usuario_zendesk": "usuario@empresa.com",
                "modulos": "Consulta de persona"
            },
            "#validacion_auditoria",
            "87042338313",
            "usuario@empresa.com",
            1743448072,
            datetime(2025, 3, 31, 19, 7, 52, 970000)
        ),
        (
            "Validación de auditoria",
            {
                "fecha": "2025-03-31",
                "hora": "19:08:36.318Z",
                "numero_nequi": "3146214892",
                "numero_870": "87051369232",
                "usuario_zendesk": "usuario2@empresa.com",
                "modulos": "Consulta de persona"
            },
            "#validacion_auditoria",
            "87051369232",
            "usuario2@empresa.com",
            1743448116,
            datetime(2025, 3, 31, 19, 8, 36, 318000)
        )
    ]

    return spark.createDataFrame(data=data, schema=schema)
@pytest.fixture
def df_add_hora(spark: SparkSession):
    schema = StructType([
        StructField("text", StringType(), True),
        StructField("attachments_fields", MapType(StringType(), StringType(), True), True),
        StructField("channel", StringType(), True),
        StructField("motivo_consulta", StringType(), True),
        StructField("usuario_zendesk", StringType(), True),
        StructField("fecha_hora_unix", IntegerType(), True),
        StructField("search_at", TimestampType(), True),
        StructField("fecha", StringType(), True)
    ])

    data = [
        (
            "Validación de auditoria",
            {
                "fecha": "2025-03-31",
                "hora": "19:07:52.970Z",
                "numero_nequi": "3003992564",
                "numero_870": "87042338313",
                "usuario_zendesk": "usuario@empresa.com",
                "modulos": "Consulta de persona"
            },
            "#validacion_auditoria",
            "87042338313",
            "usuario@empresa.com",
            1743448072,
            datetime(2025, 3, 31, 19, 7, 52, 970000),
            "2025-03-31"
        ),
        (
            "Validación de auditoria",
            {
                "fecha": "2025-03-31",
                "hora": "19:08:36.318Z",
                "numero_nequi": "3146214892",
                "numero_870": "87051369232",
                "usuario_zendesk": "usuario2@empresa.com",
                "modulos": "Consulta de persona"
            },
            "#validacion_auditoria",
            "87051369232",
            "usuario2@empresa.com",
            1743448116,
            datetime(2025, 3, 31, 19, 8, 36, 318000),
            "2025-03-31"
        )
    ]

    return spark.createDataFrame(data=data, schema=schema)
@pytest.fixture
def df_sample_for_validacion_cuenta(spark: SparkSession):
    schema = StructType([
        StructField("text", StringType(), True),
        StructField("attachments_fields", MapType(StringType(), StringType(), True), True),
        StructField("channel", StringType(), True),
        StructField("motivo_consulta", StringType(), True),
        StructField("usuario_zendesk", StringType(), True),
        StructField("fecha_hora_unix", IntegerType(), True),
        StructField("search_at", TimestampType(), True),
        StructField("fecha", StringType(), True),
        StructField("hora", StringType(), True)
    ])

    data = [
        (
            "Validación de auditoria",
            {
                "fecha": "2025-03-31",
                "hora": "19:07:52.970Z",
                "numero_nequi": "3003992564",
                "numero_870": "87042338313",
                "usuario_zendesk": "usuario@empresa.com",
                "modulos": "Consulta de persona"
            },
            "#validacion_auditoria",
            "87042338313",
            "usuario@empresa.com",
            1743448072,
            datetime(2025, 3, 31, 19, 7, 52, 970000),
            "2025-03-31",
            "19:07:52.970Z"
        ),
        (
            "Validación de auditoria",
            {
                "fecha": "2025-03-31",
                "hora": "19:08:36.318Z",
                "numero_nequi": "3146214892",
                "numero_870": None,
                "usuario_zendesk": "usuario2@empresa.com",
                "modulos": "Consulta de persona"
            },
            "#validacion_auditoria",
            "87051369232",
            "usuario2@empresa.com",
            1743448116,
            datetime(2025, 3, 31, 19, 8, 36, 318000),
            "2025-03-31",
            "19:08:36.318Z"
        )
    ]

    return spark.createDataFrame(data=data, schema=schema)
@pytest.fixture
def df_add_primary_key(spark: SparkSession):
    schema = StructType([
        StructField("text", StringType(), True),
        StructField("attachments_fields", MapType(StringType(), StringType(), True), True),
        StructField("channel", StringType(), True),
        StructField("motivo_consulta", StringType(), True),
        StructField("usuario_zendesk", StringType(), True),
        StructField("fecha_hora_unix", IntegerType(), True),
        StructField("search_at", TimestampType(), True),
        StructField("fecha", StringType(), True),
        StructField("hora", StringType(), True),
        StructField("validacion_cuenta", StringType(), False)
    ])

    data = [
        (
            "Validación de auditoria",
            {
                "fecha": "2025-03-31",
                "hora": "19:07:52.970Z",
                "numero_nequi": "3003992564",
                "numero_870": "87042338313",
                "usuario_zendesk": "usuario@empresa.com",
                "modulos": "Consulta de persona"
            },
            "#validacion_auditoria",
            "87042338313",
            "usuario@empresa.com",
            1743448072,
            datetime(2025, 3, 31, 19, 7, 52),
            "2025-03-31",
            "19:07:52.970Z",
            "campo valido"
        ),
        (
            "Validación de auditoria",
            {
                "fecha": "2025-03-31",
                "hora": "19:08:36.318Z",
                "numero_nequi": "3146214892",
                "numero_870": "87051369232",
                "usuario_zendesk": "usuario2@empresa.com",
                "modulos": "Consulta de persona"
            },
            "#validacion_auditoria",
            "87051369232",
            "usuario2@empresa.com",
            1743448116,
            datetime(2025, 3, 31, 19, 8, 36),
            "2025-03-31",
            "19:08:36.318Z",
            "campo valido"
        )
    ]

    return spark.createDataFrame(data=data, schema=schema)
@pytest.fixture
def df_add_execution_timestamp(spark: SparkSession):
    schema = StructType([
        StructField("text", StringType(), True),
        StructField("attachments_fields", MapType(StringType(), StringType(), True), True),
        StructField("channel", StringType(), True),
        StructField("motivo_consulta", StringType(), True),
        StructField("usuario_zendesk", StringType(), True),
        StructField("fecha_hora_unix", IntegerType(), True),
        StructField("search_at", TimestampType(), True),
        StructField("fecha", StringType(), True),
        StructField("hora", StringType(), True),
        StructField("validacion_cuenta", StringType(), False),
        StructField("primary_key", StringType(), True)
    ])

    data = [
        (
            "Validación de auditoria",
            {
                "fecha": "2025-03-31",
                "hora": "19:07:52.970Z",
                "numero_nequi": "3003992564",
                "numero_870": "87042338313",
                "usuario_zendesk": "usuario@empresa.com",
                "modulos": "Consulta de persona"
            },
            "#validacion_auditoria",
            "87042338313",
            "usuario@empresa.com",
            1743448072,
            datetime(2025, 3, 31, 19, 7, 52),
            "2025-03-31",
            "19:07:52.970Z",
            "campo valido",
            "1743448072_87042338313_usuario@empresa.com"
        ),
        (
            "Validación de auditoria",
            {
                "fecha": "2025-03-31",
                "hora": "19:08:36.318Z",
                "numero_nequi": "3146214892",
                "numero_870": "87051369232",
                "usuario_zendesk": "usuario2@empresa.com",
                "modulos": "Consulta de persona"
            },
            "#validacion_auditoria",
            "87051369232",
            "usuario2@empresa.com",
            1743448116,
            datetime(2025, 3, 31, 19, 8, 36),
            "2025-03-31",
            "19:08:36.318Z",
            "campo valido",
            "1743448116_87051369232_usuario2@empresa.com"
        )
    ]

    return spark.createDataFrame(data=data, schema=schema)

# INICIO test función transform_data

def test_transform_data(spark: SparkSession, df_sample_data_raw: DataFrame):
    df_out = transform_data(df_sample_data_raw)

    schema_expected = StructType([
        StructField("text", StringType(), True),
        StructField("attachments_fields", MapType(StringType(), StringType(), True), True),
        StructField("channel", StringType(), True)
    ])

    data_expected = [
        (
            "Validación de auditoria",
            {
                "fecha": "2025-03-31",
                "hora": "19:07:52.970Z",
                "numero_nequi": "3003992564",
                "numero_870": "87042338313",
                "usuario_zendesk": "usuario@empresa.com",
                "modulos": "Consulta de persona"
            },
            "#validacion_auditoria"
        ),
        (
            "Validación de auditoria",
            {
                "fecha": "2025-03-31",
                "hora": "19:08:36.318Z",
                "numero_nequi": "3146214892",
                "numero_870": "87051369232",
                "usuario_zendesk": "usuario2@empresa.com",
                "modulos": "Consulta de persona"
            },
            "#validacion_auditoria"
        )
    ]

    df_expected = spark.createDataFrame(data=data_expected, schema=schema_expected)

    assert df_out.schema == df_expected.schema
    assert df_out.collect() == df_expected.collect()

# FIN test función transform_data


# INICIO test función add_motivo_consulta

def test_add_motivo_consulta(spark: SparkSession, df_sample_data_base: DataFrame):
    df_out = add_motivo_consulta(df_sample_data_base)

    schema_expected = StructType([
        StructField("text", StringType(), True),
        StructField("attachments_fields", MapType(StringType(), StringType(), True), True),
        StructField("channel", StringType(), True),
        StructField("motivo_consulta", StringType(), False)
    ])

    data_expected = [
        (
            "Validación de auditoria",
            {
                "fecha": "2025-03-31",
                "hora": "19:07:52.970Z",
                "numero_nequi": "3003992564",
                "numero_870": "87042338313",
                "usuario_zendesk": "usuario@empresa.com",
                "modulos": "Consulta de persona"
            },
            "#validacion_auditoria",
            "87042338313"
        ),
        (
            "Validación de auditoria",
            {
                "fecha": "2025-03-31",
                "hora": "19:08:36.318Z",
                "numero_nequi": "3146214892",
                "numero_870": "87051369232",
                "usuario_zendesk": "usuario2@empresa.com",
                "modulos": "Consulta de persona"
            },
            "#validacion_auditoria",
            "87051369232"
        )
    ]

    df_expected = spark.createDataFrame(data=data_expected, schema=schema_expected)

    assert df_out.schema == df_expected.schema
    assert df_out.collect() == df_expected.collect()

# FIN test función add_motivo_consulta


# INICIO test función add_usuario_zendesk

def test_add_usuario_zendesk(spark: SparkSession, df_with_motivo: DataFrame):
    df_out = add_usuario_zendesk(df_with_motivo)

    schema_expected = StructType([
        StructField("text", StringType(), True),
        StructField("attachments_fields", MapType(StringType(), StringType(), True), True),
        StructField("channel", StringType(), True),
        StructField("motivo_consulta", StringType(), False),
        StructField("usuario_zendesk", StringType(), True)
    ])

    data_expected = [
        (
            "Validación de auditoria",
            {
                "fecha": "2025-03-31",
                "hora": "19:07:52.970Z",
                "numero_nequi": "3003992564",
                "numero_870": "87042338313",
                "usuario_zendesk": "usuario@empresa.com",
                "modulos": "Consulta de persona"
            },
            "#validacion_auditoria",
            "87042338313",
            "usuario@empresa.com"
        ),
        (
            "Validación de auditoria",
            {
                "fecha": "2025-03-31",
                "hora": "19:08:36.318Z",
                "numero_nequi": "3146214892",
                "numero_870": "87051369232",
                "usuario_zendesk": "usuario2@empresa.com",
                "modulos": "Consulta de persona"
            },
            "#validacion_auditoria",
            "87051369232",
            "usuario2@empresa.com"
        )
    ]

    df_expected = spark.createDataFrame(data=data_expected, schema=schema_expected)

    assert df_out.schema == df_expected.schema
    assert df_out.collect() == df_expected.collect()

# FIN test función add_usuario_zendesk


# INICIO test función add_fecha_hora_unix

def test_add_fecha_hora_unix(spark: SparkSession, df_fecha_hora_unix: DataFrame):
    df_out = add_fecha_hora_unix(df_fecha_hora_unix)

    schema_expected = StructType([
        StructField("text", StringType(), True),
        StructField("attachments_fields", MapType(StringType(), StringType(), True), True),
        StructField("channel", StringType(), True),
        StructField("motivo_consulta", StringType(), False),
        StructField("usuario_zendesk", StringType(), True),
        StructField("fecha_hora_unix", LongType(), True)
    ])

    data_expected = [
        (
            "Validación de auditoria",
            {
                "fecha": "2025-03-31",
                "hora": "19:07:52.970Z",
                "numero_nequi": "3003992564",
                "numero_870": "87042338313",
                "usuario_zendesk": "usuario@empresa.com",
                "modulos": "Consulta de persona"
            },
            "#validacion_auditoria",
            "87042338313",
            "usuario@empresa.com",
            1743448072
        ),
        (
            "Validación de auditoria",
            {
                "fecha": "2025-03-31",
                "hora": "19:08:36.318Z",
                "numero_nequi": "3146214892",
                "numero_870": "87051369232",
                "usuario_zendesk": "usuario2@empresa.com",
                "modulos": "Consulta de persona"
            },
            "#validacion_auditoria",
            "87051369232",
            "usuario2@empresa.com",
            1743448116
        )
    ]

    df_expected = spark.createDataFrame(data=data_expected, schema=schema_expected)

    assert df_out.schema == df_expected.schema
    assert df_out.collect() == df_expected.collect()

# FIN test función add_fecha_hora_unix


# INICIO test función add_search_at

def test_add_search_at(spark: SparkSession, df_search_at: DataFrame):
    df_out = add_search_at(df_search_at)

    schema_expected = StructType([
        StructField("text", StringType(), True),
        StructField("attachments_fields", MapType(StringType(), StringType(), True), True),
        StructField("channel", StringType(), True),
        StructField("motivo_consulta", StringType(), True),
        StructField("usuario_zendesk", StringType(), True),
        StructField("fecha_hora_unix", IntegerType(), True),
        StructField("search_at", TimestampType(), True)
    ])

    data_expected = [
        (
            "Validación de auditoria",
            {
                "fecha": "2025-03-31",
                "hora": "19:07:52.970Z",
                "numero_nequi": "3003992564",
                "numero_870": "87042338313",
                "usuario_zendesk": "usuario@empresa.com",
                "modulos": "Consulta de persona"
            },
            "#validacion_auditoria",
            "87042338313",
            "usuario@empresa.com",
            1743448072,
            datetime(2025, 3, 31, 19, 7, 52, 970000),
        ),
        (
            "Validación de auditoria",
            {
                "fecha": "2025-03-31",
                "hora": "19:08:36.318Z",
                "numero_nequi": "3146214892",
                "numero_870": "87051369232",
                "usuario_zendesk": "usuario2@empresa.com",
                "modulos": "Consulta de persona"
            },
            "#validacion_auditoria",
            "87051369232",
            "usuario2@empresa.com",
            1743448116,  # ESTE ES fecha_hora_unix
            datetime(2025, 3, 31, 19, 8, 36, 318000),
        )
    ]

    df_expected = spark.createDataFrame(data=data_expected, schema=schema_expected)

    assert df_out.schema == df_expected.schema
    assert df_out.collect() == df_expected.collect()

# FIN test función add_search_at


# INICIO test función add_fecha

def test_add_fecha(spark: SparkSession, df_add_fecha: DataFrame):
    df_out = add_fecha(df_add_fecha)

    schema_expected = StructType([
        StructField("text", StringType(), True),
        StructField("attachments_fields", MapType(StringType(), StringType(), True), True),
        StructField("channel", StringType(), True),
        StructField("motivo_consulta", StringType(), True),
        StructField("usuario_zendesk", StringType(), True),
        StructField("fecha_hora_unix", IntegerType(), True),
        StructField("search_at", TimestampType(), True),
        StructField("fecha", StringType(), True)
    ])

    data_expected = [
        (
            "Validación de auditoria",
            {
                "fecha": "2025-03-31",
                "hora": "19:07:52.970Z",
                "numero_nequi": "3003992564",
                "numero_870": "87042338313",
                "usuario_zendesk": "usuario@empresa.com",
                "modulos": "Consulta de persona"
            },
            "#validacion_auditoria",
            "87042338313",
            "usuario@empresa.com",
            1743448072,
            datetime(2025, 3, 31, 19, 7, 52, 970000),
            "2025-03-31"
        ),
        (
            "Validación de auditoria",
            {
                "fecha": "2025-03-31",
                "hora": "19:08:36.318Z",
                "numero_nequi": "3146214892",
                "numero_870": "87051369232",
                "usuario_zendesk": "usuario2@empresa.com",
                "modulos": "Consulta de persona"
            },
            "#validacion_auditoria",
            "87051369232",
            "usuario2@empresa.com",
            1743448116,
            datetime(2025, 3, 31, 19, 8, 36, 318000),
            "2025-03-31"
        )
    ]
    df_expected = spark.createDataFrame(data=data_expected, schema=schema_expected)

    assert df_out.schema == df_expected.schema
    assert df_out.collect() == df_expected.collect()

# FIN test función add_fecha


# INICIO test función add_hora

def test_add_hora(spark: SparkSession, df_add_hora: DataFrame):
    df_out = add_hora(df_add_hora)

    schema_expected = StructType([
        StructField("text", StringType(), True),
        StructField("attachments_fields", MapType(StringType(), StringType(), True), True),
        StructField("channel", StringType(), True),
        StructField("motivo_consulta", StringType(), True),
        StructField("usuario_zendesk", StringType(), True),
        StructField("fecha_hora_unix", IntegerType(), True),
        StructField("search_at", TimestampType(), True),
        StructField("fecha", StringType(), True),
        StructField("hora", StringType(), True)
    ])

    data_expected = [
        (
            "Validación de auditoria",
            {
                "fecha": "2025-03-31",
                "hora": "19:07:52.970Z",
                "numero_nequi": "3003992564",
                "numero_870": "87042338313",
                "usuario_zendesk": "usuario@empresa.com",
                "modulos": "Consulta de persona"
            },
            "#validacion_auditoria",
            "87042338313",
            "usuario@empresa.com",
            1743448072,
            datetime(2025, 3, 31, 19, 7, 52, 970000),
            "2025-03-31",
            "19:07:52.970Z"
        ),
        (
            "Validación de auditoria",
            {
                "fecha": "2025-03-31",
                "hora": "19:08:36.318Z",
                "numero_nequi": "3146214892",
                "numero_870": "87051369232",
                "usuario_zendesk": "usuario2@empresa.com",
                "modulos": "Consulta de persona"
            },
            "#validacion_auditoria",
            "87051369232",
            "usuario2@empresa.com",
            1743448116,
            datetime(2025, 3, 31, 19, 8, 36, 318000),
            "2025-03-31",
            "19:08:36.318Z"
        )
    ]

    df_expected = spark.createDataFrame(data=data_expected, schema=schema_expected)

    assert df_out.schema == df_expected.schema
    assert df_out.collect() == df_expected.collect()

# FIN test función add_hora


# INICIO test función add_validacion_cuenta

def test_add_validacion_cuenta(spark: SparkSession, df_sample_for_validacion_cuenta: DataFrame):
    df_out = add_validacion_cuenta(df_sample_for_validacion_cuenta)

    schema_expected = StructType([
        StructField("text", StringType(), True),
        StructField("attachments_fields", MapType(StringType(), StringType(), True), True),
        StructField("channel", StringType(), True),
        StructField("motivo_consulta", StringType(), True),
        StructField("usuario_zendesk", StringType(), True),
        StructField("fecha_hora_unix", IntegerType(), True),
        StructField("search_at", TimestampType(), True),
        StructField("fecha", StringType(), True),
        StructField("hora", StringType(), True),
        StructField("validacion_cuenta", StringType(), False)  # <- Cambiado aquí
    ])

    data_expected = [
        (
            "Validación de auditoria",
            {
                "fecha": "2025-03-31",
                "hora": "19:07:52.970Z",
                "numero_nequi": "3003992564",
                "numero_870": "87042338313",
                "usuario_zendesk": "usuario@empresa.com",
                "modulos": "Consulta de persona"
            },
            "#validacion_auditoria",
            "87042338313",
            "usuario@empresa.com",
            1743448072,
            datetime(2025, 3, 31, 19, 7, 52, 970000),
            "2025-03-31",
            "19:07:52.970Z",
            "campo valido"
        ),
        (
            "Validación de auditoria",
            {
                "fecha": "2025-03-31",
                "hora": "19:08:36.318Z",
                "numero_nequi": "3146214892",
                "numero_870": None,
                "usuario_zendesk": "usuario2@empresa.com",
                "modulos": "Consulta de persona"
            },
            "#validacion_auditoria",
            "87051369232",
            "usuario2@empresa.com",
            1743448116,
            datetime(2025, 3, 31, 19, 8, 36, 318000),
            "2025-03-31",
            "19:08:36.318Z",
            "Error: Campo nulo"
        )
    ]

    df_expected = spark.createDataFrame(data=data_expected, schema=schema_expected)

    assert df_out.schema == df_expected.schema
    assert df_out.collect() == df_expected.collect()

# FIN test función add_validacion_cuenta


# INICIO test función test_add_primary_key

def test_add_primary_key(spark: SparkSession, df_add_primary_key: DataFrame):
    df_out = add_primary_key(df_add_primary_key)

    schema_expected = StructType([
        StructField("text", StringType(), True),
        StructField("attachments_fields", MapType(StringType(), StringType(), True), True),
        StructField("channel", StringType(), True),
        StructField("motivo_consulta", StringType(), True),
        StructField("usuario_zendesk", StringType(), True),
        StructField("fecha_hora_unix", IntegerType(), True),
        StructField("search_at", TimestampType(), True),
        StructField("fecha", StringType(), True),
        StructField("hora", StringType(), True),
        StructField("validacion_cuenta", StringType(), False),
        StructField("primary_key", StringType(), False)  # <<< Aquí está bien con nullable=True
    ])

    data_expected = [
        (
            "Validación de auditoria",
            {
                "fecha": "2025-03-31",
                "hora": "19:07:52.970Z",
                "numero_nequi": "3003992564",
                "numero_870": "87042338313",
                "usuario_zendesk": "usuario@empresa.com",
                "modulos": "Consulta de persona"
            },
            "#validacion_auditoria",
            "87042338313",
            "usuario@empresa.com",
            1743448072,
            datetime(2025, 3, 31, 19, 7, 52),
            "2025-03-31",
            "19:07:52.970Z",
            "campo valido",
            "1743448072_87042338313_usuario@empresa.com"
        ),
        (
            "Validación de auditoria",
            {
                "fecha": "2025-03-31",
                "hora": "19:08:36.318Z",
                "numero_nequi": "3146214892",
                "numero_870": "87051369232",
                "usuario_zendesk": "usuario2@empresa.com",
                "modulos": "Consulta de persona"
            },
            "#validacion_auditoria",
            "87051369232",
            "usuario2@empresa.com",
            1743448116,
            datetime(2025, 3, 31, 19, 8, 36),
            "2025-03-31",
            "19:08:36.318Z",
            "campo valido",
            "1743448116_87051369232_usuario2@empresa.com"
        )
    ]

    df_expected = spark.createDataFrame(data=data_expected, schema=schema_expected)

    assert df_out.schema == df_expected.schema
    assert df_out.collect() == df_expected.collect()

# FIN test función test_add_primary_key


# INICIO test función add_execution_timestamp

def test_add_execution_timestamp(spark: SparkSession, df_add_execution_timestamp: DataFrame):
    execution_timestamp = "2025-04-28 14:00:00"
    df_out = add_execution_timestamp(df_add_execution_timestamp, execution_timestamp)

    schema_expected = StructType([
        StructField("text", StringType(), True),
        StructField("attachments_fields", MapType(StringType(), StringType(), True), True),
        StructField("channel", StringType(), True),
        StructField("motivo_consulta", StringType(), True),
        StructField("usuario_zendesk", StringType(), True),
        StructField("fecha_hora_unix", IntegerType(), True),
        StructField("search_at", TimestampType(), True),
        StructField("fecha", StringType(), True),
        StructField("hora", StringType(), True),
        StructField("validacion_cuenta", StringType(), False),
        StructField("primary_key", StringType(), True),
        StructField("execution_timestamp", TimestampType(), True)  # <- este es el nuevo campo
    ])

    data_expected = [
        (
            "Validación de auditoria",
            {
                "fecha": "2025-03-31",
                "hora": "19:07:52.970Z",
                "numero_nequi": "3003992564",
                "numero_870": "87042338313",
                "usuario_zendesk": "usuario@empresa.com",
                "modulos": "Consulta de persona"
            },
            "#validacion_auditoria",
            "87042338313",
            "usuario@empresa.com",
            1743448072,
            datetime(2025, 3, 31, 19, 7, 52),
            "2025-03-31",
            "19:07:52.970Z",
            "campo valido",
            "1743448072_87042338313_usuario@empresa.com",
            datetime(2025, 4, 28, 14, 0, 0)
        ),
        (
            "Validación de auditoria",
            {
                "fecha": "2025-03-31",
                "hora": "19:08:36.318Z",
                "numero_nequi": "3146214892",
                "numero_870": "87051369232",
                "usuario_zendesk": "usuario2@empresa.com",
                "modulos": "Consulta de persona"
            },
            "#validacion_auditoria",
            "87051369232",
            "usuario2@empresa.com",
            1743448116,
            datetime(2025, 3, 31, 19, 8, 36),
            "2025-03-31",
            "19:08:36.318Z",
            "campo valido",
            "1743448116_87051369232_usuario2@empresa.com",
            datetime(2025, 4, 28, 14, 0, 0)
        )
    ]

    df_expected = spark.createDataFrame(data=data_expected, schema=schema_expected)

    assert df_out.schema == df_expected.schema
    assert df_out.collect() == df_expected.collect()

# FIN test función add_execution_timestamp
