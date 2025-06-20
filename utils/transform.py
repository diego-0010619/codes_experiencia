from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, coalesce, lit, unix_timestamp, to_timestamp, concat_ws,
    date_format, when, transform, map_from_entries, trim, lower, expr
)
from datetime import datetime


""" Transformaciones producto de datos logs consulta agentes """

def transform_data(df: DataFrame) -> DataFrame:
    """
    Transforma el DataFrame original extrayendo y normalizando los campos embebidos en el array JSON `attachments`.

    Esta función:
    - Extrae los elementos del array `attachments`, accediendo al primer elemento (`attachments[0]`).
    - Normaliza los campos `title` (como clave) y `value` (como valor), limpiando espacios y pasando a minúsculas.
    - Construye un mapa (`MapType`) llamado `attachments_fields` con las claves y valores del array `fields`.
    - Filtra y retorna solo las columnas relevantes: `text`, `attachments_fields`, y `channel`.

    Args:
        df (DataFrame): DataFrame original que contiene una columna `attachments`, la cual es un array de estructuras
                        con campos `fields`, cada uno con `title` y `value`.

    Returns:
        DataFrame: DataFrame transformado con las columnas:
            - `text` (string): Texto original del mensaje.
            - `attachments_fields` (map<string, string>): Mapa con los campos normalizados del primer attachment.
            - `channel` (string): Canal de donde proviene el mensaje.
    """

    df = df.withColumn(
        "attachments_fields",
        expr("""
            map_from_entries(
                transform(
                    attachments[0].fields,
                    x -> struct(
                        trim(lower(x.title)) as key,
                        IF(trim(x.value) = '', NULL, x.value) as value
                    )
                )
            )
        """)
    ).select(
        "text",
        "attachments_fields",
        "channel"
    )

    return df

def add_motivo_consulta(df: DataFrame) -> DataFrame:
    """
    Agrega la columna 'motivo_consulta' usando 'numero_870' o 'numero_nequi' del mapa 'attachments_fields'.

    Args:
        df (DataFrame): DataFrame con columna 'attachments_fields'

    Returns:
        DataFrame: DataFrame con columna 'motivo_consulta' agregada
    """
    return df.withColumn(
        "motivo_consulta",
        coalesce(col("attachments_fields")["numero_870"], col("attachments_fields")["numero_nequi"], lit("0"))
    )

def add_usuario_zendesk(df: DataFrame) -> DataFrame:
    """
    Agrega la columna 'usuario_zendesk' extraída de 'attachments_fields'.

    Args:
        df (DataFrame): DataFrame con columna 'attachments_fields'

    Returns:
        DataFrame: DataFrame con columna 'usuario_zendesk'
    """
    return df.withColumn("usuario_zendesk", col("attachments_fields")["usuario_zendesk"])

def add_fecha_hora_unix(df: DataFrame) -> DataFrame:
    """
    Agrega la columna 'fecha_hora_unix' como timestamp UNIX.

    Args:
        df (DataFrame): DataFrame con 'fecha' y 'hora' en 'attachments_fields'

    Returns:
        DataFrame: DataFrame con columna 'fecha_hora_unix'
    """
    return df.withColumn(
        "fecha_hora_unix",
        unix_timestamp(
            to_timestamp(
                concat_ws(" ", col("attachments_fields")["fecha"], col("attachments_fields")["hora"])
            )
        )
    )

def add_search_at(df: DataFrame) -> DataFrame:
    """
    Agrega la columna 'search_at' como timestamp parseado y combinado de 'fecha' y 'hora'.

    Args:
        df (DataFrame): DataFrame con 'fecha' y 'hora' en 'attachments_fields'

    Returns:
        DataFrame: DataFrame con columna 'search_at'
    """
    return df.withColumn(
        "search_at",
        to_timestamp(
            concat_ws(" ",
                date_format(to_timestamp(col("attachments_fields")["fecha"]), "yyyy-MM-dd"),
                date_format(to_timestamp(col("attachments_fields")["hora"], "HH:mm:ss.SSS'Z'"), "HH:mm:ss.SSS'Z'")
            )
        )
    )

def add_fecha(df: DataFrame) -> DataFrame:
    """
    Agrega la columna 'fecha' formateada como 'yyyy-MM-dd'.

    Args:
        df (DataFrame): DataFrame con 'fecha' en 'attachments_fields'

    Returns:
        DataFrame: DataFrame con columna 'fecha'
    """
    return df.withColumn(
        "fecha",
        date_format(to_timestamp(col("attachments_fields")["fecha"]), "yyyy-MM-dd")
    )

def add_hora(df: DataFrame) -> DataFrame:
    """
    Agrega la columna 'hora' formateada como 'HH:mm:ss.SSSZ'.

    Args:
        df (DataFrame): DataFrame con 'hora' en 'attachments_fields'

    Returns:
        DataFrame: DataFrame con columna 'hora'
    """
    return df.withColumn(
        "hora",
        date_format(to_timestamp(col("attachments_fields")["hora"], "HH:mm:ss.SSS'Z'"), "HH:mm:ss.SSS'Z'")
    )

def add_validacion_cuenta(df: DataFrame) -> DataFrame:
    """
    Valida el campo 'numero_870' y crea la columna 'validacion_cuenta'.

    Args:
        df (DataFrame): DataFrame con 'numero_870' en 'attachments_fields'

    Returns:
        DataFrame: DataFrame con columna 'validacion_cuenta'
    """
    return df.withColumn(
        "validacion_cuenta",
        when(col("attachments_fields")["numero_870"].isNull(), "Error: Campo nulo")
        .when(~col("attachments_fields")["numero_870"].rlike("^[0-9]+$"), "Error: No es un número")
        .when(~col("attachments_fields")["numero_870"].rlike("^870"), "Error: No empieza por 870")
        .otherwise("campo valido")
    )

def add_primary_key(df: DataFrame) -> DataFrame:
    """
    Crea una columna 'primary_key' combinando fecha_hora_unix, numero y usuario_zendesk.

    Args:
        df (DataFrame): DataFrame con 'fecha_hora_unix', 'numero', 'usuario_zendesk'

    Returns:
        DataFrame: DataFrame con columna 'primary_key'
    """
    return df.withColumn(
        "primary_key",
        concat_ws("_",
            col("fecha_hora_unix").cast("string"),
            coalesce(col("attachments_fields")["numero_870"], col("attachments_fields")["numero_nequi"], lit("0")),
            col("attachments_fields")["usuario_zendesk"]
        )
    )

def add_execution_timestamp(df: DataFrame, execution_timestamp: str) -> DataFrame:
    """
    Agrega una columna 'execution_timestamp' con un valor fijo proporcionado.

    Args:
        df (DataFrame): DataFrame original
        execution_timestamp (str): Timestamp en formato 'yyyy-MM-dd HH:mm:ss'

    Returns:
        DataFrame: DataFrame con 'execution_timestamp'
    """
    return df.withColumn(
        "execution_timestamp",
        to_timestamp(lit(execution_timestamp), "yyyy-MM-dd HH:mm:ss")
    )

""" Transformaciones producto de datos logs consulta agentes """
