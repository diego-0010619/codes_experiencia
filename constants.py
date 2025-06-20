from pyspark.sql.types import StringType, DoubleType, IntegerType

# Constantes globales
DOMAIN: str = "gestionfraude"
SUBDOMAIN: str = "fri"
PRODUCT_SOURCE: str = "vista360"
CAPACITY: str = "delfos"
COUNTRY: str = "co"
PRODUCT: str = "co_vista360_logs_consulta_usuarios"

# Constantes por cada productos de datos
DATA_PRODUCT: str = "zendesk_logs_consultas_agentes"

# Definiendo tipos de datos de las variables.
COLUMN_TYPES = [
    ("user", StringType()),
    ("eventDate", IntegerType()),
    ("actionName", StringType()),
    ("affectedCustomer", StringType()),
    ("message", StringType()),
    ("typeOfAlert", StringType()),
    ("typeOfAlert", StringType()),
    ("origin", StringType()),
    ("accountNumber", StringType())
]
