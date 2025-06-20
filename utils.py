import logging
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import trim, to_date, lit


# Creacion de logger
def create_custom_logger(app_name: str):
    """Funcion auxiliar que crea un logger desde el modulo de utilidades utils.py

    Args:
        app_name (str):
            Nombre de la aplicacion, en el archivo en que se crea este objeto
            se pasa como argumento > __name__.

        https://docs.python.org/3/library/logging.html#logging.LogRecord
        - fecha de ejecucion >   %(asctime)s
        - nivel de mensaje   >   [%(levelname)s]
        - nombre del logger  >   [Logger: %(name)s]
        - ruta al archivo    >   [File: %(pathname)s]
        - mensaje a loguear  >   %(message)s
        - linea de ejecucion >   (Line: %(lineno)d)

    Returns:
        logger: Formato de logueo de mensajes
    """
    logger = logging.getLogger(app_name)  # __name__
    MSG_FORMAT = "%(asctime)s [%(levelname)s] [Logger: %(name)s]: %(message)s (Line: %(lineno)d)"
    DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
    logging.basicConfig(format=MSG_FORMAT,
                        datefmt=DATETIME_FORMAT, level=logging.INFO)
    return logger

logger = create_custom_logger(__name__)

def windows_months_read(execution_date: str,
                        name_column: str,
                        window_month: int) -> str:
    # Convertir la fecha de ejecución a un objeto datetime
    exec_date = datetime.strptime(execution_date, "%Y-%m-%d")

    # Ajustar al primer día del mes actual
    exec_date_start_month = exec_date.replace(day=1)

    # Calcular la fecha inicial (primer día del mes de hace `window_month` meses)
    date_start = (exec_date_start_month - relativedelta(months=window_month))

    # Convertir las fechas a string en formato "YYYY-MM-DD"
    date_start_str = date_start.strftime("%Y-%m-%d")
    exec_date_str = exec_date.strftime("%Y-%m-%d")

    # Crear la cadena de filtro
    filter_string = f"AND {name_column} BETWEEN '{date_start_str}' AND '{exec_date_str}'"

    return filter_string
