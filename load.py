"""
This module adds metadata and loads data to Iceberg, Hudi or Hive tables
"""
import sys
import yaml
from importlib.resources import path
from pyspark.sql import DataFrame
from product_logs_consultas_agentes.extra.utils.utils import create_custom_logger

logger = create_custom_logger(__name__)


def read_yaml(yml_title):
    metadata_file = path(package='metadata', resource=f'{yml_title}.yml')

    with metadata_file as path_metadata:
        with open(path_metadata, 'r') as file:
            yml_content = yaml.load(file, Loader=yaml.FullLoader)

    return yml_content['columns']

def load_data(data=None, yaml_name=None, table_type=None, **kwargs):
    table_parameters = {
        'hudi': {'mode', 'options', 'target_path'}
    }
    if yaml_name is None:
        raise ValueError("YAML file is required")

    if table_type is None:
        raise ValueError("table_type must be hive, iceberg or hudi")

    if data is None:
        raise ValueError("dataframe is required")
    if set(kwargs) - table_parameters[table_type]:
        raise ValueError(f"Invalid parameters for method {set(kwargs) - table_parameters[table_type]}")

    yml_columns = read_yaml(yaml_name)
    for column in yml_columns:
        col_name = column['name']
        metadata = {('comment' if key == 'description' else key): value
                    for key, value in column.items() if key != 'name'}

        if col_name in data.columns:
            data = data.withMetadata(col_name, metadata)

    load_mode = {'hudi': hudi_load_data}

    return load_mode[table_type](data, **kwargs)


def hudi_load_data(data, **kwargs):
    """Save the Spark DataFrame to a Hudi table in Glue.
    """
    hudi_options = kwargs.get('options')
    hudi_mode = kwargs.get('mode')
    hudi_target = kwargs.get('target_path')

    try:
        data.write.format("hudi").options(**hudi_options).mode(hudi_mode).save(hudi_target)
        logger.info(f"Guardado exitoso de la tabla de hudi en DB: "
                    f"{hudi_options['hoodie.datasource.hive_sync.database']}, "
                    f"table: {hudi_options['hoodie.table.name']}")
    except Exception as write_error:
        logger.error(f"Error @ {sys._getframe().f_code.co_name}: {write_error}")
        raise RuntimeError from write_error
