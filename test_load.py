import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import DataFrame
from src.product_logs_consultas_agentes.extra.utils.load import read_yaml, load_data, hudi_load_data


# Test para read_yaml
@patch("src.product_logs_consultas_agentes.extra.utils.load.path")
@patch("src.product_logs_consultas_agentes.extra.utils.load.open")
@patch("src.product_logs_consultas_agentes.extra.utils.load.yaml.load")
def test_read_yaml(mock_yaml_load, mock_open, mock_path):
    # Configurar mocks
    mock_path.return_value.__enter__.return_value = "test_path"
    mock_yaml_load.return_value = {"columns": [{"name": "col1", "description": "desc1"}]}

    result = read_yaml("test_file")

    # Verificar que se llamó correctamente y el resultado es el esperado
    mock_open.assert_called_once_with("test_path", "r")
    mock_yaml_load.assert_called_once()
    assert result == [{"name": "col1", "description": "desc1"}]


# Test para load_data con parámetros faltantes
def test_load_data_missing_params():
    with pytest.raises(ValueError, match="YAML file is required"):
        load_data(data=MagicMock(), table_type="hudi")

    with pytest.raises(ValueError, match="table_type must be hive, iceberg or hudi"):
        load_data(data=MagicMock(), yaml_name="test")

    with pytest.raises(ValueError, match="dataframe is required"):
        load_data(yaml_name="test", table_type="hudi")


# Test para load_data con parámetros inválidos
def test_load_data_invalid_params():
    with pytest.raises(ValueError, match="Invalid parameters for method"):
        load_data(data=MagicMock(), yaml_name="test", table_type="hudi", invalid_param="test")


# Test para load_data con flujo correcto
@patch("src.product_logs_consultas_agentes.extra.utils.load.read_yaml")
@patch("src.product_logs_consultas_agentes.extra.utils.load.hudi_load_data")
def test_load_data_success(mock_hudi_load_data, mock_read_yaml):
    # Configurar mocks
    mock_read_yaml.return_value = [{"name": "col1", "description": "desc1"}]
    mock_df = MagicMock()
    mock_df.columns = ["col1"]

    # Llamar a la función
    load_data(data=mock_df, yaml_name="test", table_type="hudi", mode="overwrite", options={}, target_path="/path")

    # Verificar que read_yaml fue llamado
    mock_read_yaml.assert_called_once_with("test")

    # Verificar que los metadatos fueron añadidos
    mock_df.withMetadata.assert_called_once_with("col1", {"comment": "desc1"})

    # Verificar que hudi_load_data fue llamado
    mock_hudi_load_data.assert_called_once()


# Test para hudi_load_data con escritura exitosa
@patch("src.product_logs_consultas_agentes.extra.utils.load.logger")
def test_hudi_load_data_success(mock_logger):
    # Mock de DataFrame
    mock_df = MagicMock()
    mock_options = {
        "hoodie.datasource.hive_sync.database": "test_db",
        "hoodie.table.name": "test_table"
    }

    # Llamar a la función
    hudi_load_data(mock_df, options=mock_options, mode="overwrite", target_path="/path")

    # Verificar que write fue llamado
    mock_df.write.format.assert_called_once_with("hudi")
    mock_df.write.format().options.assert_called_once_with(**mock_options)
    mock_df.write.format().options().mode.assert_called_once_with("overwrite")
    mock_df.write.format().options().mode().save.assert_called_once_with("/path")

    # Verificar el log
    mock_logger.info.assert_called_once_with(
        "Guardado exitoso de la tabla de hudi en DB: test_db, table: test_table"
    )


# Test para hudi_load_data con error de escritura
@patch("src.product_logs_consultas_agentes.extra.utils.load.logger")
def test_hudi_load_data_failure(mock_logger):
    # Mock de DataFrame
    mock_df = MagicMock()
    mock_df.write.format().options().mode().save.side_effect = Exception("Write error")

    with pytest.raises(RuntimeError):
        hudi_load_data(mock_df, options={}, mode="overwrite", target_path="/path")

    # Verificar el log del error
    mock_logger.error.assert_called_once()
