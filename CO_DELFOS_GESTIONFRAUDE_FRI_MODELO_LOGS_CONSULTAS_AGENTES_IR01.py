"""
    DESC: Este DAG esta diseñado para la ejecución de los logs de las consultas a la vista 360
    ejecución del producto de datos denominado 'co_zendesk_logs_consultas_agentes'
    OWNER: Gestion Fraude
    DAG_ID: CO_DELFOS_GESTIONFRAUDE_FRI_MODELO_LOGS_CONSULTAS_AGENTES_IR01
"""
import sys
import logging
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.models.param import Param
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.hooks.glue_catalog import GlueCatalogHook
from airflow.providers.opsgenie.operators.opsgenie import OpsgenieCreateAlertOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
# Variables DAG
DAG_ID                          = "CO_DELFOS_GESTIONFRAUDE_FRI_MODELO_LOGS_CONSULTAS_AGENTES_IR01"
DAG_DESCRIPTION                 = "Este dag ejecuta el modelo de GL de la info de logs de la vista 360 " + \
                                    "denominado 'logs_consultas_agentes' de dominio Gestion Fraude"
DAG_SCHEDULE                    = f"15 * * * *"  # Se ejecuta en el minuto 15 de cada hora
# Variables por dominio de datos
ENV                             = Variable.get("env")
OWNER                           = "gestionfraude"
DOMAIN                          = "gestionfraude"
COUNTRY                         = "co"
SUBDOMAIN                       = "fri"
OPSGENIE_CONNECTION_ID          = "opsgenie-dataops"
AWS_CONNECTION_TRANSVERSAL_GL   = "co-gestionfraude-transversal-gl-aws"
if ENV == "dev":
    ACCOUNT_NUMBER              = '255646567618'
    ENV_MESH                    = "dev"
elif ENV == "qa":
    ACCOUNT_NUMBER              = '315735002216'
    ENV_MESH                    = "qc"
elif ENV == "pdn":
    ACCOUNT_NUMBER              = '044879804046'
    ENV_MESH                    = "pdn"
TAGS = [
                                f"env.{ENV.upper()}",
                                "teams.DELFOS",
                                f"dominio.{DOMAIN.upper()}",
                                f"subdominio.{SUBDOMAIN.upper()}",
                                f"pais.{COUNTRY.upper()}",
                                "criticidad.P3",
]
SLACK_CHANNEL                   = f"airflow-data-notifications-{ENV}"
DEFAULT_ARGS                    = {
                                    "owner": "GESTIONFRAUDE",
                                    "start_date": datetime(2024, 12, 18)
                                }

INTERVAL_TIMESTAMP              = "(data_interval_start.in_timezone('America/Bogota'))"
INTERVAL_TIMESTAMP              = f"{{{{ {INTERVAL_TIMESTAMP}.strftime('%Y-%m-%d %H:%M:%S') }}}}"
INTERVAL_TIMESTAMP_END          = "(data_interval_end.in_timezone('America/Bogota'))"
EXECUTION_TIMESTAMP             = f"{{{{ {INTERVAL_TIMESTAMP_END}.strftime('%Y-%m-%d %H:%M:%S') }}}}"
YEAR                            = f"{{{{ {INTERVAL_TIMESTAMP}.strftime('%Y') }}}}"
MONTH                           = f"{{{{ {INTERVAL_TIMESTAMP}.strftime('%m') }}}}"
DAY                             = f"{{{{ {INTERVAL_TIMESTAMP}.strftime('%d') }}}}"
HOUR                            = f"{{{{ {INTERVAL_TIMESTAMP}.strftime('%h') }}}}"

# Variables producto de datos
GLUE_JOB_CURADO                 = "co_delfos_gestionfraude_fri_curado_logs_consultas_agentes"
DATA_SOURCES = [
                                {
                                    "databasename": f"co_delfos_gestionfraude_raw_{ENV_MESH}_rl",
                                    "tablename": "co_vista360_logs_consulta_usuarios",
                                    "expression": f"""
                                        year='{{{{ {INTERVAL_TIMESTAMP}.strftime('%Y') }}}}'
                                        and month='{{{{ {INTERVAL_TIMESTAMP}.strftime('%m') }}}}'
                                        and day='{{{{ {INTERVAL_TIMESTAMP}.strftime('%d') }}}}'
                                        and hour='{{{{ {INTERVAL_TIMESTAMP}.strftime('%h') }}}}'
                                    """,
                                },
]
task_logger                     = logging.getLogger("airflow.task")
# =====================================================================================
#  Funciones asociadas a notificaciones de errores
# =====================================================================================
def send_error_message_on_slack(context):
    """Send a failure message over slack
    Send a custom message to a slack channel base
    on a context variable.
    """
    error_message: str = f"""
        Ha ocurrio un error procesando producto logs_consultas_agentes:
            dag: {context.get("task_instance").dag_id}
            tarea: {context.get("task_instance").task_id}
            url del log: {context.get('task_instance').log_url}
    """
    error_message_on_slack = SlackWebhookOperator(
        task_id="error_message_on_slack",
        slack_webhook_conn_id=f"slack-{SLACK_CHANNEL}",
        message=error_message,
    )
    return error_message_on_slack.execute(context=context)

def send_error_message_on_opsgenie(context):
    """Send a failure message over opsgenie
    Send a custom message over opsenie base on a
    context variable.
    """
    error_header: str = (
        f"""Error de ejecución en el DAG {context.get("task_instance").dag_id}"""
    )
    error_message: str = f"""
        Ha ocurrio un error:
            dag: {context.get("task_instance").dag_id}
            tarea: {context.get("task_instance").task_id}
            url del log: {context.get('task_instance').log_url}
    """
    error_message_on_opsgenie = OpsgenieCreateAlertOperator(
        opsgenie_conn_id=OPSGENIE_CONNECTION_ID,
        task_id="error_message_on_opsgenie",
        message=error_header,
        description=error_message,
    )
    return error_message_on_opsgenie.execute(context=context)
def orchestrate_error_message(context):
    """Orchestrates the sending of error messages
    Handle the error messages over slack and Opsgenie.
    """
    send_error_message_on_slack(context)
    send_error_message_on_opsgenie(context)
# =====================================================================================
#  Funciones asociadas a validar las actualizacion de las fuentes
# =====================================================================================
def get_validation_operator(
        task_id: str,
        database: str,
        tablename: str,
) -> object:
    """Create a Operator that validates the partition of a Data Source.
    Args:
        task_id (str): ID of the task.
        database (str): Name of the Database of the Data Source.
        tablename (str): Name of the table of the Data Source.
    Returns:
        (@task): Airflow task that uses the GlueCatalogHook.
    """
    @task(task_id=task_id)
    def validation_operator(expression: str):
        """Creates a Airflow Task using the GlueCatalogHook.
        Args:
            expression (str): Expression to filter the partition of a table.
        """
        hook = GlueCatalogHook(aws_conn_id=AWS_CONNECTION_TRANSVERSAL_GL)
        validation = hook.check_for_partition(database, tablename, expression)
        if not validation:
            error_message = f"The Data Source {database}.{tablename} is NOT up to date.With expression: {expression}"
            task_logger.error(error_message)
            # Lanza una excepción para que la tarea falle
            raise ValueError(error_message)
        task_logger.info(f"The Data Source {database}.{tablename} is up to date. With expression: {expression}")
    return validation_operator
def get_data_sources_validator_task_group(data_sources: list) -> TaskGroup:
    """Creates an Airflow Task Group that validates the partition of a group of data sources.
    Args:
        data_sources (list): List with all the information about the Data Sources.
    Returns:
        (TaskGroup): Task Group to validate the Data Sources.
    """
    with TaskGroup("data_source_validation") as data_source_validation_tasks:
        _ = [
            get_validation_operator(
                task_id=f"validate_{x['databasename']}_{x['tablename']}",
                database=x["databasename"],
                tablename=x["tablename"],
            )(expression=x["expression"])
            for x in data_sources
        ]
    return data_source_validation_tasks

@dag(
    dag_id=DAG_ID,
    description=DAG_DESCRIPTION,
    default_args=DEFAULT_ARGS,
    schedule_interval=DAG_SCHEDULE,
    catchup=False,
    tags=TAGS,
    on_failure_callback=orchestrate_error_message,
    params={"reprocess": True},
)
def workflow():
    """Executes the workflow for the
        coDelfosGestionFraude_friLogsConsultasAgentes DAG.
    This function defines the tasks and their dependencies for the DAG workflow.
    It consists of two TaskGroups:
    - data_source_validation: Contains tasks related to data source validation.
    - processing_tasks: Contains tasks related to data ingestion and processing.
    Task Dependencies:
    - The data_source_validation are executed sequentially, with each task depending on
    the successful completion of the previous task.
    - The ingestion_tasks are executed sequentially, with each task depending on
    the successful completion of the previous task.
    """
    task_start = EmptyOperator(task_id="start_process")
    validate_data_sources = get_data_sources_validator_task_group(
        data_sources=DATA_SOURCES
    )
    with TaskGroup('processing_tasks') as processing_tasks:
        EJECUTAR_GL_LOGS_CONSULTAS_AGENTES = GlueJobOperator(
            task_id='execute_gl_logs_consulta_agentes',
            job_name=GLUE_JOB_CURADO,
            wait_for_completion=False,
            aws_conn_id=AWS_CONNECTION_TRANSVERSAL_GL,
            script_args={'--execution_timestamp': EXECUTION_TIMESTAMP,
                         '--env': ENV_MESH,
                         '--JOB_NAME': GLUE_JOB_CURADO}
        )
        DETECTAR_GL_LOGS_CONSULTAS_AGENTES = GlueJobSensor(
            task_id="detect_gl_logs_consulta_agentes",
            aws_conn_id=AWS_CONNECTION_TRANSVERSAL_GL,
            job_name=GLUE_JOB_CURADO,
            run_id=EJECUTAR_GL_LOGS_CONSULTAS_AGENTES.output,
            timeout=15 * 60,
            mode='reschedule',
            poke_interval=5 * 60
        )
        EJECUTAR_GL_LOGS_CONSULTAS_AGENTES >> DETECTAR_GL_LOGS_CONSULTAS_AGENTES

    task_end = EmptyOperator(task_id="end_process")
    task_start >> validate_data_sources >> processing_tasks >> task_end

workflow()