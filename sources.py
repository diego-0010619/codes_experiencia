from typing import Dict


def table_sources(env: str) -> dict[str, dict[str, str]]:
    """generate sources from enviroment

    Args:
        env (str): ambiente de despliegue (dev, qc, pdn)

    Returns:
        dict[str,str]: datasources
    """
    sources: Dict[str, Dict[str, str]] = {
        "logs_consultas_agentes": {"databasename": f"co_delfos_gestionfraude_raw_{env}_rl",
                                      "tablename": "co_vista360_logs_consulta_usuarios"},
                                    #   "tablename": "co_logs_consultas_agentes"}, para ambientes diferentes a datalab
    }
    return sources
