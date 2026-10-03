"""Carregamento das configurações centralizadas da aplicação."""

import logging
import logging.config
from pathlib import Path
from typing import Any, Dict, Optional, Union

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_SETTINGS_PATH = PROJECT_ROOT / "config" / "settings.yaml"


def carregar_config(path: Optional[Union[str, Path]] = None) -> Dict[str, Any]:
    """Carrega configuração explícita, distribuída pelo Spark ou local.

    A prioridade é: caminho informado, ``settings.yaml`` no diretório de
    execução (entregue por ``spark-submit --files``) e, por fim, o arquivo
    local do projeto.
    """
    if path is not None:
        config_path = Path(path)
    elif Path("settings.yaml").is_file():
        config_path = Path("settings.yaml")
    else:
        config_path = DEFAULT_SETTINGS_PATH

    with config_path.open("r", encoding="utf-8") as arquivo:
        config = yaml.safe_load(arquivo)

    if not isinstance(config, dict):
        raise ValueError("O arquivo de configuração deve conter um objeto YAML.")
    return config


def configurar_logging(config_logging: Dict[str, Any]) -> None:
    """Aplica a configuração de logging carregada do arquivo YAML."""
    for handler in config_logging.get("handlers", {}).values():
        filename = handler.get("filename")
        if filename:
            Path(filename).parent.mkdir(parents=True, exist_ok=True)
    logging.config.dictConfig(config_logging)
    logging.getLogger(__name__).info("Logging configurado com sucesso via YAML.")
