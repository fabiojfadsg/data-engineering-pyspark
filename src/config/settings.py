"""Carregamento das configurações centralizadas da aplicação."""

import logging
import logging.config
from pathlib import Path
from typing import Any, Dict, Union

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SETTINGS_PATH = PROJECT_ROOT / "config" / "settings.yaml"


def carregar_config(path: Union[str, Path] = DEFAULT_SETTINGS_PATH) -> Dict[str, Any]:
    """Carrega e retorna as configurações armazenadas em um arquivo YAML."""
    with Path(path).open("r", encoding="utf-8") as arquivo:
        config = yaml.safe_load(arquivo)

    if not isinstance(config, dict):
        raise ValueError("O arquivo de configuração deve conter um objeto YAML.")
    return config


def configurar_logging(config_logging: Dict[str, Any]) -> None:
    """Aplica a configuração de logging carregada do arquivo YAML."""
    logging.config.dictConfig(config_logging)
    logging.getLogger(__name__).info("Logging configurado com sucesso via YAML.")
