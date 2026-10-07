"""Carregamento das configurações centralizadas da aplicação."""

import logging
import logging.config
from pathlib import Path
from typing import Any, Dict, Optional

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[3]


def carregar_config(path: Optional[str] = None) -> Dict[str, Any]:
    """Carrega o arquivo de configuração YAML da aplicação.

    Ordem de resolução:
    1. Caminho explícito fornecido por argumento
    2. 'settings.yaml' na raiz de execução (quando distribuído via spark-submit --files)
    3. 'config/settings.yaml' (desenvolvimento local na raiz do projeto)
    """
    if path:
        caminho = Path(path)
    elif Path("settings.yaml").is_file():
        caminho = Path("settings.yaml")
    else:
        caminho = Path("config/settings.yaml")

    with open(caminho, "r", encoding="utf-8") as arquivo:
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
