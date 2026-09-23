"""Ponto de entrada da análise de pedidos (Passos 1 a 8)."""

import logging
from pathlib import Path

from config.settings import PROJECT_ROOT, carregar_config, configurar_logging
from io_utils.data_handler import DataHandler
from pipeline.pipeline import Pipeline
from processing.transformations import Transformation
from session.spark_session import SparkSessionManager


def _resolve_path(path: str) -> str:
    """Resolve caminhos de configuração relativos à raiz do projeto."""
    configured_path = Path(path)
    return str(configured_path if configured_path.is_absolute() else PROJECT_ROOT / configured_path)


def main() -> None:
    """Monta as dependências concretas e executa o pipeline."""
    config = carregar_config()
    configurar_logging(config["logging"])
    logger = logging.getLogger(__name__)
    app_name = config["spark"]["app_name"]
    logger.info("Iniciando job: %s", app_name)

    # Raiz de composição: monta dependências concretas e as injeta no Pipeline.
    spark = SparkSessionManager.get_spark_session(app_name=app_name)
    try:
        resolved_config = dict(config)
        resolved_config["paths"] = {
            name: _resolve_path(path) for name, path in config["paths"].items()
        }
        pipeline = Pipeline(
            data_handler=DataHandler(spark), transformer=Transformation()
        )
        pipeline.run(config=resolved_config)
    finally:
        spark.stop()
        logger.info("Sessão Spark finalizada.")


if __name__ == "__main__":
    main()
