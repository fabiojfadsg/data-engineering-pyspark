"""Ponto de entrada da análise de pedidos (Passos 1 a 9)."""

import logging
import sys
from pathlib import Path

from data_engineering_pyspark.config.settings import (
    PROJECT_ROOT,
    carregar_config,
    configurar_logging,
)
from data_engineering_pyspark.io_utils.data_handler import DataHandler
from data_engineering_pyspark.io_utils.exceptions import (
    DataHandlerException,
    LoadPedidosException,
)
from data_engineering_pyspark.pipeline.pipeline import Pipeline
from data_engineering_pyspark.processing.transformations import Transformation
from data_engineering_pyspark.session.spark_session import SparkSessionManager
from pyspark.errors import PySparkException


def _resolve_path(path: str) -> str:
    """Resolve caminhos de configuração relativos à raiz do projeto."""
    configured_path = Path(path)
    return str(
        configured_path
        if configured_path.is_absolute()
        else PROJECT_ROOT / configured_path
    )


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
        logger.info("Pipeline finalizado com sucesso.")
    except LoadPedidosException:
        logger.exception("Falha no carregamento de pedidos.")
        sys.exit(1)
    except DataHandlerException:
        logger.exception("Erro na camada de leitura/escrita de dados.")
        sys.exit(1)
    except PySparkException as error:
        logger.exception(
            "Erro originado no PySpark [Classe: %s | SQLSTATE: %s].",
            error.getErrorClass(),
            error.getSqlState(),
        )
        sys.exit(1)
    except Exception:
        logger.exception("Erro inesperado durante a execução do job.")
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("Sessão Spark finalizada.")


if __name__ == "__main__":
    main()
