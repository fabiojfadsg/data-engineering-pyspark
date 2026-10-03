"""Operações de entrada e saída de dados da aplicação."""

import logging

from pyspark.errors import AnalysisException, PySparkException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    ArrayType,
    DateType,
    FloatType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from data_engineering_pyspark.io_utils.exceptions import LoadPedidosException

logger = logging.getLogger(__name__)


class DataHandler:
    """Lê as fontes do pipeline e grava seu resultado final."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    @staticmethod
    def _get_schema_clientes() -> StructType:
        """Retorna o schema explícito da fonte de clientes."""
        return StructType(
            [
                StructField("id", LongType(), True),
                StructField("nome", StringType(), True),
                StructField("data_nasc", DateType(), True),
                StructField("cpf", StringType(), True),
                StructField("email", StringType(), True),
                StructField("interesses", ArrayType(StringType()), True),
            ]
        )

    @staticmethod
    def _get_schema_pedidos() -> StructType:
        """Retorna o schema explícito da fonte de pedidos."""
        return StructType(
            [
                StructField("id_pedido", StringType(), True),
                StructField("produto", StringType(), True),
                StructField("valor_unitario", FloatType(), True),
                StructField("quantidade", LongType(), True),
                StructField("data_criacao", TimestampType(), True),
                StructField("uf", StringType(), True),
                StructField("id_cliente", LongType(), True),
            ]
        )

    def load_clientes(self, path: str) -> DataFrame:
        """Carrega clientes JSON comprimidos com schema explícito."""
        return (
            self.spark.read.option("compression", "gzip")
            .schema(self._get_schema_clientes())
            .json(path)
        )

    def load_pedidos(
        self, path: str, compression: str, header: bool, sep: str
    ) -> DataFrame:
        """Carrega pedidos CSV com schema explícito."""
        try:
            pedidos_df = (
                self.spark.read.option("compression", compression)
                .option("header", header)
                .option("sep", sep)
                .schema(self._get_schema_pedidos())
                .csv(path)
            )

            if pedidos_df.isEmpty():
                logger.warning(
                    "O arquivo de pedidos em '%s' foi lido, mas não contém registros.",
                    path,
                )
            return pedidos_df
        except AnalysisException as error:
            logger.error(
                "Erro de análise/metadados no Spark [Classe: %s]: %s",
                error.getErrorClass(),
                error,
            )
            raise LoadPedidosException(
                f"Falha ao carregar pedidos a partir de '{path}'"
            ) from error
        except PySparkException as error:
            logger.error(
                "Erro de processamento no PySpark [Classe: %s | SQLSTATE: %s]: %s",
                error.getErrorClass(),
                error.getSqlState(),
                error,
            )
            raise LoadPedidosException(
                f"Erro no motor Spark ao carregar pedidos em '{path}'"
            ) from error

    @staticmethod
    def write_parquet(df: DataFrame, path: str) -> None:
        """Grava um DataFrame em Parquet, sobrescrevendo o destino."""
        df.write.mode("overwrite").parquet(path)
        logger.info("Dados salvos com sucesso em: %s", path)
