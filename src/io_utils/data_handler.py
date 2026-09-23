"""Operações de entrada e saída de dados da aplicação."""

import logging

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
        return (
            self.spark.read.option("compression", compression)
            .option("header", header)
            .option("sep", sep)
            .schema(self._get_schema_pedidos())
            .csv(path)
        )

    @staticmethod
    def write_parquet(df: DataFrame, path: str) -> None:
        """Grava um DataFrame em Parquet, sobrescrevendo o destino."""
        df.write.mode("overwrite").parquet(path)
        logger.info("Dados salvos com sucesso em: %s", path)
