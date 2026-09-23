"""Transformações de negócio da análise de pedidos."""

from pyspark.sql import DataFrame, functions as F


class Transformation:
    """Contém regras de negócio que transformam DataFrames."""

    @staticmethod
    def add_valor_total_pedidos(pedidos_df: DataFrame) -> DataFrame:
        """Adiciona ``valor_total`` como valor unitário multiplicado pela quantidade."""
        return pedidos_df.withColumn(
            "valor_total", F.col("valor_unitario") * F.col("quantidade")
        )

    @staticmethod
    def get_top_10_clientes(pedidos_df: DataFrame) -> DataFrame:
        """Agrupa pedidos por cliente e retorna os dez maiores totais."""
        return (
            pedidos_df.groupBy("id_cliente")
            .agg(F.sum("valor_total").alias("valor_total"))
            .orderBy(F.desc("valor_total"))
            .limit(10)
        )

    @staticmethod
    def join_pedidos_clientes(
        pedidos_df: DataFrame, clientes_df: DataFrame
    ) -> DataFrame:
        """Une os totais de pedidos aos dados identificadores dos clientes."""
        return pedidos_df.join(
            clientes_df, clientes_df.id == pedidos_df.id_cliente, "inner"
        ).select(
            pedidos_df.id_cliente,
            clientes_df.nome,
            clientes_df.email,
            pedidos_df.valor_total,
        )
