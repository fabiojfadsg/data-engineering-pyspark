"""Orquestra a execução do pipeline de análise de pedidos."""

import logging
from typing import Any, Mapping

from data_engineering_pyspark.io_utils.data_handler import DataHandler
from data_engineering_pyspark.processing.transformations import Transformation

logger = logging.getLogger(__name__)


class Pipeline:
    """Executa o pipeline usando dependências recebidas no construtor."""

    def __init__(self, data_handler: DataHandler, transformer: Transformation):
        self.data_handler = data_handler
        self.transformer = transformer

    def run(self, config: Mapping[str, Any]) -> None:
        """Carrega, transforma e grava o relatório de pedidos."""
        logger.info("Pipeline iniciado.")

        paths = config["paths"]
        pedidos_options = config["file_options"]["pedidos_csv"]
        path_clientes = paths["clientes"]
        path_pedidos = paths["pedidos"]
        path_output = paths["output"]
        compression_pedidos = pedidos_options["compression"]
        header_pedidos = pedidos_options["header"]
        separator_pedidos = pedidos_options["sep"]

        logger.info("Abrindo o dataframe de clientes")
        logger.info("Obtido o path de clientes: %s", path_clientes)
        clientes_df = self.data_handler.load_clientes(path=path_clientes)
        clientes_df.show(5, truncate=False)

        logger.info("Abrindo o dataframe de pedidos")
        logger.info(
            "Parâmetros de pedidos: path=%s, compression=%s, header=%s, separator=%s",
            path_pedidos,
            compression_pedidos,
            header_pedidos,
            separator_pedidos,
        )
        pedidos_df = self.data_handler.load_pedidos(
            path=path_pedidos,
            compression=compression_pedidos,
            header=header_pedidos,
            sep=separator_pedidos,
        )

        logger.info("Adicionando a coluna valor_total")
        pedidos_df = self.transformer.add_valor_total_pedidos(pedidos_df)
        pedidos_df.show(5, truncate=False)

        logger.info("Calculando os 10 maiores clientes por valor total de pedidos")
        top_10_clientes_df = self.transformer.get_top_10_clientes(pedidos_df)
        top_10_clientes_df.show(10, truncate=False)

        logger.info("Fazendo a junção dos dataframes")
        relatorio_top_10_clientes_df = self.transformer.join_pedidos_clientes(
            top_10_clientes_df, clientes_df
        )
        relatorio_top_10_clientes_df.show(20, truncate=False)

        logger.info("Escrevendo o resultado em Parquet")
        logger.info("Obtido o path de saída: %s", path_output)
        self.data_handler.write_parquet(
            df=relatorio_top_10_clientes_df, path=path_output
        )
        logger.info("Pipeline concluído com sucesso!")
