"""Fábrica de sessões Spark para a aplicação."""

from pyspark.sql import SparkSession


class SparkSessionManager:
    """Gerencia a criação e o acesso à sessão Spark."""

    @staticmethod
    def get_spark_session(app_name: str = "alun-data-eng-pyspark-app") -> SparkSession:
        """Cria ou reutiliza uma sessão Spark local configurada para a aplicação."""
        return SparkSession.builder.appName(app_name).master("local[*]").getOrCreate()
