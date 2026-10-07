from pyspark.sql import SparkSession

from data_engineering_pyspark.session.spark_session import SparkSessionManager


class TestSparkSessionManager:

    def test_retorna_spark_session(self, spark):
        assert isinstance(SparkSessionManager.get_spark_session(app_name="t"), SparkSession)

    def test_reutiliza_a_mesma_sessao(self, spark):
        """Chamadas repetidas devolvem a MESMA instância (Singleton via getOrCreate)."""
        a = SparkSessionManager.get_spark_session(app_name="a")
        b = SparkSessionManager.get_spark_session(app_name="b")
        assert a is b
