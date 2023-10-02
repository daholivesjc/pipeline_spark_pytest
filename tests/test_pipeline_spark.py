from src.pipeline import PipelineSpark
import pytest
import os

# Spark init conf
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/opt/spark"
import findspark
findspark.init()
from pyspark.sql import DataFrame

class TestPipelineSpark:

    @pytest.fixture(scope='class')
    def get_class_instance(self):

        schema_entrada = ["ID", "Nome", "Idade"]

        dados_entrada = [
            (1, "Alice", 25),
            (2, "Bob", 30),
            (3, "Charlie", 35)
        ]

        pipeline_test = PipelineSpark(
            data = dados_entrada,
            schema = schema_entrada
        )

        return pipeline_test

    def test_valida_campos_dataframe_raw(self, get_class_instance):

        resultado = 3

        dataframe_raw_test = get_class_instance.raw()

        assert resultado == len(dataframe_raw_test.columns)

    def test_valida_unico_registro_dataframe_trusted(self, get_class_instance):

        resultado_1 = 1
        resultado_2 = DataFrame

        dataframe_raw_test = get_class_instance.raw()

        df_trusted = get_class_instance.trusted(dataframe_raw_test)

        assert resultado_1 == df_trusted.count()
        assert resultado_2 == type(dataframe_raw_test)

    def test_valida_run_execucao_true(self, get_class_instance):

        resultado = True

        is_run = get_class_instance.run()

        assert resultado == is_run

    def test_valida_exception_run(self, get_class_instance):

        with pytest.raises(Exception):

            resultado = get_class_instance.trusted(1)

            assert resultado