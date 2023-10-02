import os

class PipelineSpark():

    def __init__(self, data, schema):

        # Definindo a variável de ambiente do Java
        os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"

        # Definindo a variável de ambiente do Spark
        os.environ["SPARK_HOME"] = "/opt/spark"

        # Importando a findspark
        import findspark

        # Iniciando o findspark
        findspark.init()

        from pyspark.sql import SparkSession
        
        # SPARK INSTANCE
        self.spark = SparkSession.builder \
            .master("local[*]") \
            .config("spark.jars.packages", "io.delta:delta-core_2.12:1.1.0") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.executor.memory","4G") \
            .config("spark.driver.memory","4G") \
            .config("spark.executor.cores","12") \
            .getOrCreate()
        
        self.data = data
        self.schema = schema

    def raw(self):

        df = self.spark.createDataFrame(self.data, self.schema)

        return df
    
    def trusted(self, data):

        import pyspark.sql.functions as F

        df = data.orderBy(F.col("ID").desc()).limit(1)

        return df

    def run(self):

        is_running = True

        try:
            # raw
            df_raw = self.raw()

            # trusted
            df_trusted = self.trusted(df_raw)

            # print result
            df_trusted.show()

        except Exception as e:

            is_running = False

            raise Exception(f"Ocorreu um erro {e}!")
        
        return is_running
