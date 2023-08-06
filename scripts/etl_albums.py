from datetime import datetime
from commons import EtlSpark
from os import environ as env
import requests
from pyspark.sql.functions import concat, col, lit, when, expr, to_date


class EtlAlbums(EtlSpark):
    def __init__(self, job_name=None):
        super().__init__(job_name)
        self.process_date = datetime.now().strftime("%Y-%m-%d")

    def run(self):
        process_date = datetime.now().strftime("%Y-%m-%d")
        self.execute(process_date)

    def extract(self):
        """
        Extrae datos de la API
        """
        print(">>> [E] Extrayendo datos de la API...")

        url = "https://jsonplaceholder.typicode.com/albums"
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
        else:
            print("Error al extraer datos de la API")
            raise Exception("Error al extraer datos de la API")

        df = self.spark.read.json(
            self.spark.sparkContext.parallelize(data), multiLine=True
        )
        df.printSchema()
        df.show()

        return df

    def transform(self, df_original):
        """
        Transforma los datos
        """
        print(">>> [T] Transformando datos...")

        df = df_original.spark_df.distinct().na.drop()

        df = df.drop('id', axis='columns', inplace=True)

        df.printSchema()
        df.show()

        return df

    def load(self, df_final):
        """
        Carga los datos transformados en Redshift
        """
        print(">>> [L] Cargando datos en Redshift...")

        # add process_date column
        df_final = df_final.withColumn("process_date", lit(self.process_date))

        df_final.write \
            .format("jdbc") \
            .option("url", env['AWS_REDSHIFT_HOST']) \
            .option("dbtable", f"{env['AWS_REDSHIFT_SCHEMA']}.albums") \
            .option("user", env['AWS_REDSHIFT_USER']) \
            .option("password", env['AWS_REDSHIFT_PASSWORD']) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

        print(">>> [L] Datos cargados exitosamente")
