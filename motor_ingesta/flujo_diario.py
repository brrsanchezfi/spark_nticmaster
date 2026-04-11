import json
import os
from datetime import timedelta
# from loguru import logger

class FlujoDiario:

    def __init__(self, config_file: str):
        """
        Inicializa el flujo diario cargando la configuración y creando la SparkSession.

        Lee el fichero JSON indicado en config_file y lo almacena en self.config.
        Según el valor de EXECUTION_ENVIRONMENT, crea una sesión remota en Databricks
        (usando DatabricksSession y el CLUSTER_ID definido en la config) o una
        SparkSession local con el nombre de app 'FlujoDiario'.

        :param config_file: Ruta al fichero JSON de configuración. Debe contener:
                            - EXECUTION_ENVIRONMENT: 'databricks' para conectar al cluster
                              remoto, cualquier otro valor (o ausente) para sesión local.
                            - CLUSTER_ID: ID del cluster Databricks (solo necesario si
                              EXECUTION_ENVIRONMENT es 'databricks').
        """

        with open(config_file) as f:
            self.config = json.load(f)
            env = self.config.get("EXECUTION_ENVIRONMENT", "local")
            print(f"Enviroment: {env}")
            if env == "databricks":
                # Conecta remotamente al cluster Databricks
                from databricks.connect import DatabricksSession
                cluster_id = self.config.get("CLUSTER_ID", "")
                self.spark = (DatabricksSession.builder
                              .clusterId(cluster_id)
                              .getOrCreate())
            else:
                # SparkSession local (producción en cluster o tests indirectos)
                from pyspark.sql import SparkSession
                self.spark = (SparkSession.builder
                              .appName("FlujoDiario")
                              .getOrCreate())


    # def procesa_diario(self, data_file: str):
    #     """
    #     Completa la documentación
    #     :param data_file:
    #     :return:
    #     """
    #
    #     # raise NotImplementedError("completa el código de esta función")   # borra esta línea cuando resuelvas
    #     try:
    #         # Procesamiento diario: crea un nuevo objeto motor de ingesta con self.config, invoca a ingesta_fichero,
    #         # después a las funciones que añaden columnas adicionales, y finalmente guarda el DF en la tabla indicada en
    #         # self.config["output_table"], que debe crearse como tabla manejada (gestionada), sin usar ningún path,
    #         # siempre particionando por FlightDate. Tendrás que usar .write.option("path", ...).saveAsTable(...) para
    #         # indicar que queremos crear una tabla externa en el momento de guardar.
    #         # Conviene cachear el DF flights_df así como utilizar el número de particiones indicado en
    #         # config["output_partitions"]
    #
    #         motor_ingesta = ...
    #         flights_df = ...
    #
    #         # Paso 1. Invocamos al método para añadir la hora de salida UTC
    #         flights_with_utc = ...                # reemplaza por la llamada adecuada
    #
    #
    #         # -----------------------------
    #         #  CÓDIGO PARA EL EJERCICIO 4
    #         # -----------------------------
    #         # Paso 2. Para resolver el ejercicio 4 que arregla el intervalo faltante entre días,
    #         # hay que leer de la tabla self.config["output_table"] la partición del día previo si existiera. Podemos
    #         # obviar este código hasta llegar al ejercicio 4 del notebook
    #         dia_actual = flights_df.first().FlightDate
    #         dia_previo = dia_actual - timedelta(days=1)
    #         try:
    #             flights_previo = spark.read.table(...).where(F.col(...) == ...)
    #             logger.info(f"Leída partición del día {dia_previo} con éxito")
    #         except Exception as e:
    #             logger.info(f"No se han podido leer datos del día {dia_previo}: {str(e)}")
    #             flights_previo = None
    #
    #         if flights_previo:
    #             # añadir columnas a F.lit(None) haciendo cast al tipo adecuado de cada una, y unirlo con flights_previo.
    #             # OJO: hacer select(flights_previo.columns) para tenerlas en el mismo orden antes de
    #             # la unión, ya que la columna de partición se había ido al final al escribir
    #
    #             df_unido = ...
    #             # Spark no permite escribir en la misma tabla de la que estamos leyendo. Por eso salvamos
    #             df_unido.write.mode("overwrite").saveAsTable("tabla_provisional")
    #             df_unido = self.spark.read.table("tabla_provisional")
    #
    #         else:
    #             df_unido = flights_with_utc           # lo dejamos como está
    #
    #         # Paso 3. Invocamos al método para añadir información del vuelo siguiente
    #         df_with_next_flight = ...
    #
    #         # Paso 4. Escribimos el DF en la tabla externa config["output_table"] con ubicación config["output_path"], con
    #         # el número de particiones indicado en config["output_partitions"]
    #         # df_with_next_flight.....(...)..write.mode("overwrite").option("partitionOverwriteMode", "dynamic")....
    #         df_with_next_flight\
    #             .coalesce(...)\
    #             .write...
    #
    #
    #         # Borrar la tabla provisional si la hubiéramos creado
    #         self.spark.sql("DROP TABLE IF EXISTS tabla_provisional")
    #
    #     except Exception as e:
    #         logger.error(f"No se pudo escribir la tabla del fichero {data_file}")
    #         raise e
    #

if __name__ == '__main__':

    flujo = FlujoDiario("../config/config.json")
    print("Spark version:", flujo.spark.version)

    data = [("Madrid", 1), ("Barcelona", 2), ("Valencia", 3)]
    df = flujo.spark.createDataFrame(data, ["ciudad", "id"])
    df.show()
    print("Total filas:", df.count())

    df.createOrReplaceTempView("ciudades")
    flujo.spark.sql(
        "SELECT ciudad, id*10 AS valor FROM ciudades WHERE id > 1"
    ).show()
    # Recuerda que puedes crear el wheel ejecutando en la línea de comandos: python setup.py bdist_wheel
