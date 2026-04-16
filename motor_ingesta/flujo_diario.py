import json
import os
from datetime import timedelta
from motor_ingesta.motor_ingesta import MotorIngesta
from motor_ingesta.agregaciones import aniade_hora_utc, aniade_intervalos_por_aeropuerto
from loguru import logger

from pyspark.sql import functions as F

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
                              .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
                              .enableHiveSupport()
                              .getOrCreate())


    def procesa_diario(self, data_file: str):
        """
        Ejecuta el flujo de procesamiento diario para un fichero de vuelos.

        1. Crea un MotorIngesta con la configuración actual e ingesta el fichero JSON.
        2. Añade la hora de salida en UTC mediante add_utc_departure_time.
        3. (Ejercicio 4) Lee la partición del día previo si existe y la une al DF actual
        para resolver el intervalo faltante entre días.
        4. Añade información del vuelo siguiente de cada avión.
        5. Escribe el resultado en la tabla externa indicada en config["output_table"],
        particionando dinámicamente por FlightDate.

        :param data_file: Ruta al fichero JSON con los datos de vuelos del día.
        """
        try:
            # ── Ingesta ──────────────────────────────────────────────────────────
            motor_ingesta = MotorIngesta(self.config, self.spark)
            flights_df = motor_ingesta.ingesta_fichero(data_file).cache()
            flights_df = flights_df.repartition(self.config["output_partitions"])

            # ── Paso 1: hora de salida UTC ────────────────────────────────────────
            flights_with_utc = aniade_hora_utc(self.spark ,flights_df)

            # ── Crear tabla si no existe (solo si ya hay datos) ──────────────────
            logger.info(f"Verificando existencia de tabla {self.config['output_table']}...")
            try:
                self.spark.sql(f"""
                    CREATE TABLE IF NOT EXISTS {self.config["output_table"]}
                    USING parquet
                """)
                logger.info(f"Tabla {self.config['output_table']} lista")
            except Exception as e:
                logger.info(f"Tabla aún no existe, será creada en la escritura: {str(e)}")

            # ─────────────────────────────────────────────────────────────────────
            #  CÓDIGO PARA EL EJERCICIO 4
            # ─────────────────────────────────────────────────────────────────────
            tabla = self.config["output_table"]

            dia_actual = flights_df.first().FlightDate
            dia_previo = dia_actual - timedelta(days=1)

            # Verificar si la tabla existe
            if self.spark.catalog.tableExists(tabla):
                try:
                    flights_previo = (
                        self.spark.read
                            .table(tabla)
                            .where(F.col("FlightDate") == dia_previo)
                    )

                    # Opcional: comprobar si realmente hay datos
                    if flights_previo.limit(1).count() == 0:
                        logger.info(f"No hay datos para el día {dia_previo}")
                        flights_previo = None
                    else:
                        logger.info(f"Leída partición del día {dia_previo} con éxito")

                except Exception as e:
                    logger.warning(f"Error leyendo datos del día previo: {str(e)}")
                    flights_previo = None
            else:
                logger.info(f"La tabla {tabla} no existe todavía (primer día de ejecución)")
                flights_previo = None


            # Uso correcto del DataFrame
            if flights_previo is not None:
                columnas_previo = flights_previo.columns
                columnas_utc    = flights_with_utc.columns

                flights_with_utc_aligned = flights_with_utc
                for col_name in columnas_previo:
                    if col_name not in columnas_utc:
                        dtype = dict(flights_previo.dtypes)[col_name]
                        flights_with_utc_aligned = flights_with_utc_aligned.withColumn(
                            col_name, F.lit(None).cast(dtype)
                        )

                df_unido = flights_previo.union(
                    flights_with_utc_aligned.select(columnas_previo)
                )

                # Evitar conflicto lectura/escritura
                df_unido.write.mode("overwrite").saveAsTable("tabla_provisional")
                df_unido = self.spark.read.table("tabla_provisional")

            else:
                df_unido = flights_with_utc

            # ── Paso 3: vuelo siguiente ───────────────────────────────────────────
            df_with_next_flight = aniade_intervalos_por_aeropuerto(df_unido)

            # ── Paso 4: escritura en tabla externa ────────────────────────────────

            # Escritura
            logger.info(f"Escribiendo datos en {self.config['output_table']}...")
            (df_with_next_flight
                .coalesce(self.config["output_partitions"])
                .write
                .mode("overwrite")
                .option("partitionOverwriteMode", "dynamic")
                # .option("path", self.config["output_path"])
                .partitionBy("FlightDate")
                .saveAsTable(self.config["output_table"]))
            logger.info(f"Datos escritos correctamente en {self.config['output_table']}")

            try:
                self.spark.sql("DROP TABLE IF EXISTS tabla_provisional")
            except Exception as e:
                logger.warning(f"No se pudo borrar tabla_provisional: {str(e)}")

            # df_with_next_flight.show()

        except Exception as e:
            logger.error(f"No se pudo escribir la tabla del fichero {data_file}")
            raise e
        
# if __name__ == '__main__':

#     path_config_flujo_diario = "../config/config.json"

#     path_json_primer_dia = "../data/flights_json/landing/2023-01-01.json"
#     flujo_diario = FlujoDiario(path_config_flujo_diario)

#     flujo_diario.procesa_diario(path_json_primer_dia)

#     flights_df = flujo_diario.spark.read.parquet(flujo_diario.config["output_path"])
#     flights_df.show(5)