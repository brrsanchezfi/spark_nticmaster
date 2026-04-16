# Databricks notebook source
# MAGIC %md
# MAGIC # Un motor de ingesta sencillo para el dataset de vuelos
# MAGIC
# MAGIC ### Recuerda borrar todas las líneas que dicen `raise NotImplementedError`
# MAGIC
# MAGIC El trabajo final consiste en la implementación guiada de un esqueleto de motor de ingesta para ficheros JSON recibidos diariamente con información de los vuelos que han tenido lugar en el día indicado en el nombre del fichero entre dos aeropuertos de los Estados Unidos.
# MAGIC * La estructura de cada JSON puede consultarse abriendo con un editor de texto cualquiera de los ficheros.
# MAGIC * El significado de cada una de las columnas puede consultarse en el fichero config.json que se encuentra en la carpeta config del repositorio.
# MAGIC
# MAGIC Vamos a probar nuestro paquete del motor de ingesta. Antes de ejecutar este notebook, es imprescindible haber completado todo el código del motor de ingesta. 
# MAGIC El notebook solamente valida que el código del motor de ingesta con el cual se ha generado el paquete sea correcto. Para ello, el orden en el que debemos leer, entender y completar los ficheros del repositorio es:
# MAGIC
# MAGIC 1. Clase `MotorIngesta` en el fichero `motor_ingesta.py`. Sólo hay que completar la función `ingesta_fichero`
# MAGIC 2. (Opcional) Completar los tests `test_aplana` y `test_ingesta_fichero` en el fichero `test_ingesta.py`
# MAGIC 3. Clase `FlujoDiario` en el fichero `flujo_diario.py`. Completar primero el inicializador y luego el método `procesa_diario`. Al ir completando el código de este método, veremos que a su vez necesitamos completar las dos funciones siguientes.
# MAGIC 4. Función `aniade_hora_utc` en el fichero `agregaciones.py`
# MAGIC 5. (Opcional) El test de dicha función se llama  `test_aniade_hora_utc` en el fichero `test_ingesta.py`
# MAGIC 6. Función `aniade_intervalos_por_aeropuerto` en el fichero `agregaciones.py`
# MAGIC 7. (Opcional) El test de dicha función se llama `test_aniade_intervalos_por_aeropuerto` en el fichero `test_ingesta.py`

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists default.flights

# COMMAND ----------

!pip install loguru==0.7.1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Instalamos el wheel en el cluster. 

# COMMAND ----------

!pip install --force-reinstall motor_ingesta-0.1.0-py3-none-any.whl

# COMMAND ----------

# MAGIC %restart_python 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Probamos la ingesta de un fichero
# MAGIC
# MAGIC **Ejercicio 1 (2 puntos)**. Ingestar el fichero **`2023-01-01.json`** utilizando el motor de ingesta completo. Debe crearse el objeto de la clase MotorIngesta y utilizar el método `ingesta_fichero`, dejando el resultado en la variable `flights_df`. La variable `flujo_diario` contiene un objeto FlujoDiario inicializado con el path de configuración anterior, y lo vamos a usar ahora solo para leer adecuadamente la configuración y pasársela al objeto `motor_ingesta` como el argumento config.
# MAGIC
# MAGIC * Este ejercicio requiere haber completado previamente el código del paquete de Python y haber generado el fichero .whl, y por tanto, la puntuación del ejercicio se debe a ese trabajo.

# COMMAND ----------

# motor_ingesta/__init__.py
# flujo_diario.py — líneas 4 y 5
from motor_ingesta.motor_ingesta import MotorIngesta
from motor_ingesta.agregaciones import aniade_hora_utc, aniade_intervalos_por_aeropuerto
from motor_ingesta.flujo_diario import FlujoDiario


# COMMAND ----------

# imports necesarios

path_config_flujo_diario = "config.json"       # ruta del fichero config.json, que no pertenece al paquete
path_json_primer_dia = "abfss://bronze@umcmasterbrsspark001.dfs.core.windows.net/json/flight/2023-01-01.json"           # ruta del fichero JSON de un día concreto que queremos ingestar, en nuestro caso 2023-01-01.json

flujo_diario = FlujoDiario(path_config_flujo_diario)
motor_ingesta = MotorIngesta(flujo_diario.config)
flights_df = motor_ingesta.ingesta_fichero(path_json_primer_dia)

# YOUR CODE HERE
# raise NotImplementedError

# COMMAND ----------


flights_df.count()

# COMMAND ----------

assert(flights_df.count() == 15856)
assert(len(flights_df.columns) == 18)
dtypes = dict(flights_df.dtypes)
assert(dtypes["Diverted"] == "boolean")
assert(dtypes["ArrTime"] == "int")
assert(flights_df.schema["Dest"].metadata == {"comment": "Destination Airport IATA code (3 letters)"})

# COMMAND ----------

# MAGIC %md
# MAGIC ### Probamos la función de añadir la hora en formato UTC
# MAGIC
# MAGIC **Ejercicio 2 (2 puntos)** Probar la función de añadir hora UTC con el DF `flights_df` construido anteriormente. El resultado debe dejarse en la variable `flights_with_utc`. Recuerda que esto no es propiamente un test unitario.
# MAGIC
# MAGIC * Este ejercicio requiere haber completado previamente el código de la función `aniade_hora_utc` del paquete de Python y haber generado el fichero .whl, y por tanto, la puntuación del ejercicio se debe a ese trabajo.

# COMMAND ----------

# import necesarios

flights_with_utc = aniade_hora_utc(spark,flights_df)

# YOUR CODE HERE
# raise NotImplementedError

# COMMAND ----------

from pyspark.sql import functions as F
assert(flights_with_utc.where("FlightTime is null").count() == 266)
types = dict(flights_with_utc.dtypes)
assert(flights_with_utc.dtypes[18] == ("FlightTime", "timestamp"))  # FlightTime debe ser la última columna

first_row = flights_with_utc.where("OriginAirportID = 12884").select(F.min("FlightTime").cast("string").alias("FlightTime")).first()
assert(first_row.FlightTime == "2023-01-01 10:59:00")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Probamos la función de añadir las columnas con la hora del siguiente vuelo, su aerolínea y el intervalo de tiempo transcurrido
# MAGIC
# MAGIC **Ejercicio 3 (2.5 puntos)** Invocar a la función de añadir intervalos por aeropuerto, partiendo de la variable `flights_with_utc` del apartado anterior, dejando el resultado devuelto por la función en la variable `df_with_next_flight` cacheada.
# MAGIC
# MAGIC * Este ejercicio requiere haber completado previamente el código de la función `aniade_intervalos_por_aeropuerto` en el paquete de Python y haber generado el fichero .whl, y por tanto, la puntuación del ejercicio se debe a ese trabajo.

# COMMAND ----------

# imports necesarios

df_with_next_flight = aniade_intervalos_por_aeropuerto(flights_with_utc)

# YOUR CODE HERE
# raise NotImplementedError

# COMMAND ----------

assert(df_with_next_flight.dtypes[19] == ("FlightTime_next", "timestamp"))
assert(df_with_next_flight.dtypes[20] == ("Airline_next", "string"))
assert(df_with_next_flight.dtypes[21] == ("diff_next", "bigint"))

first_row = df_with_next_flight.where("OriginAirportID = 12884")\
                               .select(F.col("FlightTime").cast("string"), 
                                       F.col("FlightTime_next").cast("string"), 
                                       F.col("Airline_next"),
                                       F.col("diff_next")).sort("FlightTime").first()

assert(first_row.FlightTime_next == "2023-01-01 16:36:00")
assert(first_row.Airline_next == "9E")
assert(first_row.diff_next == 20220)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Corregimos que el último vuelo de cada aeropuerto y cada día tiene valor nulo en las 3 columnas `_next`
# MAGIC
# MAGIC **Ejercicio 4 (2.5 puntos)**
# MAGIC
# MAGIC Tal como está implementada la lógica del flujo diario, el último vuelo de cada día no tendrá informada la columna FlightTime_next porque no se dispone todavía de datos del día siguiente. Se pide **corregir este comportamiento** para solucionar los valores nulos, modificando el código del método `procesa_diario` de manera que, antes de escribir los datos del día actual, se hayan corregido las tres columnas `_next` en los datos del día anterior al que estamos ingestando. Una manera simple (aunque no necesariamente óptima) de conseguirlo es:
# MAGIC * Leer de la tabla la partición que se escribió el día previo, si existiera dicha tabla y dicha partición.
# MAGIC * Añadir al DF devuelto por `aniade_hora_utc` las 3 columnas que le faltan para tener la misma estructura que la tabla, que son `FlightTime_next`, `Airline_next` y `diff_next` (pueden ser en ese orden si la función `aniade_intervalos_por_aeropuerto` se ha implementado para añadirlas en ese orden), pero sin darles valor (con valor None, convirtiendo cada columna al tipo de dato adecuado para que después encaje con la tabla existente).
# MAGIC * Unir el DF del día previo y el que acabamos de calcular
# MAGIC * Invocar a `aniade_intervalos_por_aeropuerto` pasando como argumento el DF resultante de la unión.
# MAGIC
# MAGIC Aparte de un test unitario (que se deja como optativo pero sin puntuación), la manera de comprobar el funcionamiento será invocar a `procesa_diario` del flujo diario, con los ficheros de dos días consecutivos, y después comprobar lo que se ha escrito en la tabla tras la ingesta del segundo fichero. Lo probaremos con los días 1 y 2 de enero de 2023.
# MAGIC
# MAGIC * Este ejercicio requiere haber completado previamente el código del paquete de Python y haber generado el fichero .whl, y por tanto, la puntuación del ejercicio se debe a ese trabajo.

# COMMAND ----------

from pyspark.sql import SparkSession




# COMMAND ----------

path_json_segundo_dia = "abfss://bronze@umcmasterbrsspark001.dfs.core.windows.net/json/flight/2023-01-02.json"  # path del fichero 2023-01-02.json

flujo_diario.procesa_diario(path_json_primer_dia)
flujo_diario.procesa_diario(path_json_segundo_dia)

# Invoca al método procesa_diario del flujo con el path del fichero 2023-01-01.json
# Después invoca al método de nuevo con el path del fichero 2023-01-02.json

# YOUR CODE HERE
# raise NotImplementedError

# COMMAND ----------

vuelos = spark.read.table("default.flights").sort("Origin", "FlightTime")
assert(vuelos.count() == 33931)
row = vuelos.where("FlightDate = '2023-01-01' and  Origin = 'ABE' and DepTime = 1734").first()
assert(row.diff_next == 44220)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio opcional
# MAGIC
# MAGIC **Ejercicio opcional (1 punto)** Completa los cuatro tests unitarios que encontrarás en el fichero `test_ingesta.py`. No tienes que escribir más código en este notebook.
# MAGIC
# MAGIC - Se podrá optar a una calificación final de hasta 9.0 puntos sin resolver este ejercicio.