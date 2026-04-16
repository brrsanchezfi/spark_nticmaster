import json
from collections import namedtuple
from pathlib import Path
from motor_ingesta.motor_ingesta import MotorIngesta
from motor_ingesta.agregaciones import aniade_intervalos_por_aeropuerto, aniade_hora_utc
from pyspark.sql import functions as F
from datetime import datetime


def test_aplana(spark):
    """
    Testea que el aplanado se haga correctamente con un DF creado ad-hoc
    :param spark: SparkSession configurada localmente
    :return:
    """
    # La variable spark es un fixture - un objeto que se crea automáticamente al arrancar todos los tests
    # (consulta conftest.py)

    # Definimos dos clases de tuplas asignando nombres a cada campo de la tupla.
    # Las usaremos después para crear objetos
    tupla3 = namedtuple("tupla3", ["a1", "a2", "a3"])
    tupla2 = namedtuple("tupla2", ["b1", "b2"])

    test_df = spark.createDataFrame(
        [(tupla3("a", "b", "c"), "hola", 3, [tupla2("pepe", "juan"), tupla2("pepito", "juanito")])],
        ["tupla", "nombre", "edad", "amigos"]
        # La columna tupla es un struct de 3 campos. La columna amigos es un array de structs, de 2 campos cada uno
    )

    # Invocamos al método aplana_df de la clase MotorIngesta para aplanar el DF test_df

    aplanado_df = MotorIngesta.aplana_df(test_df)

    # Comprobamos (assert) que cada una de las columnas a1, a2, a3, b1, b2, nombre, edad
    # están incluidas en la lista de columns de aplanado_df. Las columnas "tupla" y "amigos" ya no deben existir

    columnas = aplanado_df.columns
    assert "a1" in columnas
    assert "a2" in columnas
    assert "a3" in columnas
    assert "b1" in columnas
    assert "b2" in columnas
    assert "nombre" in columnas
    assert "edad" in columnas
    assert "tupla" not in columnas
    assert "amigos" not in columnas

def test_ingesta_fichero(spark):
    """
    Comprueba que la ingesta de un fichero JSON de prueba se hace correctamente. Utiliza el fichero
    JSON existente en la carpeta tests/resources
    :param spark: SparkSession inicializada localmente
    :return:
    """
    ##################################################
    #            EJERCICIO OPCIONAL
    ##################################################

    carpeta_este_fichero = str(Path(__file__).parent)
    path_test_config = carpeta_este_fichero + "/resources/test_config.json"
    path_test_data = carpeta_este_fichero + "/resources/test_data.json"

    # Leer el fichero test_config.json como diccionario con json.load(f)
    with open(path_test_config) as f:
        config = json.load(f)

    motor_ingesta = MotorIngesta(config, spark)

    datos_df = motor_ingesta.ingesta_fichero(path_test_data)

    # 4 columnas y los nombres correctos
    assert len(datos_df.columns) == 4
    assert all(c in datos_df.columns for c in ["nombre", "parentesco", "numero", "profesion"])

    primera_fila = datos_df.first()
    assert primera_fila.nombre == "Juan"           
    assert primera_fila.parentesco == "sobrino"    
    assert primera_fila.numero == 3                
    assert primera_fila.profesion == "Ingeniero"  

def test_aniade_intervalos_por_aeropuerto(spark):
    """
    Comprueba que las variables añadidas con información del vuelo inmediatamente posterior que sale del mismo
    aeropuerto están bien calculadas
    :param spark: SparkSession inicializada localmente
    :return:
    """

    ##################################################
    #            EJERCICIO OPCIONAL
    ##################################################

    test_df = spark.createDataFrame(
        [("JFK", "2023-12-25 15:35:00", "American_Airlines"),
         ("JFK", "2023-12-25 17:35:00", "Iberia")],
        ["Origin", "FlightTime", "Reporting_Airline"]
    ).withColumn("FlightTime", F.col("FlightTime").cast("timestamp"))

    expected_df = spark.createDataFrame(
        [("JFK", "2023-12-25 15:35:00", "American_Airlines", "2023-12-25 17:35:00", "Iberia", 7200),
         ("JFK", "2023-12-25 17:35:00", "Iberia",             None,                  None,    None)],
        ["Origin", "FlightTime", "Reporting_Airline", "FlightTime_next", "Airline_next", "diff_next"]
    ).withColumn("FlightTime",      F.col("FlightTime").cast("timestamp")) \
     .withColumn("FlightTime_next", F.col("FlightTime_next").cast("timestamp")) \
     .withColumn("diff_next",       F.col("diff_next").cast("long"))

    expected_row = expected_df.orderBy("FlightTime").first()

    result_df = aniade_intervalos_por_aeropuerto(test_df)
    actual_row = result_df.orderBy("FlightTime").first()

    assert actual_row.FlightTime_next == expected_row.FlightTime_next
    assert actual_row.Airline_next    == expected_row.Airline_next
    assert actual_row.diff_next       == expected_row.diff_next  # 7200 segundos = 2 horas

def test_aniade_hora_utc(spark):
    """
    Comprueba que la columna FlightTime en la zona horaria UTC está correctamente calculada
    :param spark: SparkSession inicializada localmente
    :return:
    """
    ##################################################
    #            EJERCICIO OPCIONAL
    ##################################################

    fichero_timezones = str(Path(__file__).parent) + "../motor_ingesta/resources/timezones.csv"

    test_df = spark.createDataFrame(
        [("JFK", "2023-12-25", 1535)],
        ["Origin", "FlightDate", "DepTime"]
    )

    expected_df = spark.createDataFrame(
        [("JFK", "2023-12-25", 1535, datetime(2023, 12, 25, 20, 35))],
        ["Origin", "FlightDate", "DepTime", "FlightTime"]
    )

    expected_row = expected_df.first()

    result_df = aniade_hora_utc(spark, test_df)
    actual_row = result_df.select("Origin", "FlightDate", "DepTime", "FlightTime").first()

    # Comparar campo a campo
    assert actual_row.Origin == expected_row.Origin
    assert actual_row.FlightDate == expected_row.FlightDate
    assert actual_row.DepTime == expected_row.DepTime
    assert actual_row.FlightTime == expected_row.FlightTime
