import json
from collections import namedtuple
from pathlib import Path
from motor_ingesta.motor_ingesta import MotorIngesta
from motor_ingesta.agregaciones import aniade_intervalos_por_aeropuerto, aniade_hora_utc
from pyspark.sql import functions as F


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
    aplanado_df = ...

    # Comprobamos (assert) que cada una de las columnas a1, a2, a3, b1, b2, nombre, edad
    # están incluidas en la lista de columns de aplanado_df. Las columnas "tupla" y "amigos" ya no deben existir

    assert(...)


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
    with ... :
        config = ...

    # Crear un objeto motor de ingesta a partir del diccionario config
    # motor_ingesta = ...
    motor_ingesta = MotorIngesta(config)

    # Ingestar el fichero JSON de datos que hay en path_test_data mediante la variable motor_ingesta
    # datos_df =
    datos_df = motor_ingesta.ingesta_fichero(path_test_data)

    # Comprobar que los datos ingestados tienen una sola fila y las columnas nombre, parentesco, numero, profesion
    assert(...)  # comprobar que tiene 4 columnas y que nombre, parentesco, numero, profesion están incluidas

    # primera_fila = ...    # extraer el objeto de la primera fila
    primera_fila = datos_df.first()
    assert(...)  # comprobar que la primera fila contiene los valores adecuados en cada uno de sus 4 campos


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
        # Completa el DataFrame que deberíamos obtener
    )

    expected_row = ...         # extraer la primera fila de expected_df

    result_df = aniade_intervalos_por_aeropuerto(test_df)
    actual_row = ...           # extraer la primera fila de result_df

    # Comparar los campos de ambos objetos Row
    # assert(...)


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
        # Completa el DataFrame que deberíamos obtener
    )

    expected_row = None  # extraer la primera fila de expected_df

    result_df = aniade_hora_utc(spark, test_df)
    actual_row = ...  # extraer la primera fila de result_df

    # Comparar los campos de ambos objetos Row
    assert(...)
