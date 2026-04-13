from pathlib import Path

from pyspark.sql import SparkSession, DataFrame as DF, functions as F, Window
import pandas as pd


def aniade_hora_utc(spark: SparkSession, df: DF) -> DF:
    """
    Añade al DataFrame de vuelos una columna FlightTime con la hora de despegue
    en UTC, interpretando DepTime y FlightDate según la zona horaria del aeropuerto
    de origen.

    Pasos:
      1. Lee el CSV de timezones y lo une al DF por código IATA (Origin ↔ iata_code),
         manteniendo todos los vuelos aunque el aeropuerto no esté en el CSV (left join).
      2. Construye la columna FlightTime como timestamp local concatenando FlightDate
         y DepTime (con padding a 4 dígitos para garantizar formato HHMM).
      3. Convierte FlightTime a UTC usando la zona horaria de la columna iana_tz.
      4. Elimina las columnas auxiliares (las de timezones_df y castedHour).

    :param spark: SparkSession activa.
    :param df: DataFrame con columnas FlightDate (date), DepTime (int/double),
               Origin (string) y el resto de columnas de vuelo.
    :return: DataFrame original con la columna FlightTime (timestamp UTC) añadida
             por la derecha. Las columnas del CSV de timezones y castedHour se eliminan.
    """
    path_timezones = str(Path(__file__).parent) + "/resources/timezones.csv"
    timezones_pd = pd.read_csv(path_timezones)
    timezones_df = spark.createDataFrame(timezones_pd)

    # Paso 1: left join para conservar vuelos sin timezone en el CSV
    cols_timezones = timezones_df.columns
    df_with_tz = df.join(
        timezones_df,
        on=df["Origin"] == timezones_df["iata_code"],
        how="left"
    )

    # Pasos 2a-2c: castedHour → FlightTime local → FlightTime UTC
    df_with_flight_time = (
        df_with_tz
        # (a) padding a 4 dígitos — null si DepTime es null
        .withColumn(
            "castedHour",
            F.when(F.col("DepTime").isNull(), F.lit(None).cast("string"))
            .otherwise(F.lpad(F.col("DepTime").cast("string"), 4, "0"))
        )

        # Tratar el caso especial 2400 → 0000 del día siguiente
        .withColumn(
            "fecha_ajustada",
            F.when(F.col("castedHour") == "2400",
                F.date_add(F.col("FlightDate").cast("date"), 1))
            .otherwise(F.col("FlightDate").cast("date"))
        )
        .withColumn(
            "castedHour",
            F.when(F.col("castedHour") == "2400", F.lit("0000"))
            .otherwise(F.col("castedHour"))
        )

        # (b) FlightTime — será null si castedHour es null
        .withColumn(
            "FlightTime",
            F.when(
                F.col("castedHour").isNull(), F.lit(None).cast("timestamp")
            ).otherwise(
                F.concat(
                    F.col("fecha_ajustada").cast("string"),
                    F.lit(" "),
                    F.col("castedHour").substr(1, 2),
                    F.lit(":"),
                    F.col("castedHour").substr(3, 2),
                    F.lit(":00"),
                ).cast("timestamp")
            )
        )

        # (c) convertir a UTC — to_utc_timestamp devuelve null si FlightTime es null
        .withColumn("FlightTime", F.to_utc_timestamp(F.col("FlightTime"), F.col("iana_tz")))

        # (d) borrar columnas auxiliares
        .drop(*cols_timezones, "castedHour", "fecha_ajustada")
    )
    
    return df_with_flight_time


def aniade_intervalos_por_aeropuerto(df: DF) -> DF:
    """
    Añade a cada vuelo la información del vuelo que despega justo después desde
    el mismo aeropuerto de origen.

    Las tres columnas nuevas añadidas por la derecha son:
      - FlightTime_next (timestamp UTC): hora de despegue del siguiente vuelo
        en el mismo aeropuerto.
      - Airline_next (string): compañía aérea del siguiente vuelo.
      - diff_next (long): diferencia en segundos entre FlightTime_next y
        FlightTime del vuelo actual (next − actual). Null si no hay vuelo posterior.

    :param df: DataFrame con columnas FlightTime (timestamp), Origin (string)
               y Reporting_Airline (string).
    :return: DataFrame idéntico al de entrada con las tres columnas nuevas.
             Cualquier columna auxiliar es eliminada antes de devolver el resultado.
    """
    # Ventana particionada por aeropuerto de origen y ordenada por hora de despegue
    w = Window.partitionBy("Origin").orderBy("FlightTime")

    df_with_next_flight = (
        df
        # Columna auxiliar: tupla (FlightTime, Reporting_Airline) del vuelo actual
        .withColumn(
            "_next_pair",
            F.lag(F.struct("FlightTime", "Reporting_Airline"), -1).over(w)
        )
        # Extraer los campos de la tupla como columnas independientes
        .withColumn("FlightTime_next", F.col("_next_pair.FlightTime"))
        .withColumn("Airline_next",    F.col("_next_pair.Reporting_Airline"))
        # Diferencia en segundos: (next − actual) sobre el tipo long
        .withColumn(
            "diff_next",
            F.col("FlightTime_next").cast("long") - F.col("FlightTime").cast("long")
        )
        # Borrar columna auxiliar
        .drop("_next_pair")
    )

    return df_with_next_flight