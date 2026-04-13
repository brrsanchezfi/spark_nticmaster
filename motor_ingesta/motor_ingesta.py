import json
from pyspark.sql import DataFrame as DF, functions as F, SparkSession


class MotorIngesta:
    """
    Motor de ingesta de ficheros JSON hacia DataFrames de Spark.

    Lee un fichero JSON desde DBFS o ruta local, lo aplana recursivamente
    (explotando arrays y desanidando structs) y selecciona únicamente las
    columnas declaradas en la configuración, casteándolas al tipo indicado
    e incluyendo comentarios como metadatos de columna.

    Atributos:
        config (dict): Diccionario de configuración con al menos la clave
                       'data_columns', lista de dicts con las claves
                       'name', 'type' y 'comment'.
        spark (SparkSession): SparkSession activa.
    """

    def __init__(self, config: dict):
        """
        Inicializa el motor con la configuración proporcionada y obtiene
        (o crea) la SparkSession activa.

        :param config: Diccionario de configuración. Debe incluir la clave
                       'data_columns' con una lista de diccionarios, cada uno
                       con los campos:
                           - 'name'    (str)  : nombre de la columna en el DF aplanado.
                           - 'type'    (str)  : tipo Spark al que castear (p.ej. 'string',
                                                'double', 'integer', 'date', …).
                           - 'comment' (str)  : descripción de la columna que se almacena
                                                como metadato.
        """
        self.config = config
        self.spark = SparkSession.builder.getOrCreate()

    def ingesta_fichero(self, json_path: str) -> DF:
        """
        Lee un fichero JSON, aplana su estructura anidada y devuelve un
        DataFrame con las columnas tipadas definidas en la configuración.

        El proceso es:
          1. Lectura del JSON con inferencia de esquema y soporte multilínea.
          2. Aplanado recursivo mediante :meth:`aplana_df` (explota arrays y
             desanida structs a cualquier nivel de profundidad).
          3. Selección y casteo de las columnas declaradas en
             ``self.config["data_columns"]``, añadiendo el campo 'comment'
             como metadato de cada columna.

        :param json_path: Ruta al fichero JSON (local o DBFS),
                          p.ej. ``"/dbfs/mnt/raw/flights/2023-01-01.json"``.
        :return: DataFrame de Spark con las columnas seleccionadas, casteadas
                 y anotadas con metadatos.
        """
        # 1. Lectura del JSON inferiendo el esquema
        flights_day_df = self.spark.read.option("multiline", "true").json(json_path)

        # 2. Aplanado recursivo (explota arrays, desanida structs)
        aplanado_df = MotorIngesta.aplana_df(flights_day_df)

        # 3. Selección con casteo y metadatos
        lista_obj_column = [
            F.col(diccionario["name"])
             .cast(diccionario["type"])
             .alias(diccionario["name"], metadata={"comment": diccionario["comment"]})
            for diccionario in self.config["data_columns"]
        ]

        resultado_df = aplanado_df.select(*lista_obj_column)
        return resultado_df

    @staticmethod
    def aplana_df(df: DF) -> DF:
        """
        Aplana un DataFrame de Spark que tenga columnas de tipo array y de tipo estructura.

        :param df: DataFrame de Spark que contiene columnas de tipo array o columnas de tipo estructura, incluyendo
                   cualquier nivel de anidamiento y también arrays de estructuras. Asumimos que los nombres de los
                   campos anidados son todos distintos entre sí, y no van a coincidir cuando sean aplanados.
        :return: DataFrame de Spark donde todas las columnas de tipo array han sido explotadas y las estructuras
                 han sido aplanadas recursivamente.
        """
        to_select = []
        schema = df.schema.jsonValue()
        fields = schema["fields"]
        recurse = False

        for f in fields:
            if f["type"].__class__.__name__ != "dict":
                to_select.append(f["name"])
            else:
                if f["type"]["type"] == "array":
                    to_select.append(F.explode(f["name"]).alias(f["name"]))
                    recurse = True
                elif f["type"]["type"] == "struct":
                    to_select.append(f"{f['name']}.*")
                    recurse = True

        new_df = df.select(*to_select)
        return MotorIngesta.aplana_df(new_df) if recurse else new_df