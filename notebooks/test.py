# imports necesarios

from motor_ingesta.motor_ingesta import MotorIngesta
from motor_ingesta.agregaciones import aniade_hora_utc, aniade_intervalos_por_aeropuerto

from motor_ingesta.flujo_diario import FlujoDiario

path_config_flujo_diario = "../config/config.json"

path_json_primer_dia = "../data/flights_json/landing/2023-01-01.json"
path_json_segundo_dia = "../data/flights_json/landing/2023-01-02.json"


flujo_diario = FlujoDiario(path_config_flujo_diario)

flujo_diario.procesa_diario(path_json_primer_dia)

flujo_diario.procesa_diario(path_json_segundo_dia)

# Leer el resultado para verificar
flights_df = flujo_diario.spark.read.parquet(flujo_diario.config["output_path"])
flights_df.show(5)