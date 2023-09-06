from config.config import Config
from pyspark.sql import SparkSession
from log.log import Log

from etl.extract import Extract
from etl.transform import Transform
from etl.load import Load


def run():
    # Se crea el log y se lee el fichero de configuración
    log = Log()
    config = Config(log)

    # Se inicializa el SparkSession
    try:
        spark_builder = (SparkSession.builder.appName("suscripcion_digital"))
        spark_session = spark_builder.getOrCreate()

    except Exception as e:
        log.exception('Error obteniendo la sesion de Spark:')
        raise e

    log.info('Sesion de Saprk obtenida con exito')

    # Se inicializan las diferentes clases de ETL
    extract = Extract(config, spark_session, log)
    transform = Transform(config, spark_session, log)
    load = Load(config, spark_session, log)

    # Se ejecuta el flujo del programa
    log.info('-------Comienza el proceso de Extract-------')
    sources_list = extract.read_source()
    log.info('-------Comienza el proceso de Transform-------')
    sources_list = transform.transformations(sources_list)
    log.info('-------Comienza el proceso de Load-------')
    load.load(sources_list)


run()
