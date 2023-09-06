from config.config import Config
from pyspark.sql import dataframe, SparkSession
from log.log import Log

from etl.extract import Extract
from etl.transform import Transform
from etl.load import Load

def run():
    #Extraemos info config
    log = Log()
    config = Config(log)


    try:
        spark_builder = (SparkSession.builder.appName("suscripcion_digital"))
        spark_session = spark_builder.getOrCreate()

    except Exception as e:
        log.exception('Error obteniendo la sesion de Spark:')
        raise e

    log.info('Sesion de Saprk obtenida con exito')


    extract = Extract(config, spark_session, log)
    transform = Transform(config, spark_session, log)
    load = Load(config, spark_session, log)

    log.info('-------Comienza el proceso de Extract-------')
    sourcesList = extract.read_source()
    log.info('-------Comienza el proceso de Transform-------')
    sourcesList = transform.transformations(sourcesList)
    log.info('-------Comienza el proceso de Load-------')
    load.load(sourcesList)


run()
