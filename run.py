from config.config import Config
from pyspark.sql import dataframe, SparkSession

from etl.extract import Extract
from etl.transform import Transform
from etl.load import Load

def run():
    #Extraemos info config
    config = Config()

    spark_builder = (SparkSession.builder.appName("suscripcion_digital"))
    spark_session = spark_builder.getOrCreate()
    extract = Extract(config, spark_session)
    sourcesList = extract.read_source()

    transform = Transform(config, spark_session)

    sourcesList = transform.transformations(sourcesList)

    print("antes del load")
    sourcesList['person_inputs'].show()

    load = Load(config,spark_session)

    load.load(sourcesList)
    sourcesList['person_inputs'].show()


    #ETL con dicha info

run()
