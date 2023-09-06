from config.config import Config
from pyspark.sql import dataframe, SparkSession

from log.log import Log


class Extract:

    def __init__(self, config: Config, spark_session: SparkSession, log: Log):
        self.config = config
        self.spark_session = spark_session
        self.log = log

    def read_source(self):
        sourcesDic = {}

        for source in self.config.sourcesList:

            name = source['name']
            try:
                if source['format'] == 'JSON':
                    df = self.spark_session.read.json(source['path'])

                sourcesDic[name] = df
            except Exception as e:
                self.log.error('Error leyendo el fichero con nombre '+ name)
                raise e
            self.log.info(name + ' leido correctamente')


        return sourcesDic

