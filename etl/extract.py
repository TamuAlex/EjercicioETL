from config.config import Config
from pyspark.sql import dataframe, SparkSession


class Extract:

    def __init__(self, config: Config, spark_session: SparkSession):
        self.config = config
        self.spark_session = spark_session

    def read_source(self):
        sourcesDic = {}

        for source in self.config.sourcesList:
            print(source)
            name = source['name']

            if source['format'] == 'JSON':
                df = self.spark_session.read.json(source['path'])

            sourcesDic[name] = df

        return sourcesDic

