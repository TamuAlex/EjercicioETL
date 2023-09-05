from config.config import Config
from pyspark.sql import dataframe, SparkSession
from pyspark.sql import functions as F, types as T
class Load:

    def __init__(self, config: Config, spark_session: SparkSession):
        self.config = config
        self.spark_session = spark_session


    def load(self, source_list):
        print("Entrando en el load")
        for source_df in source_list.values():
            df_ok = source_df.filter(F.col('Error') == '').drop('Error')
            df_ko = source_df.filter(F.col('Error') != '')

            for sink in self.config.sinks:
                print("Leyendo siks")
                if sink['name']=='raw-ok':
                    for path in sink['paths']:
                        df_ok.coalesce(1).write.mode(sink['saveMode']).format(sink['format']).save(path)
                if sink['name']=='raw-ko':
                    for path in sink['paths']:
                        df_ko.coalesce(1).write.mode(sink['saveMode']).format(sink['format']).save(path)



