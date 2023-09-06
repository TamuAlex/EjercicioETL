from config.config import Config
from pyspark.sql import dataframe, SparkSession
from pyspark.sql import functions as F, types as T

from log.log import Log


class Load:
    """ Clase que se encarga de guardar los datos

    Esta clase toma los datos ya transformados y los guarda en los
    sinks que vienen en el fichero de configuración

    Attributes:
        config: Onbjeto config con la info de los metadatos
        spark_session: sesion de spark
        log: log generico

    """

    def __init__(self, config: Config, spark_session: SparkSession, log:Log):
        """
        Constructor de la clase Load

        Args:
            config: Onbjeto config con la info de los metadatos
            spark_session: sesion de spark
            log: log generico
        """
        self.config = config
        self.spark_session = spark_session
        self.log = log


    def load(self, source_list):
        """
        Funcion que se encarga de hacer la carga de datos

        Args:
            source_list: lista de dataframes a cargar
        """
        for source_df in source_list.values():
            df_ok = source_df.filter(F.col('Error') == '').drop('Error')
            df_ko = source_df.filter(F.col('Error') != '')

            for sink in self.config.sinks:
                try:
                    if sink['name']=='raw-ok':
                        for path in sink['paths']:
                            df_ok.coalesce(1).write.mode(sink['saveMode']).format(sink['format']).save(path)
                    if sink['name']=='raw-ko':
                        for path in sink['paths']:
                            df_ko.coalesce(1).write.mode(sink['saveMode']).format(sink['format']).save(path)
                except Exception as e:
                    self.log.exception('Error en el Load: ')
                self.log.info('Load ejecutado correctamente')

