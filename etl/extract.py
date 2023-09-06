from config.config import Config
from pyspark.sql import SparkSession

from log.log import Log


class Extract:
    """ Clase encargada de realizar la extraccion

    Esta clase se encarga de leer los ficheros de entrada, extraer
    su información, y transformarla en dataframes

    Attributes:
        config: Onbjeto config con la info de los metadatos
        spark_session: sesion de spark
        log: log generico

    """

    def __init__(self, config: Config, spark_session: SparkSession, log: Log):
        """
        Constructor de la clase Extract

        Args:
            config: Onbjeto config con la info de los metadatos
            spark_session: sesion de spark
            log: log generico
        """

        self.config = config
        self.spark_session = spark_session
        self.log = log

    def read_source(self):
        """
        Metodo que lee la información origen en función de su formato, y la
        transforma en dataframes

        Returns:
            un diccionario cuyas claves son el nombre del origen de los datos, y su
            valor asociado el dataframe de los datos de dicho origen

        """

        sources_dic = {}

        for source in self.config.sourcesList:

            name = source['name']

            try:
                # Se comprueba el formato, y se lee de acuerdo a el
                if source['format'] == 'JSON':
                    df = self.spark_session.read.json(source['path'])

                # Si no se ha encontrado el formato, se avisa en el log
                else:
                    self.log.warning('el formato ' + source['format'] + ' no esta implementado')
                    df = None

                # Se añade el par (nombre->DataFrame) al diccionario
                sources_dic[name] = df

            except Exception as e:
                self.log.error('Error leyendo el fichero con nombre ' + name)
                raise e
            self.log.info(name + ' leido correctamente')

        return sources_dic
