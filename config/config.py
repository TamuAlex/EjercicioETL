from json import load


class Config:
    """Clase que almacena metadatos

        Esta clase lee y almacena los metadatos contenidos en el
        fichero de configuración

    Attributes:
        sourcesList: Lista de ficheros a leer en el dataflow
        transformationsList: Lista de transformaciones a llevar a cabo
        sinks: Lista de lugares donde guardar los ficheros una vez transformados
    """

    def __init__(self, log):
        """ Inicializa la clase Config

        Args:
            log: Log general

        """
        self.log = log

        # Se lee el fichero de configuracion
        try:
            with open('config/config.json') as config_file:
                json_file = load(config_file)
        except Exception as e:
            self.log.exception('Error leyendo el fichero de configuración')
            raise e

        # Se cargan la información necesaria
        try:
            self.sourcesList = json_file['dataflows'][0]['sources']
            self.transformationsList = json_file['dataflows'][0]['transformations']
            self.sinks = json_file['dataflows'][0]['sinks']
        except Exception as e:
            self.log.exception('Error con el formato del fichero de configuración')
            raise e
        self.log.info('Fichero de configuración leido correctamente')
