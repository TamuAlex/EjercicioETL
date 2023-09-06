from json import load

class Config:

    def __init__(self, log):
        self.log = log

        try:
            with open('config/config.json') as configFile:
                jsonFile = load(configFile)
        except Exception as e:
            self.log.exception('Error leyendo el fichero de configuración')
            raise e

        try:
            self.sourcesList = jsonFile['dataflows'][0]['sources']
            self.transformationsList = jsonFile['dataflows'][0]['transformations']
            self.sinks = jsonFile['dataflows'][0]['sinks']
        except Exception as e:
            self.log.exception('Error con el formato del fichero de configuración')
            raise e
        self.log.info('Fichero de configuración leido correctamente')