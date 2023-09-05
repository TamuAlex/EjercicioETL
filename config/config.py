from json import load

class Config:

    def __init__(self):
        with open('config/config.json') as configFile:
            jsonFile = load(configFile)


        self.sourcesList = jsonFile['dataflows'][0]['sources']
        self.transformationsList = jsonFile['dataflows'][0]['transformations']
        self.sinks = jsonFile['dataflows'][0]['sinks']




config = Config()
#print(config.sourcesList)
#print(config.transformationsList)
# #print(config.sinks)
