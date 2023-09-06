import logging


class Log:
    """
    Logger generico
    """

    def __init__(self):
        self.logger = logging.getLogger("py4j")
        self.logger.addHandler(logging.FileHandler('log/logFile.log'))
        self.logger.setLevel(logging.INFO)

    def error(self, message):
        self.logger.error(message)
        return None

    def warning(self, message):
        self.logger.warning(message)
        return None

    def info(self, message):
        self.logger.info(message)
        return None

    def debug(self, message):
        self.logger.error(message)
        return None

    def exception(self, message):
        self.logger.exception(message)
        return None
