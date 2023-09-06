from pyspark.sql.functions import col, lit

from config.config import Config
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from log.log import Log


class Transform:
    """ Clase que se encarga de realizar las transformaciones

    Esta clase toma los dataframes leidos por la clase Extract, y realiza
    las transformaciones necesarias en funcion de lo establecido en el
    fichero de configuración

    Attributes:
        config: Onbjeto config con la info de los metadatos
        spark_session: sesion de spark
        log: log generico
    """

    def __init__(self, config: Config, spark_session: SparkSession, log: Log):
        """
        Constructor de la clase Transform

        Args:
            config: Onbjeto config con la info de los metadatos
            spark_session: sesion de spark
            log: log generico
        """

        self.config = config
        self.spark_session = spark_session
        self.log = log

    def transformations(self, sources_list):
        """
        Esta clase itera primero sobre cada uno de los dataframes a transformar,
        para luego iterar sobre cada una de las transformaciones a realizar,
        llamando a los metodos indicados para cada una de ellas.

        También añade una columna de error al dataframe donde se van almacenando
        todas las validaciones no cumplidas

        Args:
            sources_list: diccionario con los nombres y los dataframes a transformar

        Returns:
            el mismo diccionario que entra como argumento, pero con sus dataframes transformados
        """

        # Primero se itera sobre cada dataframe a transformar
        for (source_name, source_df) in sources_list.items():
            self.log.info('comenzamos con la transformacion de ' + source_name)

            # Al dataframe se le añade la columna que recoje los errores
            source_df = source_df.withColumn("Error", lit(""))

            # A continuación se itera sobre cada una de las transformaciones a realizar
            for transformation in self.config.transformationsList:

                if transformation['type'] == 'validate_fields':
                    try:
                        for validation in transformation['params']['validations']:
                            source_df = self.validate(source_df, validation['field'], validation['validations'])
                    except Exception as e:
                        self.log.error('Error a la hora de realizar la validacion con nombre: ' + transformation['name'])
                    self.log.info('La validacion con nombre ' + transformation['name'] + ' se ha realizado correctamente')

                if transformation['type'] == 'add_fields':
                    try:
                        for fields in transformation['params']['addFields']:
                            source_df = self.add_fields(source_df, fields['name'], fields['function'])
                    except Exception as e:
                        self.log.error('Error a la hora de realizar la transformacion de añadir campos con nombre: ' + transformation['name'])
                    self.log.info('La transformacion de añadir campos con nombre ' + transformation['name'] + ' se ha realizado correctamente')

            sources_list[source_name] = source_df
        return sources_list


    def validate(self, df, field, validations):
        """
        Funcion que se encarga de, dado un campo, y una lista de validaciones,
        validar si el campo cumple dichas validaciones.

        en caso de no cumplirlas, se almacena el error en la columna Error

        Args:
            df: dataframe a validar
            field: campo sobre el que se ejecuta la validacion
            validations: lista de validaciones a realizar

        Returns:
            El mismo dataframe de entrada, pero con la columna Error actualizada
            para las entradas que no cumplen alguna validación
        """

        for validation in validations:
            if validation == 'notEmpty':
                df = df.withColumn("Error", F.when(col(field)=='', F.concat(col('Error'),lit(f" {field} is empty;"))).otherwise(col('Error')))

            if validation == 'notNull':
                df = df.withColumn("Error",
                              F.when(col(field).isNull(), F.concat(col('Error'), lit(f" {field} is null;"))).otherwise(
                                  col('Error')))

        return df


    def add_fields(self, df, field_name, function):
        """
        Función que se encarga de, dado un nombre de columna y una funcion, añade una
        columna al dataframe condicha funcion

        Args:
            df: dataframe a transformar
            field_name: nombre de la columna
            function: funcion a añadir

        Returns:
            dataframe ya transformado
        """

        if function == 'current_timestamp':
            df = df.withColumn(field_name, F.current_timestamp())

        return df