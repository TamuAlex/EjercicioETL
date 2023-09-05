from pyspark.sql.functions import col, lit

from config.config import Config
from pyspark.sql import dataframe, SparkSession
from pyspark.sql import functions as F, types as T


class Transform:

    def __init__(self, config: Config, spark_session: SparkSession):
        self.config = config
        self.spark_session = spark_session

    def transformations(self, sourcesList):
        for (source_name, source_df) in sourcesList.items():

            source_df = source_df.withColumn("Error", lit(""))

            for transformation in self.config.transformationsList:
                if transformation['type'] == 'validate_fields':
                    for validation in transformation['params']['validations']:
                        source_df = self.validate(source_df, validation['field'], validation['validations'])


                if transformation['type'] == 'add_fields':
                    for fields in transformation['params']['addFields']:
                        source_df = self.add_fields(source_df, fields['name'], fields['function'])

            sourcesList[source_name] = source_df
        return sourcesList


    def validate(self, df, field, validations):
        for validation in validations:
            if validation == 'notEmpty':
                df = df.withColumn("Error", F.when(col(field)=='', F.concat(col('Error'),lit(f" {field} is empty;"))).otherwise(col('Error')))

            if validation == 'notNull':
                df = df.withColumn("Error",
                              F.when(col(field).isNull(), F.concat(col('Error'), lit(f" {field} is null;"))).otherwise(
                                  col('Error')))

        return df


    def add_fields(self, df, field_name, function):
        if function == 'current_timestamp':
            df = df.withColumn(field_name, F.current_timestamp())

        return df