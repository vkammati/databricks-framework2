"""
Basic Transformation Module
"""
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import input_file_name, concat_ws, split, slice

def transform_basic(df: DataFrame) -> DataFrame:
    """
    Function Takes in a dataframe and returns back the same Spark dataframe,
    this is wrapper function mostly used for static/lookup files
    where there is no transformation required between layers.

    :param df: DataFrame
    :return: DataFrame
    """
    
    return df

def transform_yrmono(df, clmn="yrmono"):
    """
    Function Takes in a dataframe and returns back a dataframe
    which takes the column name as input and derives yrmono from the input file name

    :param df: DataFrame
    :param clmn: Column name
    :return: DataFrame
    """
    return df.withColumn(clmn, concat_ws("", slice(split(input_file_name(), "/"), 9, 1)))
