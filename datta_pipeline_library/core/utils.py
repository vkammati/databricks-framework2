"""
Reusable Functions Module
"""
from pyspark.sql.functions import max
from datta_pipeline_library.core.spark_init import spark

def read_table(dataset_name, latest=True):
    """
        Read's delta table from the path provided

        :param:table_path : physical path of the data
        :param: latest : Boolean Parameter
        When set to True, only latest data is filtered out based on ingested_at column
        When set to False, returns the full dataframe
    """
    import dlt
    df = dlt.read(dataset_name)
    if latest:
        get_latest(df, "ingested_at")
    return df

def get_latest(df, col):
    """

    :param df: Dataframe
    :param col: column name
    :return: df
    """
    # [TODO] Handle when the column _ingested_dt doesn't exists.
    filter_value = df.select(max(col)).collect()[0][0]
    df = df.where(f"{col} = '{filter_value}'")
    return df

def get_ingested_dt():
    """
    Column Extension funciton which returns current timestamp
    """
    return spark.sql("select current_timestamp()").first()[0]

def has_column(df, clm):
    """
    function checks if a column exists for a given dataframe
    :param df: Input Dataframe
    :param clm: column name string to check against the dataframe
    :return: Boolean
    """
    return clm in df.columns
