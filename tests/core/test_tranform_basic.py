from datta_pipeline_library.core.transform_basic import transform_basic
from pyspark.sql import SparkSession
from chispa.dataframe_comparer import assert_df_equality


def test_transform_basic():
    # data = [("a", "b", "100", "M", 3000), ("x", "y", "200", "F", 4000)]

    # input_df = spark.createDataFrame(data)
    # output_df = transform_basic(input_df)
    # assert_df_equality(input_df, output_df)
    pass
