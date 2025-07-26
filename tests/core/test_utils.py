from datta_pipeline_library.core.utils import get_latest
from pyspark.sql import SparkSession
from chispa.dataframe_comparer import assert_df_equality


class TestGetLatest:
    def test_utils_get_latest_str(self):
        # data = [{"name": "a", "ingested_dt": "2023-01-12 14:31:12.802000"},
        #         {"name": "x", "ingested_dt": "2023-01-17 14:31:12.802000"}]

        # expected_df = spark.createDataFrame([{"name": "x", "ingested_dt": "2023-01-17 14:31:12.802000"}])

        # input_df = spark.createDataFrame(data)
        # output_df = get_latest(input_df, "ingested_dt")
        # assert_df_equality(output_df, expected_df)
        pass

    def test_utils_get_latest_int(self):
        # data = [{"name": "a", "ingested_dt": 3000},
        #         {"name": "x", "ingested_dt": 4000}]

        # expected_df = spark.createDataFrame([{"name": "x", "ingested_dt": 4000}])

        # input_df = spark.createDataFrame(data)
        # output_df = get_latest(input_df, "ingested_dt")
        # assert_df_equality(output_df, expected_df)
        pass
