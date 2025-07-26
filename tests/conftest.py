import pytest
import os
import sys
from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['pipeline'] = "Test"

@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Create and configure the Spark session for all tests.
    :return: the Spark session for all unit tests
    """
    spark=(SparkSession
            .builder
            .master("local[*]")
            .appName("unit-tests")
            .getOrCreate())
    return spark
