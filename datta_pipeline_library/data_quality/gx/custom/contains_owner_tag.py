"""
Custom Expectation for validating owner
"""

from typing import Optional

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import (
    SparkDFExecutionEngine
)
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
)

import pyspark.sql.functions as F


class ColumnValuesToHaveOwner(ColumnMapMetricProvider):
    """ Owner Tag Custom Expectation """
    condition_metric_name = "tags.owner"

    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, **kwargs):
        # Note : This method is built for demonstration purpose only.
        accessor_domain_kwargs = kwargs.get('_accessor_domain_kwargs')
        column_name = accessor_domain_kwargs.get('column')
        df = kwargs.get('_table')
        query = F.when((~(df[column_name].contains('"Owner":')) | (df[column_name].isNull()) | (
            df[column_name].contains('"Owner":""'))), F.lit(False)).otherwise(F.lit(True))
        return query


class ExpectColumnValuesToHaveOwner(ColumnMapExpectation):
    """Validates if the given column contains 'Owner' tag. This is specifically useful for dnastas data."""

    examples = [
        {
            "data": {
                "positive": ['{"Owner":"ItsoAzureSupport@shell.com"}'],
                "neg": ['{"SoldToCode":"10000002"}'],
                "neg_empty_value": ['{"SoldToCode":"10000002","Owner":""}'],
            },

            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": False,
                    "in": {"column": "positive"},
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": False,
                    "in": {"column": "neg"},
                    "out": {
                        "success": False,
                    },
                },
            ],
        }
    ]

    map_metric = "tags.owner"

    success_keys = ("mostly",)

    default_kwarg_values = {}

    def validate_configuration(
            self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:
        """
        Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that
        necessary configuration arguments have been provided for the validation of the expectation.

        Args:
            configuration (OPTIONAL[ExpectationConfiguration]): \
                An optional Expectation Configuration entry that will be used to configure the expectation
        Returns:
            None. Raises InvalidExpectationConfigurationError if the config is not validated successfully
        """

        super().validate_configuration(configuration)
        configuration = configuration or self.configuration
