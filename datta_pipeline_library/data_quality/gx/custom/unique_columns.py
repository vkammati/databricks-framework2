"""
Custom Expectation for validating owner
"""

from typing import Dict, Optional

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.execution_engine import ExecutionEngine, SparkDFExecutionEngine
from great_expectations.expectations.expectation import TableExpectation
from great_expectations.expectations.metrics.metric_provider import metric_value
from great_expectations.expectations.metrics.table_metric_provider import (
    TableMetricProvider,
)
from great_expectations.validator.metric_configuration import MetricConfiguration

METRIC_NAME = "table.columns.unique"
TABLE_COLUMNS = "table.columns"


class TableColumnsUnique(TableMetricProvider):
    """ Table Column Unique Custom Expectation """
    metric_name = METRIC_NAME

    @metric_value(engine=SparkDFExecutionEngine)
    def _spark(
            cls,
            execution_engine,
            metric_domain_kwargs,
            metric_value_kwargs,
            metrics,
            runtime_configuration,
    ):
        df, _, _ = execution_engine.get_compute_domain(
            metric_domain_kwargs, domain_type=MetricDomainTypes.TABLE
        )
        return set(df.columns)

    @classmethod
    def _get_evaluation_dependencies(
            cls,
            metric: MetricConfiguration,
            configuration: Optional[ExpectationConfiguration] = None,
            execution_engine: Optional[ExecutionEngine] = None,
            runtime_configuration: Optional[dict] = None,
    ):
        return {
            TABLE_COLUMNS: MetricConfiguration(
                TABLE_COLUMNS, metric.metric_domain_kwargs
            ),
        }


class ExpectTableColumnsToBeUnique(TableExpectation):
    """Expect table to contain columns with unique contents."""

    examples = [
        {
            "data": {
                "col1": [1, 2, 3, 4, 5],
                "col2": [2, 3, 4, 5, 6],
                "col3": [3, 4, 5, 6, 7],
            },
            "tests": [
                {
                    "title": "strict_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"strict": True},
                    "out": {"success": True},
                }
            ],
        },
        {
            "data": {
                "col1": [1, 2, 3, 4, 5],
                "col2": [1, 2, 3, 4, 5],
                "col3": [3, 4, 5, 6, 7],
            },
            "tests": [
                {
                    "title": "loose_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"strict": False},
                    "out": {"success": True},
                },
                {
                    "title": "strict_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"strict": True},
                    "out": {"success": False},
                },
            ],
        },
    ]

    metric_dependencies = (METRIC_NAME, TABLE_COLUMNS)

    # This a tuple of parameter names that can affect whether the Expectation evaluates to True or False.
    success_keys = ("strict",)

    # This dictionary contains default values for any parameters that should have default values.
    default_kwarg_values = {"strict": True}

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

        strict = configuration.kwargs.get("strict")

        # Check other things in configuration.kwargs and raise Exceptions if needed
        try:
            assert (
                    isinstance(strict, bool) or strict is None
            ), "strict must be a boolean value"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

    def _validate(
            self,
            configuration: ExpectationConfiguration,
            metrics: Dict,
            runtime_configuration: dict = None,
            execution_engine: ExecutionEngine = None,
    ):
        unique_columns = metrics.get(METRIC_NAME)
        table_columns = metrics.get(TABLE_COLUMNS)
        strict = configuration.kwargs.get("strict")

        duplicate_columns = unique_columns.symmetric_difference(table_columns)

        if strict is True:
            success = len(duplicate_columns) == 0
        else:
            success = len(duplicate_columns) < len(table_columns)

        return {
            "success": success,
            "result": {"observed_value": {"duplicate_columns": duplicate_columns}},
        }

    library_metadata = {
        "tags": ["uniqueness"]
    }
