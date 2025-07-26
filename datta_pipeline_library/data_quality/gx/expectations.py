"""
expectations configuration
"""
'''from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.data_context import BaseDataContext
import logging


def add_expect_column_values_to_not_be_null(columns: list[tuple[str, dict]]) -> list[ExpectationConfiguration]:
    """
    Check column values are not null for specified columns

    @param columns: List of Columns
    @return: List of Expectation Configuration
    """
    expectations = []

    def get_content():
        """
        ???
        @return: ?
        """
        if 'mostly' in kwargs.keys():
            return f"Values from the column '{c}' should not be null {kwargs['mostly'] * 100}% of the time."
        else:
            return f"Values from the column '{c}' should not be null."

    for c, kwargs in columns:
        if type(kwargs) is not dict:
            logging.warn(
                f"kwargs are not type of dict. This expectation will be skipped for column '{c}'")
            continue
        elif not c:
            logging.warn(
                f"column name is a required value. This expectation will be skipped for column '{c}'")
            continue
        elif 'mostly' in kwargs.keys() and not (kwargs['mostly'] > 0.00 or kwargs['mostly'] < 1.00):
            logging.warn(
                f"value of 'mostly' should be between 0.0 - 1.0. This expectation will be skipped for column '{c}'")
            continue
        else:
            kwargs['column'] = c
            expectation_configuration = ExpectationConfiguration(

                expectation_type="expect_column_values_to_not_be_null",
                kwargs=kwargs,
                meta={
                    "notes": {
                        "format": "markdown",
                        "content": get_content()
                    },
                    "author": {
                        "name": "Databricks CoE",
                        "email": "GX-PT-IDA-DatabricksCoE@shell.com"
                    }
                }
            )
            expectations.append(expectation_configuration)
    return expectations


def add_expect_column_values_to_be_in_set(columns: list[tuple[str, dict]]) -> list[ExpectationConfiguration]:
    """
        Returns list of ExpectationConfiguration for the given column names

        Args:

        columns (list[tuple[str, dict]]): List of cilumn names that you would like apply this validation

            tuple[str, dict]:
            In this tuple first parameter is a column name 
            second parameter is kwargs(key word arguments) to the underlying expectation
            example of kwargs :
                {'value_set':['dev', 'prod', 'uat'], 'mostly':0.99} 
                above example checks values from the give column falls in ['dev', 'prod', 'uat'] 99% of the time.

        Examples:
            >>> add_expect_column_values_to_be_in_set(context, azure_subscriptions_suite, [('location',
            {'value_set':["north", "west]}),
            ('type', {'value_set': ["virtual_machine", "app_service"],'mostly': 0.95})])

        Returns:
        `list[ExpectationConfiguration]`

    """
    expectations = []

    # TODO: refactor/ find a better way to define err messages

    def get_content():
        """
        ???
        @rtype: ???
        """
        if 'mostly' in kwargs.keys():
            return f"Values from the column '{c}' should fall in a set {str(kwargs['value_set'])} " \
                   f"{kwargs['mostly'] * 100}% of the time."
        else:
            return f"Values from the column '{c}' should fall in a set {str(kwargs['value_set'])}"

    for c, kwargs in columns:
        if type(kwargs) is not dict:
            logging.warn(
                f"kwargs are not type of dict. This expectation will be skipped for column '{c}'")
            continue
        elif not c:
            logging.warn(
                f"column name is a required value. This expectation will be skipped for column '{c}'")
            continue
        elif 'value_set' not in kwargs.keys():
            logging.warn(
                f"'value_set' is a required kwarg. This expectation will be skipped for column '{c}'")
            continue
        else:
            kwargs['column'] = c
            expectation_configuration = ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs=kwargs,
                meta={
                    "notes": {
                        "format": "markdown",
                        "content": get_content()
                    },
                    "author": {
                        "name": "Databricks CoE",
                        "email": "GX-PT-IDA-DatabricksCoE@shell.com"
                    }
                }
            )
            expectations.append(expectation_configuration)
    return expectations
'''