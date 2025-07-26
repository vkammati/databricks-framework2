"""
Expectations for Azure Subscriptions
"""

from datta_pipeline_library.data_quality.gx.expectations \
    import add_expect_column_values_to_not_be_null, add_expect_column_values_to_be_in_set
from great_expectations.core.expectation_configuration import ExpectationConfiguration

expectations = []

expectations.extend(add_expect_column_values_to_not_be_null([
    ('id', {}),
    ('subscriptionId', {}),
    ('tags', {'mostly': 0.92})
]))

expectations.extend(add_expect_column_values_to_be_in_set([
    ('location', {
        'value_set': ['eastasia', 'northeurope', 'eastus2', 'westeurope']
    })
]))

expectations.extend(add_expect_column_values_to_be_in_set([
    ('state', {
        'value_set': ['Enabled', 'Disabled']
    })
]))
