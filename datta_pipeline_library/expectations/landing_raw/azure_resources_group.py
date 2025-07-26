"""
Expectations for Azure Resources Group
"""

from datta_pipeline_library.data_quality.gx.expectations \
    import add_expect_column_values_to_not_be_null, add_expect_column_values_to_be_in_set
from great_expectations.core.expectation_configuration import ExpectationConfiguration

expectations = []

expectations.extend(add_expect_column_values_to_not_be_null([
    ('id', {}),
    ('location', {}),
    ('type', {'mostly': 0.1}),
    ('tags', {'mostly': 0.92})
]))

expectations.extend(add_expect_column_values_to_be_in_set([
    ('location', {
        'value_set': ['eastasia', 'northeurope', 'eastus2', 'westeurope']
    })
]))
