"""
Expectations for Cost Management
"""

from datta_pipeline_library.data_quality.gx.expectations \
    import add_expect_column_values_to_not_be_null, add_expect_column_values_to_be_in_set
from great_expectations.core.expectation_configuration import ExpectationConfiguration

expectations = []

expectations.extend(add_expect_column_values_to_not_be_null([
    ('id', {}),
    ('name', {}),
    ('sku', {'mostly': 0.1}),
    ('properties', {})
]))

expectations.extend(add_expect_column_values_to_be_in_set([
    ('type', {
        'value_set': ['Microsoft.CostManagement/query']
    })
]))
