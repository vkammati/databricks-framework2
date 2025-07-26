"""
Helpers of UC
"""
from datta_pipeline_library.helpers.utils import assert_env


def get_catalog_name(business_domain: str, env: str) -> str:
    """Get Unity Catalog's catalog name.
    
    :param business_domain: the business domain name
    :param env: environment in which code is deployed, either "dev", "tst", "pre", or "prd"
    :return: UC's catalog name
    """
    assert_env(env)
    return f"{business_domain}-unitycatalog-{env}"


def get_raw_schema_name(app_name: str) -> str:
    """Get the raw schema name.
    
    :param app_name: the application name
    :return: the raw schema name
    """
    return f"raw-{app_name}"
    

def get_euh_schema_name(app_name: str) -> str:
    """Get the enriched-unharmonized schema name.
    
    :param app_name: the application name
    :return: the enriched-unharmonized schema name
    """
    return f"euh-{app_name}"
    

def get_eh_schema_name(data_governance_domain: str, data_area: str) -> str:
    """Get the enriched-harmonized schema name.
    
    :param data_governance_domain: data governance domain name
    :param data_area: data area name
    :return: the enriched-harmonized schema name
    """
    return f"eh-{data_governance_domain}-{data_area}"


def get_curated_schema_name(use_case: str) -> str:
    """Get the curated schema name.
    
    :param use_case: the use case name
    :return: the curated schema name
    """
    return f"curated-{use_case}"
