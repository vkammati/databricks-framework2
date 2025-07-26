"""
Helpers for all ADLS functions
"""
from datta_pipeline_library.helpers.utils import assert_env, assert_layer
from datta_pipeline_library.helpers.spn import AzureSPN
from datta_pipeline_library.core.spark_init import spark


def get_landing_path(project_name: str, env: str, is_confidential: bool) -> str:
    """Get path in landing container.
    
    :param project_name: project name
    :param env: environment in which code is deployed, either "dev", "tst", "pre", or "prd"
    :param is_confidential: True if this is confidential data, False otherwise
    :return: path where data must be stored in the landing container
    """
    assert_env(env)
    confidentiality_prefix = "confidential" if is_confidential else "non_confidential"
    return f"{confidentiality_prefix}/{project_name}_{env}"


def get_raw_path(app_name: str, env: str) -> str:
    """Get path in raw container.
    
    :param app_name: application name
    :param env: environment in which code is deployed, either "dev", "tst", "pre", or "prd"
    :return: path where data must be stored in the raw container
    """
    assert_env(env)
    return f"{app_name}_{env}"


def get_euh_path(app_name: str, env: str) -> str:
    """Get path in enriched unharmonized container.
    
    :param app_name: application name
    :param env: environment in which code is deployed, either "dev", "tst", "pre", or "prd"
    :return: path where data must be stored in the enriched unharmonized container
    """
    assert_env(env)
    return f"{app_name}_{env}"


def get_eh_path(data_governance_domain: str, data_area: str, env: str) -> str:
    """Get path in enriched harmonized container.
    
    :param data_governance_domain: data governance domain name
    :param data_area: data area name
    :param env: environment in which code is deployed, either "dev", "tst", "pre", or "prd"
    :return: path where data must be stored in the enriched harmonized container
    """
    assert_env(env)
    return f"{data_governance_domain}/{data_area}_{env}"


def get_curated_path(use_case: str, env: str) -> str:
    """Get path in curated container.
    
    :param use_case: use case name
    :param env: environment in which code is deployed, either "dev", "tst", "pre", or "prd"
    :return: path where data must be stored in the curated container
    """
    assert_env(env)
    return f"{use_case}_{env}"


def get_container_url(container: str, storage_account: str, deltalake_container: str) -> str:
    """Get ADLS Gen2 container url.
    
    :param container: container name
    :param storage_account: storage account name
    :return ADLS Gen2 container url
    """
    assert_layer(container)
    if container =="landing":
        return f"abfss://{container}@{storage_account}.dfs.core.windows.net"
    else:
        return f"abfss://{deltalake_container}@{storage_account}.dfs.core.windows.net/{container}"


# TODO: add unit test
def configure_spark_to_use_spn_to_write_to_adls_gen2(storage_account: str, spn: AzureSPN):
    """Configure Spark to use an SPN to write to an ADLS Gen2 storage container.
    
    :param storage_account: ADLS Gen2 storage account
    :param spn: Azure SPN used to read data from the ADLS Gen2 storage account
    """
    class_obj = "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
    tenant_url = f"https://login.microsoftonline.com/{spn.tenant_id}/oauth2/token"
    conn_str_pfx = "fs.azure.account"
    conn_str_sfx = "dfs.core.windows.net"

    spark.conf.set(f"{conn_str_pfx}.auth.type.{storage_account}.{conn_str_sfx}", "OAuth")
    spark.conf.set(f"{conn_str_pfx}.oauth.provider.type.{storage_account}.{conn_str_sfx}", class_obj)
    spark.conf.set(f"{conn_str_pfx}.oauth2.client.id.{storage_account}.{conn_str_sfx}", spn.spn_client_id)
    spark.conf.set(f"{conn_str_pfx}.oauth2.client.secret.{storage_account}.{conn_str_sfx}", spn.spn_client_secret)
    spark.conf.set(f"{conn_str_pfx}.oauth2.client.endpoint.{storage_account}.{conn_str_sfx}", tenant_url)
    spark.conf.set(f"spark.databricks.dataLineage.enabled", "true")
