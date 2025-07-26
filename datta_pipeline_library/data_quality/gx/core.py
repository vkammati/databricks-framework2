"""
Great Expectations Core
"""
'''from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    DataContextConfig,
    DatasourceConfig,
)

from datta_pipeline_library.data_quality.gx import constants
import logging


def init_gx(target_container_name: str,
            metadata_path: str,
            data_docs_path: str,
            storage_connection_string: str) -> BaseDataContext:
    """

        Standardized function to initialize great expectations for EDPL.
        :param:target_container_name (str): Name of the container where data is being moved.
        :param:application_prefix (str): Your data set's application prefix in ADLS.
                    example - ms_azure_api, xyz_sql_database_name
        :param:dataset_name (str): Name of your dataset. example - azure_resources, cost_management
        :param:storage_connection_string (str): Name of secret scope where we are storing great expectations metadata.
        :example:
            >>> ge_context = init_gx(target_container, application_prefix, dataset_name, geio_storage_connection_string)
        :return:`great_expectations.data_context.BaseDataContext`
    """

    if not storage_connection_string:
        raise ValueError(
            "Sorage account connection string should not be null.")
    try:
        return __init_ge_project(target_container_name,
                                 metadata_path,
                                 data_docs_path,
                                 storage_connection_string)
    except Exception as e:
        raise ValueError(str(e))


def get_spark_runtime_datasource():
    """

    @return: Json Object containing Data Source Config
    """
    return {
        "spark_autoloader_batch": DatasourceConfig(
            class_name="Datasource",
            execution_engine={
                "class_name": "SparkDFExecutionEngine"
            },
            data_connectors={
                "default_runtime_data_connector_name": {
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["default_identifier"],
                },
            }
        )
    }


def get_az_backend(target_container_name: str,
                   ge_metadata_prefix: str,
                   storage_connection_string: str,
                   store_suffix: str):
    """

    @param target_container_name: target path
    @param ge_metadata_prefix: metadat prefix
    @param storage_connection_string: Storage connection string
    @param store_suffix: Suffix
    @return: Azure Backend configuration
    """
    return {
        "class_name": "TupleAzureBlobStoreBackend",
        "container": target_container_name,
        "prefix": f"{ge_metadata_prefix}/{store_suffix}",
        "connection_string": storage_connection_string
    }


def __init_ge_project(target_container_name: str,
                      ge_metadata_prefix: str,
                      ge_data_docs_path: str,
                      storage_connection_string: str
                      ) -> BaseDataContext:
    """
        Private method to hide creating BaseContext
        :param:target_container_name (str): Name of the container where data is being moved.
        :param:application_prefix (str): Your data set's application prefix in ADLS.
                    example - ms_azure_api, xyz_sql_database_name
        :param:dataset_name (str): Name of your dataset. example - azure_resources, cost_management
        :param:storage_connection_string (str): Name of secret scope where
        we are storing great expectations metadata.
        :example:
            >>> ge_context = init_gx(target_container, application_prefix, dataset_name, geio_storage_connection_string)
        :return:`great_expectations.data_context.BaseDataContext`
    """

    data_context_config = DataContextConfig(
        datasources=get_spark_runtime_datasource(),
        stores={
            "expectations_az_store": {
                "class_name": "ExpectationsStore",
                "store_backend": get_az_backend(target_container_name,
                                                ge_metadata_prefix,
                                                storage_connection_string,
                                                "expectations")
            },
            "validations_az_store": {
                "class_name": "ValidationsStore",
                "store_backend": get_az_backend(target_container_name,
                                                ge_metadata_prefix,
                                                storage_connection_string,
                                                "validations"),
            },
            "checkpoints_az_store": {
                "class_name": "CheckpointStore",
                "store_backend": get_az_backend(target_container_name,
                                                ge_metadata_prefix,
                                                storage_connection_string,
                                                "checkpoints"),
            },
            "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
        },
        expectations_store_name="expectations_az_store",
        validations_store_name="validations_az_store",
        checkpoint_store_name="checkpoints_az_store",
        evaluation_parameter_store_name="evaluation_parameter_store",
        data_docs_sites={
            "az_site": {
                "class_name": "SiteBuilder",
                "store_backend": {
                    "class_name": "TupleAzureBlobStoreBackend",
                    "container": constants.GE_DOC_CONTAINER_NAME,
                    "prefix": ge_data_docs_path,
                    "connection_string": storage_connection_string
                },
                "site_index_builder": {
                    "class_name": "DefaultSiteIndexBuilder",
                    "show_cta_footer": False,
                },
                "show_how_to_buttons": False
            }
        },
        anonymous_usage_statistics={
            "enabled": False
        }
    )

    logging.info(f"""Initialized great expectations with following paths:\n
            target container name - {target_container_name}
            ge prefix - {ge_metadata_prefix}
            ge data docs path - {ge_data_docs_path}""")

    return BaseDataContext(project_config=data_context_config)


def get_ge_batch(ge_context, dataset_name, expectation_suite_name, dataframe):
    """
        Creates checkpoint and returns great expectations runtime batch using default data source.
        :param:ge_context: Great expectations context
        :param:dataset_name: Name of the daset
        :param:expectation_suite_name: Name of the expectation suit that we are validating :dataframe
        :param:dataframe: spark dataframe
        :return: (great_expectations.core.batch.RuntimeBatchRequest, checkpoint_name)
    """
    batch_request = RuntimeBatchRequest(
        datasource_name="spark_autoloader_batch",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name=dataset_name,
        batch_identifiers={"default_identifier": "default_identifier"},
        runtime_parameters={"batch_data": dataframe},
    )
    checkpoint_name = dataset_name + "_checkpoint"
    checkpoint_config = {
        "name": checkpoint_name,
        "config_version": 1,
        "run_name_template": constants.CHECKPOINT_RUN_NAME_TEMPLATE,
        "class_name": "SimpleCheckpoint",
        "expectation_suite_name": expectation_suite_name,
    }
    ge_context.add_checkpoint(**checkpoint_config)
    return batch_request, checkpoint_name


def parse_ge_validation_result(results) -> tuple:
    """
        Parses great expectations results 
        :param:results: Great expectations execution result
        :return: (success:bool, success percentage:str)
    """
    if results:
        validation_id = results.list_validation_result_identifiers()[0]
        if validation_id:
            return results.success, results.get_statistics().get('validation_statistics').get(validation_id)[
                'success_percent']
    return None, None


def configure_gx(dataset_name, expectations, metadata_path, data_docs_path, raw_container_name,
                 azure_connection_string):
    """
        Initialise great expectations, adds expectations to the expectation suite.
        :param:dataset_name: Name of the dataset
        :param:expectations: List of expectations for the given :dataset_name
        :param:metadata_path: Path to store great expectations metadata
        :param:data_docs_path: Path to store great expectations docs
        :param:raw_container_name: Name of ADLS raw container name
        :param:azure_connection_string: ADLS storage account connection string
        :return: (great expectations context, expectation suite name)
    """
    try:
        gx_context = init_gx(raw_container_name,
                             metadata_path, data_docs_path, azure_connection_string)
    except Exception as e:
        logging.error(str(e))

    expectations_suite_name = f"{dataset_name}_expectations_suite"

    logging.info(f"Initializing expectation suite for {dataset_name}.")

    expectation_suite = gx_context.create_expectation_suite(
        expectations_suite_name, overwrite_existing=True)

    for expectation in expectations:
        expectation_suite.add_expectation(expectation)

    gx_context.save_expectation_suite(expectation_suite)

    logging.info(f"Initialized expectation suite for {dataset_name}.")

    return gx_context, expectations_suite_name
'''

def execute_validation(df, dataset_name, expectations, success_percent_threshold, base_config):
    """Executes great expectations against provided dataset.
    
    It reads delta table and performs validation on the returned data.

    :param df: ?
    :param dataset_name: name of the dataset
    :param expectations: ?
    :param success_percent_threshold: success percent threshold that needs to pass in order to conclude validation.
    :param base_config: BaseConfig object
    """
    '''azure_connection_string = base_config.azure_connection_string
    dirlst = base_config.get_dataset_config(dataset_name)

    raw_container_name = dirlst['raw_container_name']
    gx_metadata_path, gx_data_dcos_path = dirlst['gx_metadata_path'], dirlst['gx_data_docs_path']

    gx_context, expectations_suite_name = configure_gx(dataset_name, expectations, gx_metadata_path,
                                                       gx_data_dcos_path, raw_container_name, azure_connection_string)

    logging.info(f"Started running data validation on {dataset_name} with suite name {expectations_suite_name}.")

    logging.info(f"Validation on batch with {df.count()} records.")

    batch_request, checkpoint_name = get_ge_batch(
        gx_context, dataset_name, expectations_suite_name, df)

    ge_results = gx_context.run_checkpoint(
        checkpoint_name=checkpoint_name,
        batch_request=batch_request,
        expectation_suite_name=expectations_suite_name
    )
    success, percent = parse_ge_validation_result(ge_results)
    if success:
        logging.info(
            f"Data validation succeeded for dataset {dataset_name} "
            f"with expectation suite {expectations_suite_name}. Success percentage - {percent} ")
    else:
        if float(percent) >= float(success_percent_threshold):
            logging.info(
                f"Data validation succeeded for dataset {dataset_name} "
                f"with expectation suite {expectations_suite_name}. Success percentage - {percent}, "
                f"configured success_percent_threshold - {success_percent_threshold}.")
        else:
            logging.info(
                f"Data validation failed for dataset {dataset_name} with expectation suite "
                f"{expectations_suite_name}. Success percentage - {percent}.")
            # TODO: get bad records once gx starts supporting unexpected_list_index
            #  https://github.com/great-expectations/great_expectations/issues/3195

    logging.info(f"Data docs built and updated here - {gx_context.build_data_docs()['az_site']}")'''
    pass