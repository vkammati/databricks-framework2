"""
Base Config contains all configuration or variables for Lakehouse Ingestion Framework
"""
import json
import logging
from datta_pipeline_library.core.spark_init import spark

from datta_pipeline_library.data_quality.gx.constants import GEIO_DATA_DOCS_FOLDER_SUFFIX, GEIO_FOLDER_SUFFIX
from datta_pipeline_library.helpers.adls import get_container_url

landing_container_name = "landing"
raw_container_name = "raw"
euh_container_name = "enriched-unharmonized"
eh_container_name = "enriched-harmonized"
curated_container_name = "curated"
deltalake_container_name =  "deltalake"

class CommonConfig:
    """ Common Config for the Lakehosue Framework """
    def __init__(
            self,
            *,
            catalog_prefix,
            project_name,
            aecorsoft_project_name,
            app_name,
            data_topology_group,
            business_domain,
            process_config_project_name,
            process_order,
            process_name,
            dlt_workflow_name,
            mars_sp_load_dlt_workflow_name,
            mars_load_dlt_workflow_name,
            **kwargs):
        self.catalog_prefix = catalog_prefix
        self.project_name = project_name
        self.aecorsoft_project_name = aecorsoft_project_name
        self.app_name = app_name
        self.data_topology_group = data_topology_group
        self.business_domain = business_domain
        self.process_config_project_name = process_config_project_name
        self.process_order = process_order
        self.process_name = process_name
        self.dlt_workflow_name = dlt_workflow_name
        self.mars_sp_load_dlt_workflow_name = mars_sp_load_dlt_workflow_name
        self.mars_load_dlt_workflow_name = mars_load_dlt_workflow_name

    @classmethod
    def from_file(cls, file_path):
        with open(file_path, "r") as f:
            conf = json.load(f)

        return cls(**conf)

    def get_as_dict(self):
        return self.__dict__


class EnvConfig:
    """ Environment specific Config for the Lakehosue Framework """
    def __init__(
            self,
            *,
            env,
            kv_key,
            spn_client_id_key,
            spn_client_secret_key,
            storage_account,
            edc_user_id_key,
            edc_user_pwd_key,
            api_url,
            security_object_aad_group,
            security_end_user_aad_group,
            security_functional_dev_aad_group,
            security_functional_readers_aad_group,
            security_security_readers_aad_group,
            security_mars_end_user_aad_group,
            access_grp,
            datta_subfolder_name,
            aecorsoft_datta_subfolder_name,
            bw_datta_subfolder_name,
            workspace_url,
            data_governance_domain,
            data_area,
            use_case,
            **kwargs):
        self.env = env
        self.kv_key = kv_key
        self.spn_client_id_key = spn_client_id_key
        self.spn_client_secret_key = spn_client_secret_key
        self.storage_account = storage_account
        self.edc_user_id_key = edc_user_id_key
        self.edc_user_pwd_key = edc_user_pwd_key
        self.api_url = api_url
        self.security_object_aad_group = security_object_aad_group
        self.security_end_user_aad_group = security_end_user_aad_group
        self.security_functional_dev_aad_group = security_functional_dev_aad_group
        self.security_functional_readers_aad_group = security_functional_readers_aad_group
        self.security_security_readers_aad_group = security_security_readers_aad_group
        self.security_mars_end_user_aad_group = security_mars_end_user_aad_group
        self.access_grp = access_grp
        self.datta_subfolder_name = datta_subfolder_name
        self.aecorsoft_datta_subfolder_name = aecorsoft_datta_subfolder_name
        self.bw_datta_subfolder_name = bw_datta_subfolder_name
        self.workspace_url = workspace_url
        self.data_governance_domain = data_governance_domain
        self.data_area = data_area
        self.use_case = use_case

    @classmethod
    def from_file(cls, file_path):
        with open(file_path, "r") as f:
            conf = json.load(f)

        return cls(**conf)

    def get_as_dict(self):
        return self.__dict__


class CollibraConfig:
    """ Collibra Specific Config for the Lakehosue Framework """
    def __init__(self, edc_user_id: str, edc_user_pwd: str, api_url: str, json_string: str = None):
        self.edc_user_id = edc_user_id
        self.edc_user_pwd = edc_user_pwd
        self.api_url = api_url
        self.json_string = json_string

    def read_json_string_from_file(self, file_path: str):
        """Set json_string parameter based on json string stored in a json file.

        :param file_path: path of the json file containing the json string
        """
        with open(file_path, "r") as f:
            json_string = json.load(f)

        self.json_string = json_string

class GreatExpectationsConfig:
    """ Great Expectations Config for the Lakehosue Framework """
    def __init__(self, azure_connection_string: str):
        self.azure_connection_string = azure_connection_string

    def get_as_dict(self):
        return self.__dict__

class BaseConfig:
    """ Base Config for the Lakehosue Framework """
    STATIC_FILE_PREFIX = "dna_static_files"

    def __init__(
            self,
            *,
            env,
            storage_account,
            catalog_prefix,
            project_name,
            aecorsoft_project_name,
            datta_subfolder_name,
            aecorsoft_datta_subfolder_name,
            bw_datta_subfolder_name,
            app_name,
            data_governance_domain,
            data_area,
            data_topology_group,
            use_case,
            business_domain,
            access_grp,
            **kwargs):
        self.env = env
        self.storage_account = storage_account
        self.catalog_prefix = catalog_prefix
        self.project_name = project_name
        self.aecorsoft_project_name = aecorsoft_project_name
        self.datta_subfolder_name=datta_subfolder_name
        self.aecorsoft_datta_subfolder_name = aecorsoft_datta_subfolder_name
        self.bw_datta_subfolder_name = bw_datta_subfolder_name
        self.app_name = app_name
        self.data_governance_domain = data_governance_domain
        self.data_area = data_area
        self.data_topology_group = data_topology_group
        self.use_case = use_case
        self.business_domain = business_domain
        self.access_grp = access_grp
        # NB: unique id used to prevent conflicts in UC and ADLS for integration tests
        self.unique_id = None
        self.unique_id_schema = None
        self.load_type= None

    @classmethod
    
    # def from_confs(cls, env_config: EnvConfig, common_config: CommonConfig, gx_config: GreatExpectationsConfig):
    #     env_config_vars = env_config.get_as_dict()
    #     common_config_vars = common_config.get_as_dict()
    #     gx_config_vars = gx_config.get_as_dict()
    #     return BaseConfig(**env_config_vars, **common_config_vars, **gx_config_vars)
    def from_confs(cls, env_config: EnvConfig, common_config: CommonConfig):
        env_config_vars = env_config.get_as_dict()
        common_config_vars = common_config.get_as_dict()
        # gx_config_vars = gx_config.get_as_dict()
        return BaseConfig(**env_config_vars, **common_config_vars)

    # TODO: add unit tests everywhere
    def get_tbl_owner_grp(self) -> str:
        return self.access_grp['tbl_owner_grp']

    def get_tbl_read_grp(self) -> str:
        return self.access_grp['tbl_read_grp']

    def set_unique_id(self, unique_id: str):
        self.unique_id = unique_id
    
    def set_unique_id_schema(self, unique_id_schema: str):
        self.unique_id_schema = unique_id_schema

    def append_unique_id(self, path: str, layer: str, sep: str) -> str:
        if layer == 'landing':
            return path if not self.unique_id else f"{path}{sep}{self.unique_id}"
        else:
            return path if not self.unique_id_schema else f"{path}{sep}{self.unique_id_schema}"
        
    def append_unique_id_to_path(self, path: str, layer: str) -> str:
        return self.append_unique_id(path, layer, sep="/")

    def append_unique_id_to_schema(self, schema: str, layer: str) -> str:
        return self.append_unique_id(schema, layer, sep="-")

    def format_uc_name(self, name: str):
        """Table, schema, and catalog names are lowercased by Unity Catalog."""
        return name.lower().replace(".", "_")

    def get_uc_catalog_name(self) -> str:
        uc_catalog = f"{self.catalog_prefix}-unitycatalog-{self.env}"
        return self.format_uc_name(uc_catalog)

    def get_uc_raw_schema(self) -> str:
        raw_schema_prefix = f"raw-{self.app_name}"
        raw_schema = self.append_unique_id_to_schema(raw_schema_prefix, 'raw')
        return self.format_uc_name(raw_schema)

    def get_uc_euh_schema(self) -> str:
        euh_schema_prefix = f"euh-{self.app_name}"
        euh_schema = self.append_unique_id_to_schema(euh_schema_prefix, 'euh')
        return self.format_uc_name(euh_schema)

    def get_uc_eh_schema(self) -> str:
        eh_schema_prefix = f"eh-{self.data_governance_domain}-{self.data_area}"
        eh_schema = self.append_unique_id_to_schema(eh_schema_prefix, 'eh')
        return self.format_uc_name(eh_schema)

    def get_uc_curated_schema(self) -> str:
        curated_schema_prefix = f"curated-{self.use_case}"
        curated_schema = self.append_unique_id_to_schema(curated_schema_prefix, 'curated')
        return self.format_uc_name(curated_schema)

    def get_landing_folder_suffix(self, confidentiality: bool = False) -> str:
        confidentiality_prefix = "confidential" if confidentiality else "internal"
        folder_path = f"{confidentiality_prefix}/{self.project_name}_{self.env}/{self.datta_subfolder_name}"
        print("base_config folder_path:::",folder_path)
        return self.append_unique_id_to_path(folder_path, 'landing')

    def get_landing_folder_path(self, confidentiality: bool = False) -> str:
        landing_container_url = get_container_url(landing_container_name, self.storage_account,deltalake_container_name)
        print("base_config landing_container_url:::",landing_container_url)
        landing_folder_suffix = self.get_landing_folder_suffix(confidentiality)
        print("base_config landing_folder_suffix:::",landing_folder_suffix)
        return f"{landing_container_url}/{landing_folder_suffix}"

    def get_aecorsoft_landing_folder_suffix(self, confidentiality: bool = False) -> str:
        aecorsoft_confidentiality_prefix = "confidential" if confidentiality else "internal"
        aecorsoft_folder_path = f"{aecorsoft_confidentiality_prefix}/{self.aecorsoft_project_name}_{self.env}/{self.aecorsoft_datta_subfolder_name}"
        print("base_config aecorsoft_folder_path:::",aecorsoft_folder_path)
        return self.append_unique_id_to_path(aecorsoft_folder_path, 'landing')

    def get_aecorsoft_landing_folder_path(self, confidentiality: bool = False) -> str:
        aecorsoft_landing_container_url = get_container_url(landing_container_name, self.storage_account,deltalake_container_name)
        aecorsoft_landing_folder_suffix = self.get_aecorsoft_landing_folder_suffix(confidentiality)
        print("base_config aecorsoft_landing_folder_suffix:::",aecorsoft_landing_folder_suffix)
        return f"{aecorsoft_landing_container_url}/{aecorsoft_landing_folder_suffix}"
    
    def get_bw_landing_folder_suffix(self, confidentiality: bool = False) -> str:
        bw_confidentiality_prefix = "confidential" if confidentiality else "internal"
        bw_folder_path = f"{bw_confidentiality_prefix}/{self.project_name}_{self.env}/{self.bw_datta_subfolder_name}"
        print("base_config bw_folder_path:::",bw_folder_path)
        return self.append_unique_id_to_path(bw_folder_path, 'landing')
    
    def get_bw_landing_folder_path(self, confidentiality: bool = False) -> str:
        bw_landing_container_url = get_container_url(landing_container_name, self.storage_account,deltalake_container_name)
        bw_landing_folder_suffix = self.get_bw_landing_folder_suffix(confidentiality)
        print("base_config bw_landing_folder_suffix:::",bw_landing_folder_suffix)
        return f"{bw_landing_container_url}/{bw_landing_folder_suffix}"    
    
    def get_raw_folder_suffix(self) -> str:
        folder_path = f"{self.app_name}_{self.env}/{self.datta_subfolder_name}"
        print("base_config raw folder_path:::",folder_path)
        return self.append_unique_id_to_path(folder_path, 'raw')

    def get_raw_folder_path(self) -> str:
        raw_container_url = get_container_url(raw_container_name, self.storage_account,deltalake_container_name)
        raw_folder_suffix = self.get_raw_folder_suffix()
        print("base_config raw_container_url:::",raw_container_url)
        print("base_config raw_folder_suffix:::",raw_folder_suffix)
        return f"{raw_container_url}/{raw_folder_suffix}"

    def get_euh_folder_suffix(self) -> str:
        folder_path = f"{self.app_name}_{self.env}"
        print("base_config euh folder_path:::",folder_path)
        return self.append_unique_id_to_path(folder_path, 'euh')

    def get_euh_folder_path(self) -> str:
        euh_container_url = get_container_url(euh_container_name, self.storage_account,deltalake_container_name)
        euh_folder_suffix = self.get_euh_folder_suffix()
        print("base_config euh_container_url:::",euh_container_url)
        print("base_config euh_folder_suffix:::",euh_folder_suffix)
        return f"{euh_container_url}/{euh_folder_suffix}"

    def get_eh_folder_suffix(self) -> str:
        folder_path = f"{self.data_governance_domain}/{self.data_area}_{self.env}"
        return self.append_unique_id_to_path(folder_path, 'eh')

    def get_eh_folder_path(self) -> str:
        eh_container_url = get_container_url(eh_container_name, self.storage_account,deltalake_container_name)
        eh_folder_suffix = self.get_eh_folder_suffix()
        return f"{eh_container_url}/{eh_folder_suffix}"

    def get_curated_folder_suffix(self) -> str:
        folder_path = f"{self.use_case}_{self.env}"
        return self.append_unique_id_to_path(folder_path, 'curated')

    def get_curated_src_folder_suffix(self) -> str: 
        folder_path = f"{self.use_case}_{self.env}"
        return self.append_unique_id_to_path(folder_path, 'curated')

    def get_curated_src_folder_path(self) -> str:
        curated_src_container_url = get_container_url(curated_container_name, self.storage_account,deltalake_container_name)
        curated_src_folder_suffix = self.get_curated_src_folder_suffix()
        return f"{curated_src_container_url}/{curated_src_folder_suffix}"
    
    def get_curated_folder_path(self) -> str:
        curated_container_url = get_container_url(curated_container_name, self.storage_account,deltalake_container_name)
        curated_folder_suffix = self.get_curated_folder_suffix()
        return f"{curated_container_url}/{curated_folder_suffix}"

    def gx_prefix_path(self) -> str:
        prefix_path = self.app_name + "_" + self.env
        return self.append_unique_id_to_path(prefix_path, 'curated')

    def set_load_type(self, load_type: str):
        self.load_type = load_type

    def get_load_type(self) -> str:
        return self.load_type
    
    def get_dataset_config(self, dataset_name: str, confidentiality: bool = False) -> dict:
        """Get a dataset config.
        
        :param dataset_name: dataset name
        :param confidentiality: if True, sets the landing folder prefix to "confidential",
        else to "internal"
        :return: dictionary with all paths and table name configurations
        """
        landing_folder_path = self.get_landing_folder_path(confidentiality) 
        print("core/base_config.py get_dataset_config landing_folder_path:", landing_folder_path)
        aecorsoft_landing_folder_path = self.get_aecorsoft_landing_folder_path(confidentiality)
        print("core/base_config.py get_dataset_config aecorsoft_landing_folder_path:", aecorsoft_landing_folder_path)
        bw_landing_folder_path = self.get_bw_landing_folder_path(confidentiality)
        print("core/base_config.py get_dataset_config bw_landing_folder_path:", bw_landing_folder_path)

        raw_folder_path = self.get_raw_folder_path()
        print("core/base_config.py get_dataset_config raw_folder_path:", raw_folder_path)
        euh_folder_path = self.get_euh_folder_path()
        print("core/base_config.py get_dataset_config euh_folder_path:", euh_folder_path)
        eh_folder_path = self.get_eh_folder_path()
        curated_folder_path = self.get_curated_folder_path()
        curated_src_folder_path = self.get_curated_src_folder_path()
        aecorsoft_raw_source_name="Aecorsoft"
        load_type = self.get_load_type()

        dir_dict = {
            "landing_view_name": f"view_{dataset_name}",
            "aecorsoft_landing_view_name": f"aecorsoft_view_{dataset_name}",
            "bw_landing_view_name": f"bw_view_{dataset_name}",
            "landing_data_path": f"{landing_folder_path}/{dataset_name}/{dataset_name}_{load_type}",
            "aecorsoft_landing_data_path": f"{aecorsoft_landing_folder_path}/{dataset_name}/{dataset_name}_{load_type}",  
            "bw_landing_data_path": f"{bw_landing_folder_path}/{dataset_name}",           
            "raw_container_name": raw_container_name,
            "raw_table_name": f"raw_{dataset_name}",
            "raw_data_path": f"{raw_folder_path}/{dataset_name}/",
            "aecorsoft_raw_data_path": f"{raw_folder_path}/{aecorsoft_raw_source_name}_{dataset_name}/",
            "euh_table_name": f"euh_{dataset_name}",
            "euh_data_path": f"{euh_folder_path}/{dataset_name}/",
            "eh_table_name": f"eh_{dataset_name}",
            "eh_data_path": f"{eh_folder_path}/{dataset_name}/",
            "curated_table_name": f"curated_{dataset_name}",
            "curated_data_path": f"{curated_folder_path}/{dataset_name}/",
            "curated_src_table_name": f"curated_{dataset_name}",
            "curated_src_data_path": f"{curated_src_folder_path}/{dataset_name}/",
            "gx_metadata_path": f"{self.gx_prefix_path()}/{dataset_name}_{GEIO_FOLDER_SUFFIX}",
            "gx_data_docs_path": f"{self.gx_prefix_path()}/{dataset_name}/{GEIO_DATA_DOCS_FOLDER_SUFFIX}",
            # TODO: use this instead
            #gx_raw_metadata_path = f"{application_prefix}/
            # {dataset_name}_{constants.GEIO_FOLDER_SUFFIX}",
            #gx_data_docs_path = f"{confidentiality_prefix}/{application_prefix}
            # /{dataset_name}/{constants.GEIO_DATA_DOCS_FOLDER_SUFFIX}"
        }

        logging.info(f"Config Details: {dir_dict}")
        return dir_dict
