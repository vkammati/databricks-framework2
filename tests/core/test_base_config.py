from datta_pipeline_library.core.base_config import BaseConfig

BASE_CONFIG_1 = {
    "env": "tst",
    "spn_client_id": "my-spn-client-id",
    "spn_client_secret": "my-spn-client-secret",
    "tenant_id": "my-tenant-id",
    "edc_user_id": "my-edc-user-id",
    "edc_user_pwd": "my-edc-user-name",
    "azure_connection_string": "my-azure-conn-string",
    "json_string": {
        "key_1": "value_1"
    },
    "storage_account": "azdna312eunadlslifwiyswa",
    "catalog_prefix": "cross_ds",
    "project_name": "adf",
    "aecorsoft_project_name": "aecorsoft",
    "datta_subfolder_name":"DS_HANA_CDD",
    "aecorsoft_datta_subfolder_name":"GSAP_ECC_C94",
    "bw_datta_subfolder_name":"DS_GSAP_BW",
    "app_name": "ds",
    "data_governance_domain": "crossds",
    "data_area": "customer_finance_product_asset_cp",
    "data_topology_group": "DS-CrossDS",
    "use_case": "fcb",
    "business_domain": "CROSS_DS",
    "access_grp": {
        "tbl_owner_grp": "owner_grp",
        "tbl_read_grp": "read_grp"
    }
}


class TestGetDatasetConfig:
    def test_confidentiality(self):
        config = BaseConfig(**BASE_CONFIG_1)
        dataset_name = "USE_CASE_FCB_DN_SUPPLY_MARGIN"
        actual_dataset_config = config.get_dataset_config(dataset_name, confidentiality=True)

        expected_dataset_config = {
            "curated_data_path": "abfss://deltalake@azdna312eunadlslifwiyswa.dfs.core.windows.net/curated/fcb_tst/USE_CASE_FCB_DN_SUPPLY_MARGIN/",
            "curated_table_name": "curated_USE_CASE_FCB_DN_SUPPLY_MARGIN"
        }

        eh_keys = ['curated_data_path', 'curated_table_name']
        actual_dataset_config_final = {key: actual_dataset_config[key] for key in eh_keys}

        assert actual_dataset_config_final == expected_dataset_config

    def test_non_confidentiality_with_unique_id(self):
        config = BaseConfig(**BASE_CONFIG_1)
        config.set_unique_id("sede-x-DATTA-FCB-CURATED-feature-fcb_curated")
        dataset_name = "USE_CASE_FCB_DN_SUPPLY_MARGIN"
        actual_dataset_config = config.get_dataset_config(dataset_name, confidentiality=False)

        expected_dataset_config = {
            "curated_data_path": "abfss://deltalake@azdna312eunadlslifwiyswa.dfs.core.windows.net/curated/fcb_tst/USE_CASE_FCB_DN_SUPPLY_MARGIN/",
            "curated_table_name": "curated_USE_CASE_FCB_DN_SUPPLY_MARGIN"
        }

        eh_keys = ['curated_data_path', 'curated_table_name']
        actual_dataset_config_final = {key: actual_dataset_config[key] for key in eh_keys}

        assert actual_dataset_config_final == expected_dataset_config


class TestUcRelatedFeatures:

    def test_get_tbl_owner_grp(self):
        config = BaseConfig(**BASE_CONFIG_1)
        actual = config.get_tbl_owner_grp()
        expected = "owner_grp"
        assert actual == expected

    def test_get_tbl_read_grp(self):
        config = BaseConfig(**BASE_CONFIG_1)
        actual = config.get_tbl_read_grp()
        expected = "read_grp"
        assert actual == expected

    def test_get_uc_catalog_name_without_unique_id(self):
        config = BaseConfig(**BASE_CONFIG_1)

        actual_catalog = config.get_uc_catalog_name()
        expected_catalog = "cross_ds-unitycatalog-tst"
        assert actual_catalog == expected_catalog

    def test_get_uc_catalog_name_with_unique_id(self):
        config = BaseConfig(**BASE_CONFIG_1)
        config.set_unique_id("sede-x-DATTA-FCB-CURATED-feature-fcb_curated")

        actual_catalog = config.get_uc_catalog_name()
        expected_catalog = "cross_ds-unitycatalog-tst"
        assert actual_catalog == expected_catalog

    def test_format_uc_name(self):
        config = BaseConfig(**BASE_CONFIG_1)

        assert config.format_uc_name("Catalog") == "catalog"
        assert config.format_uc_name("CATALOG") == "catalog"
        assert config.format_uc_name("catalog") == "catalog"
        assert config.format_uc_name("Release-v0.0.1") == "release-v0_0_1"

    def test_get_uc_raw_schema(self):
        config = BaseConfig(**BASE_CONFIG_1)
        actual_uc_raw_schema = config.get_uc_raw_schema()
        expected_uc_raw_schema = "raw-ds"
        assert actual_uc_raw_schema == expected_uc_raw_schema

    def test_get_uc_raw_schema_with_unique_id(self):
        config = BaseConfig(**BASE_CONFIG_1)
        config.set_unique_id_schema("sede-x-DATTA-FCB-CURATED-feature-fcb_curated")
        actual_uc_raw_schema = config.get_uc_raw_schema()
        expected_uc_raw_schema = "raw-ds-sede-x-datta-fcb-curated-feature-fcb_curated"
        assert actual_uc_raw_schema == expected_uc_raw_schema

    def test_get_uc_euh_schema(self):
        config = BaseConfig(**BASE_CONFIG_1)
        actual_uc_euh_schema = config.get_uc_euh_schema()
        expected_uc_euh_schema = "euh-ds"
        assert actual_uc_euh_schema == expected_uc_euh_schema

    def test_get_uc_euh_schema_with_unique_id(self):
        config = BaseConfig(**BASE_CONFIG_1)
        config.set_unique_id_schema("sede-x-DATTA-FCB-CURATED-feature-fcb_curated")
        actual_uc_euh_schema = config.get_uc_euh_schema()
        expected_uc_euh_schema = "euh-ds-sede-x-datta-fcb-curated-feature-fcb_curated"
        assert actual_uc_euh_schema == expected_uc_euh_schema

    def test_get_uc_eh_schema(self):
        config = BaseConfig(**BASE_CONFIG_1)
        actual_uc_eh_schema = config.get_uc_eh_schema()
        expected_uc_eh_schema = "eh-crossds-customer_finance_product_asset_cp"
        assert actual_uc_eh_schema == expected_uc_eh_schema

    def test_get_uc_eh_schema_with_unique_id(self):
        config = BaseConfig(**BASE_CONFIG_1)
        config.set_unique_id_schema("sede-x-DATTA-FCB-CURATED-feature-fcb_curated")
        actual_uc_eh_schema = config.get_uc_eh_schema()
        expected_uc_eh_schema = "eh-crossds-customer_finance_product_asset_cp-sede-x-datta-fcb-curated-feature-fcb_curated"
        assert actual_uc_eh_schema == expected_uc_eh_schema

    def test_get_uc_curated_schema(self):
        config = BaseConfig(**BASE_CONFIG_1)
        actual_uc_curated_schema = config.get_uc_curated_schema()
        expected_uc_curated_schema = "curated-fcb"
        assert actual_uc_curated_schema == expected_uc_curated_schema

    def test_get_uc_curated_schema_with_unique_id(self):
        config = BaseConfig(**BASE_CONFIG_1)
        config.set_unique_id_schema("sede-x-DATTA-FCB-CURATED-feature-fcb_curated")
        actual_uc_curated_schema = config.get_uc_curated_schema()
        expected_uc_curated_schema = "curated-fcb-sede-x-datta-fcb-curated-feature-fcb_curated"
        assert actual_uc_curated_schema == expected_uc_curated_schema