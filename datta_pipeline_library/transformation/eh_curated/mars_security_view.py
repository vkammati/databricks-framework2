# Databricks notebook source
pip install msal

# COMMAND ----------

pip install adal

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from datta_pipeline_library.core.spark_init import spark
from datta_pipeline_library.core.base_config import (
    BaseConfig,
    CommonConfig,
    EnvConfig
)
from datta_pipeline_library.helpers.adls import configure_spark_to_use_spn_to_write_to_adls_gen2
from datta_pipeline_library.helpers.spn import AzureSPN
from datta_pipeline_library.helpers.uc import (
    get_catalog_name
)

# COMMAND ----------

env = dbutils.widgets.get(name="env")
repos_path = dbutils.widgets.get(name="repos_path")
unique_repo_branch_id = dbutils.widgets.get(name="unique_repo_branch_id")
unique_repo_branch_id_schema = dbutils.widgets.get(name="unique_repo_branch_id_schema")
mars_data_dump_output_view_name = dbutils.widgets.get(name="mars_data_dump_output_view_name")
margin_bucket_collection_view_name = dbutils.widgets.get(name="margin_bucket_collection_view_name")
margin_bucket_collection_table_name = dbutils.widgets.get(name="margin_bucket_collection_table_name")
mars_data_dump_output_table_name = dbutils.widgets.get(name="mars_data_dump_output_table_name")
security_schema = dbutils.widgets.get(name="security_schema")
security_function_name = dbutils.widgets.get(name="security_function_name")

common_conf = CommonConfig.from_file(f"/Workspace/{repos_path.strip('/')}/conf/common/common_conf.json")
env_conf = EnvConfig.from_file(f"/Workspace/{repos_path.strip('/')}/conf/{env}/conf.json")

base_config = BaseConfig.from_confs(env_conf, common_conf)
if unique_repo_branch_id:
    base_config.set_unique_id(unique_repo_branch_id)
if unique_repo_branch_id_schema:
    base_config.set_unique_id_schema(unique_repo_branch_id_schema)

# COMMAND ----------

kv = env_conf.kv_key

# values from key vault
tenant_id = dbutils.secrets.get(scope=kv, key="AZ-AS-SPN-DATTA-TENANT-ID")
spn_client_id = dbutils.secrets.get(scope=kv, key=env_conf.spn_client_id_key)
spn_client_secret = dbutils.secrets.get(scope=kv, key=env_conf.spn_client_secret_key)

spn = AzureSPN(tenant_id, spn_client_id, spn_client_secret)

configure_spark_to_use_spn_to_write_to_adls_gen2(env_conf.storage_account, spn)

# COMMAND ----------

uc_catalog_name = base_config.get_uc_catalog_name()
print("uc_catalog_name : ",uc_catalog_name)

uc_curated_schema = base_config.get_uc_curated_schema()
print("uc_curated_schema : ", uc_curated_schema)

uc_raw_schema = base_config.get_uc_raw_schema()
print("uc_raw_schema : ", uc_raw_schema)

tbl_owner_grp = base_config.get_tbl_owner_grp()
print("tbl_owner_grp : ",tbl_owner_grp)

tbl_read_grp = base_config.get_tbl_read_grp()
print("tbl_read_grp : ",tbl_owner_grp)

security_object_aad_group_name = env_conf.security_object_aad_group
print("security object aad group name: ", security_object_aad_group_name)

security_end_user_aad_group_name = env_conf.security_mars_end_user_aad_group
print("security end user aad group name: ", security_end_user_aad_group_name)

security_functional_dev_aad_group = env_conf.security_functional_dev_aad_group
print("security functional dev aad group name: ", security_functional_dev_aad_group)

security_functional_readers_aad_group = env_conf.security_functional_readers_aad_group
print("security functional readers aad group name: ", security_functional_readers_aad_group)

security_security_readers_aad_group = env_conf.security_security_readers_aad_group
print("security security readers aad group name: ", security_security_readers_aad_group)

# COMMAND ----------

spark.sql(f"""GRANT USE SCHEMA ON SCHEMA `{uc_catalog_name}`.`{uc_curated_schema}` TO `{security_end_user_aad_group_name}`""")
print("USE SCHEMA granted to ", security_end_user_aad_group_name)

# COMMAND ----------

# assignViewPermission: This function assigns Permission to the view created
def assignViewPermission(catalog,schema,view_name,tbl_owner, security_end_user_aad_group, security_object_aad_group, security_functional_readers_aad_group, security_functional_dev_aad_group):
    spark.sql(f"""GRANT ALL PRIVILEGES ON VIEW `{catalog}`.`{schema}`.{view_name} TO `{tbl_owner}`""")
    print("All privileges access given to tbl owner", tbl_owner)
    spark.sql(f"""GRANT ALL PRIVILEGES ON VIEW `{catalog}`.`{schema}`.{view_name} TO `{security_functional_dev_aad_group}`""")
    print("All privileges access given to functional developers", security_functional_dev_aad_group)
    spark.sql(f"""GRANT SELECT ON VIEW `{catalog}`.`{schema}`.{view_name} TO `{security_end_user_aad_group}`""")
    print("Reader access granted to ", security_end_user_aad_group)
    spark.sql(f"""GRANT SELECT ON VIEW `{catalog}`.`{schema}`.{view_name} TO `{security_functional_readers_aad_group}`""")
    print("Reader access granted to ", security_functional_readers_aad_group)
    spark.sql(f"""ALTER VIEW `{catalog}`.`{schema}`.{view_name} owner to `{security_object_aad_group}`""")
    print("Table Owner is assigned to ", security_object_aad_group)

# COMMAND ----------

# DBTITLE 1,mars_data_dump_output_view creation
use_catalog = "USE CATALOG `"+uc_catalog_name+"`"
spark.sql(use_catalog)
spark.sql(f"""DROP VIEW IF EXISTS `{uc_catalog_name}`.`{uc_curated_schema}`.{mars_data_dump_output_view_name}""")
spark.sql(f"""CREATE OR REPLACE VIEW `{uc_catalog_name}`.`{uc_curated_schema}`.{mars_data_dump_output_view_name} 
            WITH SCHEMA EVOLUTION AS SELECT * FROM 
            `{uc_catalog_name}`.`{uc_curated_schema}`.{mars_data_dump_output_table_name} t 
            WHERE t.OPFLAG='D' OR EXISTS ( 
            SELECT 1 FROM `{uc_catalog_name}`.`{security_schema}`.{security_function_name}('Profit_Center') pc WHERE  t.Profit_Center = REPLACE(LTRIM(REPLACE(pc.allowed_value ,'0',' ')),' ','0'))
            AND t.OPFLAG='D' OR EXISTS ( 
            SELECT 1 FROM `{uc_catalog_name}`.`{security_schema}`.{security_function_name}('Company_Code') eu WHERE t.Company = eu.allowed_value)
            AND (CASE WHEN '{env}' IN ('dev','tst') 
                THEN t.OPFLAG='D' OR EXISTS (
                    SELECT 1 FROM `{uc_catalog_name}`.`{security_schema}`.{security_function_name}('Source_ID') sid WHERE t.Source_ID = sid.allowed_value) 
                ELSE 1=1 END)""")
    
assignViewPermission(uc_catalog_name,uc_curated_schema,mars_data_dump_output_view_name,tbl_owner_grp, security_end_user_aad_group_name, security_object_aad_group_name, security_functional_readers_aad_group, security_functional_dev_aad_group)

print("MARS DDO view dropped and recreated in the schema")

# COMMAND ----------

# DBTITLE 1,margin_bucket_collection_view creation
use_catalog = "USE CATALOG `"+uc_catalog_name+"`"
spark.sql(use_catalog)
spark.sql(f"""DROP VIEW IF EXISTS `{uc_catalog_name}`.`{uc_curated_schema}`.{margin_bucket_collection_view_name}""")
spark.sql(f"""CREATE OR REPLACE VIEW `{uc_catalog_name}`.`{uc_curated_schema}`.{margin_bucket_collection_view_name}
            WITH SCHEMA EVOLUTION AS SELECT * FROM 
            `{uc_catalog_name}`.`{uc_curated_schema}`.{margin_bucket_collection_table_name} t 
            WHERE t.OPFLAG='D' OR EXISTS ( 
            -- SELECT 1 FROM `{uc_catalog_name}`.`{security_schema}`.{security_function_name}('Profit_Center') pc WHERE  t.Profit_Center = REPLACE(LTRIM(REPLACE(pc.allowed_value ,'0',' ')),' ','0'))
            --AND EXISTS ( 
            SELECT 1 FROM `{uc_catalog_name}`.`{security_schema}`.{security_function_name}('Company_Code') eu WHERE t.Company = eu.allowed_value)
            AND (CASE WHEN '{env}' IN ('dev','tst') 
                THEN t.OPFLAG='D' OR EXISTS (
                    SELECT 1 FROM `{uc_catalog_name}`.`{security_schema}`.{security_function_name}('Source_ID') sid WHERE t.Source_ID = sid.allowed_value) 
                ELSE 1=1 END)""")

assignViewPermission(uc_catalog_name,uc_curated_schema,margin_bucket_collection_view_name,tbl_owner_grp, security_end_user_aad_group_name, security_object_aad_group_name, security_functional_readers_aad_group, security_functional_dev_aad_group)
    
print("MARS MBC view dropped and recreated in the schema")
