# Databricks notebook source
pip install msal

# COMMAND ----------

pip install adal

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession()

# COMMAND ----------

from datta_pipeline_library.core.base_config import (
    BaseConfig,
    CollibraConfig,
    CommonConfig,
    EnvConfig
)
from datta_pipeline_library.helpers.adls import configure_spark_to_use_spn_to_write_to_adls_gen2
from datta_pipeline_library.helpers.spn import AzureSPN
# from datta_pipeline_library.edc.collibra import fetch_business_metadata
from datta_pipeline_library.helpers.uc import (
    get_catalog_name,
    get_raw_schema_name,
    get_euh_schema_name,
    get_eh_schema_name,
)

# COMMAND ----------

env = dbutils.widgets.get(name="env")
repos_path = dbutils.widgets.get(name="repos_path")
unique_repo_branch_id = dbutils.widgets.get(name="unique_repo_branch_id")
unique_repo_branch_id_schema = dbutils.widgets.get(name="unique_repo_branch_id_schema")

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
edc_user_id = dbutils.secrets.get(scope=kv, key=env_conf.edc_user_id_key)
edc_user_pwd = dbutils.secrets.get(scope=kv, key=env_conf.edc_user_pwd_key)

spn = AzureSPN(tenant_id, spn_client_id, spn_client_secret)
configure_spark_to_use_spn_to_write_to_adls_gen2(env_conf.storage_account, spn)

# COMMAND ----------

uc_catalog_name = base_config.get_uc_catalog_name()
print("uc_catalog_name : ",uc_catalog_name)

uc_curated_schema = base_config.get_uc_curated_schema()
print("uc_curated_schema : ", uc_curated_schema)

tbl_owner_grp = base_config.get_tbl_owner_grp()
print("tbl_owner_grp : ",tbl_owner_grp)

tbl_read_grp = base_config.get_tbl_read_grp()
print("tbl_read_grp : ",tbl_owner_grp)
landing_non_confidential_folder_path = base_config.get_landing_folder_path()
if env=="dev":
  landing_sp_folder_path = landing_non_confidential_folder_path.replace("DS_HANA_CDD", "FS_Usecase_MARS")
elif env=="tst":
  landing_sp_folder_path = landing_non_confidential_folder_path.replace("DS_HANA_CDD", "FS_Usecase_MARS")
elif env=="pre":
  landing_sp_folder_path = landing_non_confidential_folder_path.replace("DS_HANA_ZDD", "FS_Usecase_MARS")
elif env=="prd":
  landing_sp_folder_path = landing_non_confidential_folder_path.replace("DS_HANA_PDD", "FS_Usecase_MARS")
else:
  print("please provide the environment variable") 

print("landing_sp_folder_path : ",landing_sp_folder_path)

curated_folder_path = base_config.get_curated_folder_path()
print("curated_folder_path : ",curated_folder_path)

# COMMAND ----------

# DBTITLE 1,BPS New File Load
bps_new_table_name="CUSTOM_UC_MARS_BPS_NEW"
bps_new_table_path = curated_folder_path+"/"+bps_new_table_name
bps_new_file_landing_path = landing_sp_folder_path+"/BPS New.parquet"
bps_new_df = spark.read.format("parquet").option("mergeSchema", "true").load(bps_new_file_landing_path)
bps_new_df = bps_new_df.where("Company is not null and Account is not null")
bps_new_df = bps_new_df.withColumn("Source_ID", lit("Flat_000")).withColumn("OPFLAG", lit("I"))

bps_new_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{bps_new_table_path}/")

# COMMAND ----------

# DBTITLE 1,BPS reversal File Load
bps_reversal_table_name="CUSTOM_UC_MARS_BPS_REVERSAL"
bps_reversal_table_path = curated_folder_path+"/"+bps_reversal_table_name
bps_reversal_file_landing_path = landing_sp_folder_path+"/BPS reversal.parquet"
bps_reversal_df = spark.read.format("parquet").option("mergeSchema", "true").load(bps_reversal_file_landing_path)
bps_reversal_df = bps_reversal_df.where("Company is not null and Account is not null")
bps_reversal_df = bps_reversal_df.withColumn("Source_ID", lit("Flat_000")).withColumn("OPFLAG", lit("I"))

bps_reversal_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{bps_reversal_table_path}/")

# COMMAND ----------

# DBTITLE 1,NA Allocation File Load
na_allocation_table_name="CUSTOM_UC_MARS_NA_ALLOCATION"
na_allocation_table_path = curated_folder_path+"/"+na_allocation_table_name
na_allocation_file_landing_path = landing_sp_folder_path+"/NA allocation.parquet"
na_allocation_df = spark.read.format("parquet").option("mergeSchema", "true").load(na_allocation_file_landing_path)
na_allocation_df = na_allocation_df.where("Company is not null")
na_allocation_df = na_allocation_df.withColumn("Source_ID", lit("Flat_000")).withColumn("OPFLAG", lit("I"))

na_allocation_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{na_allocation_table_path}/")

# COMMAND ----------

# DBTITLE 1,C1 Mapping File Load
c1_mapping_table_name="CUSTOM_UC_MARS_C1_MAPPING"
c1_mapping_table_path = curated_folder_path+"/"+c1_mapping_table_name
c1_mapping_file_landing_path = landing_sp_folder_path+"/C1 Mapping.parquet"
c1_mapping_df = spark.read.format("parquet").option("mergeSchema", "true").load(c1_mapping_file_landing_path)
c1_mapping_df = c1_mapping_df.withColumn("Source_ID", lit("Flat_000")).withColumn("OPFLAG", lit("I"))

c1_mapping_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{c1_mapping_table_path}/")

# COMMAND ----------

# DBTITLE 1,Manual Margin Explanation File Load
manula_margin_explanation_table_name="CUSTOM_UC_MARS_MANUAL_MARGIN_EXPLANATION"
manula_margin_explanation_table_path = curated_folder_path+"/"+manula_margin_explanation_table_name
manula_margin_explanation_file_landing_path = landing_sp_folder_path+"/manual margin explination.parquet"
manula_margin_explanation_df = spark.read.format("parquet").option("mergeSchema", "true").load(manula_margin_explanation_file_landing_path)
manula_margin_explanation_df = manula_margin_explanation_df.where("Company is not null")
manula_margin_explanation_df = manula_margin_explanation_df.withColumn("Source_ID", lit("Flat_000")).withColumn("OPFLAG", lit("I"))

manula_margin_explanation_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{manula_margin_explanation_table_path}/")

# COMMAND ----------

# DBTITLE 1,Margin Bucket Collection File Load
# margin_bucket_collection_table_name="CUSTOM_UC_MARS_MARGIN_BUCKET_COLLECTION"
# margin_bucket_collection_table_path = curated_folder_path+"/"+margin_bucket_collection_table_name
# margin_bucket_collection_file_landing_path = landing_sp_folder_path+"/Margin Bucket Collection.parquet"
# margin_bucket_collection_df = spark.read.format("parquet").option("mergeSchema", "true").load(margin_bucket_collection_file_landing_path)
# margin_bucket_collection_df = margin_bucket_collection_df.withColumn("Source_ID", lit("Flat_000"))

# margin_bucket_collection_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{margin_bucket_collection_table_path}/")

# COMMAND ----------

# DBTITLE 1,Bio Premia File Load
bio_premia_table_name="CUSTOM_UC_MARS_BIO_PREMIA"
bio_premia_table_path = curated_folder_path+"/"+bio_premia_table_name
bio_premia_file_landing_path = landing_sp_folder_path+"/Bio premia.parquet"
bio_premia_df = spark.read.format("parquet").option("mergeSchema", "true").load(bio_premia_file_landing_path)
bio_premia_df = bio_premia_df.where("Company is not null")
bio_premia_df = bio_premia_df.withColumn("Source_ID", lit("Flat_000")).withColumn("OPFLAG", lit("I"))

bio_premia_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{bio_premia_table_path}/")

# COMMAND ----------

# DBTITLE 1,Logistics TFP File Load
logistics_tfp_table_name="CUSTOM_UC_MARS_LOGISTICS_TFP"
logistics_tfp_table_path = curated_folder_path+"/"+logistics_tfp_table_name
logistics_tfp_file_landing_path = landing_sp_folder_path+"/Logistics_TFP.parquet"
logistics_tfp_df = spark.read.format("parquet").option("mergeSchema", "true").load(logistics_tfp_file_landing_path)

logistics_tfp_df = logistics_tfp_df.where("Company_Code is not null and FCB_Plant is not null")
logistics_tfp_df = logistics_tfp_df.withColumn("Source_ID", lit("Flat_000")).withColumn("OPFLAG", lit("I"))

logistics_tfp_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{logistics_tfp_table_path}/")

# COMMAND ----------

# DBTITLE 1,Purchase CSO File Load
purchase_cso_table_name="CUSTOM_UC_MARS_PURCHASE_CSO"
purchase_cso_table_path = curated_folder_path+"/"+purchase_cso_table_name
purchase_cso_file_landing_path = landing_sp_folder_path+"/Purchase CSO.parquet"
purchase_cso_df = spark.read.format("parquet").option("mergeSchema", "true").load(purchase_cso_file_landing_path)
purchase_cso_df = purchase_cso_df.where("Company is not null and Account is not null")
purchase_cso_df = purchase_cso_df.withColumn("Source_ID", lit("Flat_000")).withColumn("OPFLAG", lit("I"))

purchase_cso_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{purchase_cso_table_path}/")

# COMMAND ----------

# DBTITLE 1,MASTER-Plant File Load
master_plant_table_name="CUSTOM_UC_MARS_MASTER_PLANT"
master_plant_table_path = curated_folder_path+"/"+master_plant_table_name
master_plant_file_landing_path = landing_sp_folder_path+"/MASTER_plant.parquet"
master_plant_df = spark.read.format("parquet").option("mergeSchema", "true").load(master_plant_file_landing_path)
master_plant_df = master_plant_df.where("Plant is not null")
master_plant_df = master_plant_df.withColumn("Source_ID", lit("Flat_000")).withColumn("OPFLAG", lit("I"))

master_plant_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{master_plant_table_path}/")

# COMMAND ----------

# DBTITLE 1,MASTER-Material File Load
master_material_table_name="CUSTOM_UC_MARS_MASTER_MATERIAL"
master_material_table_path = curated_folder_path+"/"+master_material_table_name
master_material_file_landing_path = landing_sp_folder_path+"/MASTER_Material.parquet"
master_material_df = spark.read.format("parquet").option("mergeSchema", "true").load(master_material_file_landing_path)
master_material_df = master_material_df.where("Material is not null")
master_material_df = master_material_df.withColumn("Source_ID", lit("Flat_000")).withColumn("OPFLAG", lit("I"))

master_material_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{master_material_table_path}/")

# COMMAND ----------

# DBTITLE 1,MASTER-MvtType File Load
master_mvt_type_table_name="CUSTOM_UC_MARS_MASTER_MVT_TYPE"
master_mvt_type_table_path = curated_folder_path+"/"+master_mvt_type_table_name
master_mvt_type_file_landing_path = landing_sp_folder_path+"/MASTER_MvtType.parquet"
master_mvt_type_df = spark.read.format("parquet").option("mergeSchema", "true").load(master_mvt_type_file_landing_path)
master_mvt_type_df = master_mvt_type_df.where("MvT is not null")
master_mvt_type_df = master_mvt_type_df.withColumn("Source_ID", lit("Flat_000")).withColumn("OPFLAG", lit("I"))

master_mvt_type_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{master_mvt_type_table_path}/")

# COMMAND ----------

# DBTITLE 1,MASTER-Accounts File Load
master_accounts_table_name="CUSTOM_UC_MARS_MASTER_ACCOUNTS"
master_accounts_table_path = curated_folder_path+"/"+master_accounts_table_name
master_accounts_file_landing_path = landing_sp_folder_path+"/MASTER_Accounts.parquet"
master_accounts_df = spark.read.format("parquet").option("mergeSchema", "true").load(master_accounts_file_landing_path)
master_accounts_df = master_accounts_df.where("Account is not null")
master_accounts_df = master_accounts_df.withColumn("Source_ID", lit("Flat_000")).withColumn("OPFLAG", lit("I"))

master_accounts_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{master_accounts_table_path}/")

# COMMAND ----------

# DBTITLE 1,MASTER-FA File Load
master_fa_table_name="CUSTOM_UC_MARS_MASTER_FA"
master_fa_table_path = curated_folder_path+"/"+master_fa_table_name
master_fa_file_landing_path = landing_sp_folder_path+"/MASTER_FA.parquet"
master_fa_df = spark.read.format("parquet").option("mergeSchema", "true").load(master_fa_file_landing_path)
master_fa_df = master_fa_df.where("Functional_Area is not null")
master_fa_df = master_fa_df.withColumn("Source_ID", lit("Flat_000")).withColumn("OPFLAG", lit("I"))

master_fa_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{master_fa_table_path}/")

# COMMAND ----------

# DBTITLE 1,C3 items File Load
c3_items_table_name="CUSTOM_UC_MARS_C3_ITEMS"
c3_items_table_path = curated_folder_path+"/"+c3_items_table_name
c3_items_file_landing_path = landing_sp_folder_path+"/C3 items.parquet"
c3_items_df = spark.read.format("parquet").option("mergeSchema", "true").load(c3_items_file_landing_path)
c3_items_df = c3_items_df.withColumn("Source_ID", lit("Flat_000")).withColumn("OPFLAG", lit("I"))

c3_items_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{c3_items_table_path}/")

# COMMAND ----------

# DBTITLE 1,Stock_Price_Effect File Load
stock_price_effect_table_name="CUSTOM_UC_MARS_STOCK_PRICE_EFFECT"
stock_price_effect_table_path = curated_folder_path+"/"+stock_price_effect_table_name
stock_price_effect_file_landing_path = landing_sp_folder_path+"/Stock_Price_Effect.parquet"
stock_price_effect_df = spark.read.format("parquet").option("mergeSchema", "true").load(stock_price_effect_file_landing_path)
stock_price_effect_df = stock_price_effect_df.withColumn("Source_ID", lit("Flat_000")).withColumn("OPFLAG", lit("I"))

stock_price_effect_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{stock_price_effect_table_path}/")

# COMMAND ----------

# DBTITLE 1,Sales_for_Logistics File Load
sales_for_logistics_table_name="CUSTOM_UC_MARS_SALES_FOR_LOGISTICS"
sales_for_logistics_table_path = curated_folder_path+"/"+sales_for_logistics_table_name
sales_for_logistics_file_landing_path = landing_sp_folder_path+"/Sales_for_Logistics.parquet"
sales_for_logistics_df = spark.read.format("parquet").option("mergeSchema", "true").load(sales_for_logistics_file_landing_path)
sales_for_logistics_df = sales_for_logistics_df.withColumn("Source_ID", lit("Flat_000")).withColumn("OPFLAG", lit("I"))

sales_for_logistics_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{sales_for_logistics_table_path}/")

# COMMAND ----------

# DBTITLE 1,Bio_TFP_Mandate File Load
bio_tfp_mandate_table_name="CUSTOM_UC_MARS_BIO_TFP_MANDATE"
bio_tfp_mandate_table_path = curated_folder_path+"/"+bio_tfp_mandate_table_name
bio_tfp_mandate_file_landing_path = landing_sp_folder_path+"/Bio_TFP_Mandate.parquet"
bio_tfp_mandate_df = spark.read.format("parquet").option("mergeSchema", "true").load(bio_tfp_mandate_file_landing_path)
bio_tfp_mandate_df = bio_tfp_mandate_df.withColumn("Source_ID", lit("Flat_000")).withColumn("OPFLAG", lit("I"))

bio_tfp_mandate_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{bio_tfp_mandate_table_path}/")

# COMMAND ----------

# DBTITLE 1,BEHG_TFP_Step1 File Load
behg_tfp_step1_table_name="CUSTOM_UC_MARS_BEHG_TFP_STEP1"
behg_tfp_step1_table_path = curated_folder_path+"/"+behg_tfp_step1_table_name
behg_tfp_step1_file_landing_path = landing_sp_folder_path+"/BEHG_TFP_Step1.parquet"
behg_tfp_step1_df = spark.read.format("parquet").option("mergeSchema", "true").load(behg_tfp_step1_file_landing_path)
behg_tfp_step1_df = behg_tfp_step1_df.withColumn("Source_ID", lit("Flat_000")).withColumn("OPFLAG", lit("I"))

behg_tfp_step1_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{behg_tfp_step1_table_path}/")

# COMMAND ----------

# DBTITLE 1,BEHG_accrual File Load
behg_accrual_table_name="CUSTOM_UC_MARS_BEHG_ACCRUAL"
behg_accrual_table_path = curated_folder_path+"/"+behg_accrual_table_name
behg_accrual_file_landing_path = landing_sp_folder_path+"/BEHG_accrual.parquet"
behg_accrual_df = spark.read.format("parquet").option("mergeSchema", "true").load(behg_accrual_file_landing_path)
behg_accrual_df = behg_accrual_df.withColumn("Source_ID", lit("Flat_000")).withColumn("OPFLAG", lit("I"))

behg_accrual_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{behg_accrual_table_path}/")

# COMMAND ----------

# DBTITLE 1,108a_TFP File load
TFP_108a_table_name="CUSTOM_UC_MARS_108A_TFP"
TFP_108a_table_path = curated_folder_path+"/"+TFP_108a_table_name
TFP_108a_file_landing_path = landing_sp_folder_path+"/TFP_108A.parquet"
TFP_108a_df = spark.read.format("parquet").option("mergeSchema", "true").load(TFP_108a_file_landing_path)
TFP_108a_df = TFP_108a_df.withColumn("Source_ID", lit("Flat_000")).withColumn("OPFLAG", lit("I"))

TFP_108a_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{TFP_108a_table_path}/")
