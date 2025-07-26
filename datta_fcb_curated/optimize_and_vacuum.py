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

# env = "dev"
# repos_path = "/Repos/DATTA-MVP2/DATTA-FCB-CURATED"
# unique_repo_branch_id = ""
# unique_repo_branch_id_schema = "gsap"

# COMMAND ----------

common_conf = CommonConfig.from_file(f"/Workspace/{repos_path.strip('/')}/conf/common/common_conf.json")
print("common_conf !!!")
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

uc_eh_schema = base_config.get_uc_eh_schema()
uc_curated_schema = base_config.get_uc_curated_schema()
print("uc_curated_schema : ",uc_curated_schema)

uc_eh_schema = uc_eh_schema.replace("-gsap","")
uc_eh_fi_schema = uc_eh_schema
uc_eh_md_schema = uc_eh_schema.replace("-finance", "-masterdata")
uc_eh_mm_schema = uc_eh_schema.replace("-finance", "-material_mgmt")

print("eh fi schema: ", uc_eh_fi_schema)
print("eh md schema: ", uc_eh_md_schema)
print("eh mm schema: ", uc_eh_mm_schema)

# COMMAND ----------

spark.sql(f"""VACUUM `{uc_catalog_name}`.`{uc_eh_fi_schema}`.fact_fi_act_line_item""")
spark.sql(f"""VACUUM `{uc_catalog_name}`.`{uc_eh_md_schema}`.md_fi_gl_account""")
spark.sql(f"""VACUUM `{uc_catalog_name}`.`{uc_eh_md_schema}`.md_mm_material""")
spark.sql(f"""VACUUM `{uc_catalog_name}`.`{uc_eh_md_schema}`.md_mm_vendor""")
spark.sql(f"""VACUUM `{uc_catalog_name}`.`{uc_eh_md_schema}`.md_mm_plant""")
spark.sql(f"""VACUUM `{uc_catalog_name}`.`{uc_eh_md_schema}`.md_sd_customer""")
spark.sql(f"""VACUUM `{uc_catalog_name}`.`{uc_eh_md_schema}`.md_fi_comp_code""")
spark.sql(f"""VACUUM `{uc_catalog_name}`.`{uc_eh_md_schema}`.md_mm_zmaterial""")
spark.sql(f"""VACUUM `{uc_catalog_name}`.`{uc_eh_md_schema}`.md_mm_mov_type""")
spark.sql(f"""VACUUM `{uc_catalog_name}`.`{uc_eh_md_schema}`.md_sd_dist_channel""")
spark.sql(f"""VACUUM `{uc_catalog_name}`.`{uc_eh_md_schema}`.md_fi_profit_center""")
spark.sql(f"""VACUUM `{uc_catalog_name}`.`{uc_curated_schema}`.use_case_fcb_dn_supply_margin""")
#MARS tables
spark.sql(f"""VACUUM `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_behg_tfp_step1""")
spark.sql(f"""VACUUM `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_bio_premia""")
spark.sql(f"""VACUUM `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_bio_tfp_mandate""")
spark.sql(f"""VACUUM `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_bps_new""")
spark.sql(f"""VACUUM `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_bps_reversal""")
spark.sql(f"""VACUUM `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_c1_mapping""")
spark.sql(f"""VACUUM `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_c3_items""")
spark.sql(f"""VACUUM `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_logistics_tfp""")
spark.sql(f"""VACUUM `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_manual_margin_explanation""")
# spark.sql(f"""VACUUM `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_margin_bucket_collection""")
spark.sql(f"""VACUUM `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_accounts""")
spark.sql(f"""VACUUM `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_fa""")
spark.sql(f"""VACUUM `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_material""")
spark.sql(f"""VACUUM `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_mvt_type""")
spark.sql(f"""VACUUM `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_plant""")
spark.sql(f"""VACUUM `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_na_allocation""")
spark.sql(f"""VACUUM `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_purchase_cso""")
spark.sql(f"""VACUUM `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_sales_for_logistics""")
spark.sql(f"""VACUUM `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_stock_price_effect""")
spark.sql(f"""VACUUM `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_behg_accrual""")
spark.sql(f"""VACUUM `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_108a_tfp""")
spark.sql(f"""VACUUM `{uc_catalog_name}`.`{uc_curated_schema}`.use_case_mars_data_dump_output""")
spark.sql(f"""VACUUM `{uc_catalog_name}`.`{uc_curated_schema}`.use_case_mars_data_dump_collection""")
spark.sql(f"""VACUUM `{uc_catalog_name}`.`{uc_curated_schema}`.use_case_mars_margin_bucket_collection""")
spark.sql(f"""VACUUM `{uc_catalog_name}`.`{uc_eh_mm_schema}`.fact_hm_daily_movement_summary""")

# COMMAND ----------

spark.sql(f"""OPTIMIZE `{uc_catalog_name}`.`{uc_eh_fi_schema}`.fact_fi_act_line_item ZORDER BY (Rec_No_Line_Itm_Rec)""")
spark.sql(f"""OPTIMIZE `{uc_catalog_name}`.`{uc_eh_md_schema}`.md_fi_gl_account ZORDER BY (Language_Key, Chart_of_Accounts, GL_Account_No)""")
spark.sql(f"""OPTIMIZE `{uc_catalog_name}`.`{uc_eh_md_schema}`.md_mm_material ZORDER BY (Material_Group, Material_Number)""")
spark.sql(f"""OPTIMIZE `{uc_catalog_name}`.`{uc_eh_md_schema}`.md_mm_vendor ZORDER BY (Account_No_Supplier)""")
spark.sql(f"""OPTIMIZE `{uc_catalog_name}`.`{uc_eh_md_schema}`.md_mm_plant ZORDER BY (Plant, Dist_Profile_Plant)""")
spark.sql(f"""OPTIMIZE `{uc_catalog_name}`.`{uc_eh_md_schema}`.md_sd_customer ZORDER BY (Customer_Number)""")
spark.sql(f"""OPTIMIZE `{uc_catalog_name}`.`{uc_eh_md_schema}`.md_fi_comp_code ZORDER BY (Company_Code)""")
spark.sql(f"""OPTIMIZE `{uc_catalog_name}`.`{uc_eh_md_schema}`.md_mm_zmaterial ZORDER BY (Material,DEX_Code, Commodity_Code)""")
spark.sql(f"""OPTIMIZE `{uc_catalog_name}`.`{uc_eh_md_schema}`.md_mm_mov_type ZORDER BY (Mvmt_Type, Mvmt_Type_Text)""")
spark.sql(f"""OPTIMIZE `{uc_catalog_name}`.`{uc_eh_md_schema}`.md_sd_dist_channel ZORDER BY (Distribution_Channel
, Distribution_Channel_Text)""")
spark.sql(f"""OPTIMIZE `{uc_catalog_name}`.`{uc_eh_md_schema}`.md_fi_profit_center ZORDER BY (Profit_Center, Profit_Center_Text)""")
spark.sql(f"""OPTIMIZE `{uc_catalog_name}`.`{uc_curated_schema}`.use_case_fcb_dn_supply_margin ZORDER BY (Rec_No_Line_Itm_Rec)""")
#MARS tables
spark.sql(f"""OPTIMIZE `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_behg_tfp_step1 ZORDER BY (Company_Code, GL_Account_Key)""")
spark.sql(f"""OPTIMIZE `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_bio_premia ZORDER BY (Company, Bio_Material)""")
spark.sql(f"""OPTIMIZE `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_bio_tfp_mandate ZORDER BY (Company_Code, FCB_Plant)""")
spark.sql(f"""OPTIMIZE `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_bps_new ZORDER BY (Company, Account)""")
spark.sql(f"""OPTIMIZE `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_bps_reversal ZORDER BY (Company, Account)""")
spark.sql(f"""OPTIMIZE `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_c1_mapping ZORDER BY (GL_Account)""")
spark.sql(f"""OPTIMIZE `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_c3_items ZORDER BY (Company_Code, Profit_Center_Key)""")
spark.sql(f"""OPTIMIZE `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_logistics_tfp ZORDER BY (Company_Code, FCB_Plant)""")
spark.sql(f"""OPTIMIZE `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_manual_margin_explanation ZORDER BY (Company, Margin_Driver)""")
# spark.sql(f"""OPTIMIZE `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_margin_bucket_collection ZORDER BY (Margin_Bucket, Company)""")
spark.sql(f"""OPTIMIZE `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_accounts ZORDER BY (Account)""")
spark.sql(f"""OPTIMIZE `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_fa ZORDER BY (Functional_Area)""")
spark.sql(f"""OPTIMIZE `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_material ZORDER BY (Material)""")
spark.sql(f"""OPTIMIZE `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_mvt_type ZORDER BY (MvT)""")
spark.sql(f"""OPTIMIZE `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_plant ZORDER BY (Plant)""")
spark.sql(f"""OPTIMIZE `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_na_allocation ZORDER BY (Company, Account)""")
spark.sql(f"""OPTIMIZE `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_purchase_cso ZORDER BY (Company, Account)""")
spark.sql(f"""OPTIMIZE `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_sales_for_logistics ZORDER BY (Company, FCB_Plant)""")
spark.sql(f"""OPTIMIZE `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_stock_price_effect ZORDER BY (Company, FCB_Plant)""")
spark.sql(f"""OPTIMIZE `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_behg_accrual ZORDER BY (Company_Code, FCB_Plant)""")
spark.sql(f"""OPTIMIZE `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_108a_tfp ZORDER BY (Country_Code, Plant_GSAP)""")
spark.sql(f"""OPTIMIZE `{uc_catalog_name}`.`{uc_curated_schema}`.use_case_mars_data_dump_output ZORDER BY (Company, Profit_Center, Year, Month)""")
spark.sql(f"""OPTIMIZE `{uc_catalog_name}`.`{uc_curated_schema}`.use_case_mars_data_dump_collection ZORDER BY (Company, Profit_Center)""")
spark.sql(f"""OPTIMIZE `{uc_catalog_name}`.`{uc_curated_schema}`.use_case_mars_margin_bucket_collection ZORDER BY (Company, Plant, Year, Month)""")
spark.sql(f"""OPTIMIZE `{uc_catalog_name}`.`{uc_eh_mm_schema}`.fact_hm_daily_movement_summary ZORDER BY (Source_ID,
Company_Code, Plant, Material, Movement_Type, Material_Doc_Number, Material_Item_Number,Material_Document_Year,Counter)""")
