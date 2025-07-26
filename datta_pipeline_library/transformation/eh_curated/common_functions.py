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
load_type=dbutils.widgets.get(name="load_type")
fcb_dynamic_view_name = dbutils.widgets.get(name="fcb_dynamic_view_name")
# env = "dev"
# repos_path = "/Repos/DATTA-MARS-LATEST/DATTA-FCB-CURATED"
# unique_repo_branch_id = ""
# unique_repo_branch_id_schema = ""
# fcb_dynamic_view_name = "vw_use_case_fcb_dn_supply_margin"
# load_type = "INIT"

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
# configure_spark_to_use_spn_to_write_to_adls_gen2(env_conf.storage_account, spn)

# COMMAND ----------

uc_catalog_name = base_config.get_uc_catalog_name()
print("uc_catalog_name : ",uc_catalog_name)

uc_raw_schema = base_config.get_uc_raw_schema()
print("uc_raw_schema : ",uc_raw_schema)

uc_euh_schema = base_config.get_uc_euh_schema()
print("uc_euh_schema : ",uc_euh_schema)

uc_eh_schema = base_config.get_uc_eh_schema()
uc_curated_schema = base_config.get_uc_curated_schema()
print("uc_curated_schema : ",uc_curated_schema)

eh_folder_path = base_config.get_eh_folder_path()
curated_folder_path = base_config.get_curated_folder_path()
print("curated_folder_path : ",curated_folder_path)

tbl_owner_grp = base_config.get_tbl_owner_grp()
print("tbl_owner_grp : ",tbl_owner_grp)
tbl_read_grp = base_config.get_tbl_read_grp()
print("tbl_read_grp : ",tbl_owner_grp)

uc_eh_schema = uc_eh_schema.replace("-gsap", "")
eh_folder_path = eh_folder_path.replace("/gsap","")
eh_fi_folder_path=eh_folder_path
eh_md_folder_path=eh_folder_path.replace("/finance_", "/masterdata_")
uc_eh_fi_schema = uc_eh_schema
print("uc_eh_schema : ",uc_eh_schema)
print("eh_folder_path : ",eh_folder_path)

uc_eh_md_schema = uc_eh_schema.replace("-finance", "-masterdata")
print("eh fi schema: ", uc_eh_fi_schema)
print("eh fi folder path: ", eh_fi_folder_path)
print("eh md schema: ", uc_eh_md_schema)
print("eh md folder path: ", eh_md_folder_path)

print("load_type:", load_type)

# COMMAND ----------

''' assignPermission This function assigns Permission to all the tables created '''
def assignPermission(catalog,schema,table_name,tbl_owner,tbl_read):
    spark.sql(f"ALTER table `{catalog}`.`{schema}`.{table_name} owner to `{tbl_owner}`")
    print("Table Owner is assigned")
    spark.sql(f"GRANT ALL PRIVILEGES ON TABLE `{catalog}`.`{schema}`.{table_name} TO `{tbl_owner}`")
    print("All privileges access given to tbl owner")
    spark.sql(f"GRANT SELECT ON TABLE `{catalog}`.`{schema}`.{table_name} TO `{tbl_read}`")
    print("Reader access granted")

# COMMAND ----------

def language_filter(df):
    filtered_df = df.filter(df.SPRAS=="E") # filter 'column' with the language key
    return filtered_df

def zoh_language_filter(df):
    filtered_df = df.filter(df.LANGU=="E")
    return filtered_df

def client_filter(df):
    filtered_df = df.filter(df.MANDT==110) # filter 'column' with the client value 
    return filtered_df

def skat_filter(df):
    filtered_df = df.filter((df.SAKNR >= "0") & (df.SAKNR <= "0008999999"))
    return filtered_df
    
def csku_filter(df):
    filtered_df = df.filter((df.KSTAR >= "0009000000") & (df.KSTAR <= "0009999999"))
    return filtered_df
    
def ktopl_filter(df):
    filtered_df = df.filter(df.KTOPL == 'OP01')
    return filtered_df

def glpca_filter(df):
    filtered_df = df.filter((df.RCLNT == 110) & ((df.RACCT >= "0006000000") & (df.RACCT <= "0009999999")))
    return filtered_df

def bkpf_filter(df):
    filtered_df = df.filter(col("BLART").isin('HC','HF'))
    filtered_df = filtered_df.filter(col("TCODE").isin('/DS1/HM_C_DAILY_T','FB08'))
    return filtered_df
    
def mseg02_filter(df):
    filtered_df = df.filter((df.MANDT==110)&(df.MSEHI=='L15'))
    return filtered_df

# COMMAND ----------

# DBTITLE 1,Function for column selecting and renaming
def select_and_rename_columns(df,columns):
    return df.select([f.col(c).alias(columns[c]) for c in columns.keys()])

# COMMAND ----------

def get_latest_delta(df, dlt_workflow_name, col_name, catalog, schema):
    df_max_run_start_time = spark.sql(f"""select run_start_date from `{catalog}`.`{schema}`.process_status where run_id in (select max(run_id) as max_run_id from `{catalog}`.`{schema}`.process_status where status = 'completed' and dlt_workflow_name = '{dlt_workflow_name}') and dlt_workflow_name = '{dlt_workflow_name}'""").first()[0]
    print(df_max_run_start_time)
    filtered_df = df.filter((col(f"{col_name}") >= df_max_run_start_time))
                            
    return filtered_df

# COMMAND ----------

from pyspark.sql.functions import col

def get_latest_delete_records(df, dlt_workflow_name,col_name, delete_flag_col_name, catalog, schema):
    df_max_run_start_time = spark.sql(f"""select run_start_date from `{catalog}`.`{schema}`.process_status where run_id in (select max(run_id) as max_run_id from `{catalog}`.`{schema}`.process_status where status = 'completed' and dlt_workflow_name = '{dlt_workflow_name}') and dlt_workflow_name = '{dlt_workflow_name}'""").first()[0]
    print(df_max_run_start_time)
    filtered_df = df.filter((col(f"{col_name}") >= df_max_run_start_time))
    filtered_delete_df = filtered_df.filter((col(f"{delete_flag_col_name}") == "D"))
                    
    return filtered_delete_df

# COMMAND ----------

from datetime import datetime, timedelta
 
# Set the date to January 1, 2025
now = datetime.now()
# now = datetime(2025,2,1)
 
# Get the first day of the current month
first_of_this_month = now.replace(day=1)
 
# Function to get the year and month of a date
def get_year_month(date):
    return date.strftime('%Y'), date.strftime('%m')
 
# Calculate the end of the last month
end_of_last_month = first_of_this_month - timedelta(days=1)
prev_1st_year, prev_1st_month = get_year_month(end_of_last_month)
 
# Calculate the end of the second last month
first_of_last_month = first_of_this_month - timedelta(days=first_of_this_month.day)
end_of_2nd_last_month = first_of_last_month - timedelta(days=1)
prev_2nd_year, prev_2nd_month = get_year_month(end_of_2nd_last_month)
 
# Calculate the end of the third last month
first_of_2nd_last_month = first_of_last_month - timedelta(days=first_of_last_month.day)
end_of_3rd_last_month = first_of_2nd_last_month - timedelta(days=1)
prev_3rd_year, prev_3rd_month = get_year_month(end_of_3rd_last_month)
 
# Calculate the end of the fourth last month
first_of_3rd_last_month = first_of_2nd_last_month - timedelta(days=first_of_2nd_last_month.day)
end_of_4th_last_month = first_of_3rd_last_month - timedelta(days=1)
prev_4th_year, prev_4th_month = get_year_month(end_of_4th_last_month)
 
endOfLastMonth = end_of_2nd_last_month.strftime('%Y%m')
end_of_4th_last_month = end_of_4th_last_month.strftime('%Y%m') 

prev_1st_year = endOfLastMonth[:4]
prev_1st_month = endOfLastMonth[4:6]

prev_4th_year = end_of_4th_last_month[:4]
prev_4th_month = end_of_4th_last_month[4:6]
 
print(prev_1st_year,prev_1st_month)
print(prev_4th_year,prev_4th_month)

# COMMAND ----------

def mars_source_system_id():
    if env=='dev':
        gsap_source_system_name = 'D94'
    elif env=='tst':
        gsap_source_system_name = 'C94'   
    elif env=='pre':
        gsap_source_system_name = 'Z94'
    elif env=='prd':
        gsap_source_system_name = 'P94'
    else:
        gsap_source_system_name = ''
    
    return gsap_source_system_name
