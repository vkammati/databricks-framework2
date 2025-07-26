# Databricks notebook source
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

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql import functions as F
from pyspark.sql.types import *
# from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp,date_format
from pyspark.sql.functions import col, trim, lit, regexp_replace, col, concat_ws
from datetime import datetime
from delta.tables import *
# spark = SparkSession.getActiveSession()

# COMMAND ----------

# from pyspark.sql import SparkSession
# spark = SparkSession.getActiveSession()

# spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# COMMAND ----------

# from pyspark.dbutils import DBUtils

# spark = SparkSession.builder.getOrCreate()
# dbutils = DBUtils(spark)

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
# eh_folder_path = eh_folder_path.replace("/gsap","")
print("uc_eh_schema : ",uc_eh_schema)
# print("eh_folder_path : ",eh_folder_path)

uc_eh_md_schema = uc_eh_schema.replace("-finance", "-masterdata")
print("uc_eh_md_schema : ",uc_eh_md_schema)

print("load_type:", load_type)

# COMMAND ----------

ddo_source_system_id = mars_source_system_id()

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

# FCB Posting Period has three digits so we are adding one leading zero 
prev_1st_month = '0' + prev_1st_month
prev_4th_month = '0' + prev_4th_month

print(prev_1st_year,prev_1st_month)
print(prev_4th_year,prev_4th_month)

# logic to consider the data maintained from the control table with Active Flag 'Y' and to consider the respective month and year

df_year_month = spark.sql(f"""SELECT DISTINCT year,month
                             FROM `{uc_catalog_name}`.`{uc_curated_schema}`.mars_year_month_control_table 
                             WHERE UPPER(Active_Flag) = 'Y'
                         """)
cnt = df_year_month.count()
print(cnt)

if cnt == 1:
    prev_1st_month = df_year_month.collect()[0][1]
    prev_1st_year = df_year_month.collect()[0][0]
    print ("when cnt ==1:",prev_1st_month )
    print ("when cnt ==1:",prev_1st_year )
elif cnt == 0:
    print ("when cnt ==0:",prev_1st_month )
    print ("when cnt ==0:",prev_1st_year )
elif cnt>1:
    print("ERROR MSG: More than one active flag found in control table")

print(prev_1st_year,prev_1st_month)

# COMMAND ----------

def language_filter(df):
    filtered_df = df.filter(df.SPRAS=="E") # filter 'column' with the language key
    return filtered_df

def client_filter(df):
    filtered_df = df.filter(df.MANDT==110) # filter 'column' with the client value 
    return filtered_df

# COMMAND ----------

ddc_table_name="USE_CASE_MARS_DATA_DUMP_COLLECTION"
ddc_table_path = curated_folder_path+"/"+ddc_table_name
print("DDC",ddc_table_name)
print("DDC_path",ddc_table_path)

ddo_table_name="USE_CASE_MARS_DATA_DUMP_OUTPUT"
ddo_table_path = curated_folder_path+"/"+ddo_table_name
print("DDO",ddo_table_name)
print("DDO_path",ddo_table_path)

# COMMAND ----------

# DBTITLE 1,402:101_C1_HANA_ANCHOR
df_402 = spark.sql(f"""
SELECT
  "C1 HANA Anchor" AS Source,
  Company_Code AS Company,
  Profit_Center_Key AS Profit_Center,
  Cont_Level AS Contribution_Level,
  GL_Account_Key AS Account,
  Functional_Area,
  FCB_Plant,
  MM_Plant AS Partner_Plant,
  FCB_Material,
  MM_Material AS Partner_Material,
  Customer_Vendor_Number,
  Movement_Type_FCB AS Movement_Type,
  Source_ID,
  OPFLAG,
  Sum(Amount_in_LC) AS Amount_FiFo_LC,
  Sum(Weight_in_KG) AS Quantity_KG,
  Sum(Volume_in_L15) AS Volume_L15,
  0 AS Amount_CCS_LC
FROM
  hana_c1_df_final_303
GROUP BY
  Company_Code,
  Profit_Center_Key,
  Cont_Level,
  GL_Account_Key,
  Functional_Area,
  FCB_Plant,
  MM_Plant,
  FCB_Material,
  MM_Material,
  Customer_Vendor_Number,
  Movement_Type_FCB,
  Source_ID,
  OPFLAG
""")

# COMMAND ----------

# DBTITLE 1,402a: 111_Dummy purchase TFP
df_402a=spark.sql("""
SELECT
  "C1 HANA Purchase Dummy Anchor" AS Source,
  Company_Code AS Company,
  Profit_Center_Key AS Profit_Center,
  Cont_Level AS Contribution_Level,
  GL_Account_Key AS Account,
  Functional_Area,
  FCB_Plant,
  MM_Plant AS Partner_Plant,
  FCB_Material,
  MM_Material AS Partner_Material,
  Customer_Vendor_Number,
  Movement_Type_FCB AS Movement_Type,
  Source_ID,
  OPFLAG,
  Sum(Amount_in_LC) AS Amount_FiFo_LC,
  Sum(Weight_in_KG) AS Quantity_KG,
  Sum(Volume_in_L15) AS Volume_L15,
  0 AS Amount_CCS_LC
FROM
  hana_dummy_df_final
GROUP BY
  Company_Code,
  Profit_Center_Key,
  Cont_Level,
  GL_Account_Key,
  Functional_Area,
  FCB_Plant,
  MM_Plant,
  FCB_Material,
  MM_Material,
  Customer_Vendor_Number,
  Movement_Type_FCB,
  Source_ID,
  OPFLAG
""")

# COMMAND ----------

# DBTITLE 1,403: 102_C1 - Jet consignment]
df_403 = spark.sql("""    
SELECT
  "Jet Transfer" AS Source,
  C1Jet_consignment_102.Company_Code AS Company,
  '310001' AS Profit_Center,
  'C1' AS Contribution_Level,
  "6380303" AS Account,
  "SV31" AS Functional_Area,
  C1Jet_consignment_102.Plant AS FCB_Plant,
  C1Jet_consignment_102.Receiving_Plant AS Partner_Plant,
  C1Jet_consignment_102.Material AS FCB_Material,
  C1Jet_consignment_102.Receiving_Material AS Partner_Material,
  '' AS  Customer_Vendor_Number,
  C1Jet_consignment_102.Movement_Type AS Movement_Type,
  C1Jet_consignment_102.Source_ID,
  C1Jet_consignment_102.OPFLAG,
  Sum(-(C1Jet_consignment_102.Amount_in_LC)) AS Amount_FiFo_LC,
  Sum(C1Jet_consignment_102.Quantity_KG) AS Quantity_KG,
  0 AS Volume_L15,
  Sum(-(C1Jet_consignment_102.Amount_in_LC)) AS Amount_CCS_LC
FROM
  C1Jet_consignment_102
GROUP BY
  C1Jet_consignment_102.Company_Code,
  C1Jet_consignment_102.Plant,
  C1Jet_consignment_102.Material,
  C1Jet_consignment_102.Receiving_Plant,
  C1Jet_consignment_102.Receiving_Material,
  C1Jet_consignment_102.Movement_Type,
  C1Jet_consignment_102.Source_ID,
  C1Jet_consignment_102.OPFLAG,
  (MOD(CAST(C1Jet_consignment_102.Material_Document_Item AS INTEGER), 2) = 0)
HAVING
  ((MOD(CAST(C1Jet_consignment_102.Material_Document_Item AS INTEGER), 2) = 0) = 0)
  """)

# COMMAND ----------

# DBTITLE 1,404: 102_C1 - Jet consignment]
df_404 = spark.sql("""             
SELECT
  "Jet Transfer" AS Source,
  C1Jet_consignment_102.Company_Code AS Company,
  '310001' AS Profit_Center,
  'C1' AS Contribution_Level,
  "6380203" AS Account,
  "SV30" AS Functional_Area,
  C1Jet_consignment_102.Plant AS Partner_Plant,
  C1Jet_consignment_102.Receiving_Plant AS FCB_Plant,
  C1Jet_consignment_102.Material AS Partner_Material,
  C1Jet_consignment_102.Receiving_Material AS FCB_Material,  
  '#' AS Customer_Vendor_Number,
  C1Jet_consignment_102.Movement_Type AS Movement_Type,
  C1Jet_consignment_102.Source_ID,
  C1Jet_consignment_102.OPFLAG,
  Sum((C1Jet_consignment_102.Amount_in_LC)) AS Amount_FiFo_LC,
  Sum(-(C1Jet_consignment_102.Quantity_KG)) AS Quantity_KG,  
  0 AS Volume_L15,
  Sum((C1Jet_consignment_102.Amount_in_LC)) AS Amount_CCS_LC
FROM
  C1Jet_consignment_102
GROUP BY
  C1Jet_consignment_102.Company_Code,
  C1Jet_consignment_102.Plant,
  C1Jet_consignment_102.Material,
  C1Jet_consignment_102.Receiving_Plant,
  C1Jet_consignment_102.Receiving_Material,
  C1Jet_consignment_102.Movement_Type,
  C1Jet_consignment_102.Source_ID,
  C1Jet_consignment_102.OPFLAG,
  MOD(C1Jet_consignment_102.Material_Document_Item, 2) = 0
HAVING
  ((MOD(C1Jet_consignment_102.Material_Document_Item, 2) = 0) = 0)
  """)

# COMMAND ----------

# DBTITLE 1,405: 103_BCS reversal
df_405 = spark.sql(f"""
SELECT
  "BCS reversal" AS Source,
  Company,
  '310001' AS Profit_Center,
  'C1' AS Contribution_Level,
  Account,
  CASE WHEN Account = "6380201" THEN "OP37" ELSE Functional_Area END AS Functional_Area,
  Plant AS FCB_Plant,
  '' AS Partner_Plant,
  Material AS FCB_Material,
  '' AS Partner_Material,
  Customer_Or_Vendor AS Customer_Vendor_Number, 
  '' AS Movement_Type, 
  Source_ID,
  OPFLAG,
  Sum(C1_Margin_FIFO) AS Amount_FiFo_LC,
  Sum(Weight_Kg) AS Quantity_KG,
  0 AS Volume_L15,
  0 AS Amount_CCS_LC  
FROM
  `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_bps_reversal
GROUP BY
  Source,
  Company,
  Account,
  Customer_Or_Vendor,
  Source_ID,
  OPFLAG,
  Plant,
  Material,
  Functional_Area
""")

# COMMAND ----------

# DBTITLE 1,406: [104: BCS new]
df_406 = spark.sql(f"""              
SELECT
  "BCS new" AS Source,
  Company,
  '310001' AS Profit_Center,
  'C1' AS Contribution_Level,
  Account,
  Functional_Area,
  Plant AS FCB_Plant,
  '' AS Partner_Plant,
  Material AS FCB_Material,
  '' AS Partner_Material,
  Customer_Or_Vendor AS Customer_Vendor_Number,
  '' AS Movement_Type, 
  Source_ID,
  OPFLAG,
  Sum(C1_Margin_FIFO) AS Amount_FiFo_LC,
  Sum(Weight_Kg) AS Quantity_KG,
  0 AS Volume_L15,
  0 AS Amount_CCS_LC
FROM
  `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_bps_new
GROUP BY
  Source,
  Company,
  Account,
  Customer_Or_Vendor,
  Plant,
  Material,
  Functional_Area,
  Source_ID,
  OPFLAG
""")

# COMMAND ----------

# DBTITLE 1,407: [105: NA allocation]
df_407 = spark.sql(f"""
SELECT
  "NA allocation" AS Source,
  Company,
  '310001' AS Profit_Center,
  'C1' AS Contribution_Level,
  Account,
  'CO' AS Functional_Area,  
  Plant AS FCB_Plant,
  '' AS Partner_Plant,
  Material AS FCB_Material,
  '' AS Partner_Material,
  Cust_Or_Ven_Number AS Customer_Vendor_Number,
  '' AS Movement_Type,
  Source_ID,
  OPFLAG,
  Sum(C1_Margin_FIFO) AS Amount_FiFo_LC, 
  Sum(Weight_Kg) AS Quantity_KG,
  0 AS Volume_L15,
  0 AS Amount_CCS_LC   
FROM
  `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_na_allocation
GROUP BY
  Source,
  Company,
  Account,
  Cust_Or_Ven_Number,
  Plant,
  Material,
  Source_ID,
  OPFLAG
""")

# COMMAND ----------

# DBTITLE 1,604b: [108b: BEHG_accrual] LEFT JOIN [202: MASTER-Plant]
df_604b = spark.sql(f"""
SELECT
  "BEHG" AS Source,
  behg_accural.Company_Code AS Company, 
  '310001' AS Profit_Center,
  'C1' AS Contribution_Level,
  "6000200" AS Account,
  'CO' AS Functional_Area,
  behg_accural.FCB_Plant,
  '' AS Partner_Plant,
  behg_accural.FCB_Material,
  '' AS Partner_Material,
  '#' AS Customer_Vendor_Number, 
  '' AS Movement_Type,
  behg_accural.Source_ID,
  behg_accural.OPFLAG,
  Sum((behg_accural.Amount_in_LC) * -1) AS Amount_FiFo_LC,
  0 AS Quantity_KG,
  0 AS Volume_L15,
  0 AS Amount_CCS_LC 
FROM
  (
    `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_behg_accrual behg_accural
    LEFT JOIN `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_plant mstr_plnt
    ON trim(lower(behg_accural.FCB_Plant)) = trim(lower(mstr_plnt.Plant))
  )
  LEFT JOIN `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_material mstr_mtrl
  ON trim(lower(behg_accural.FCB_Material)) = trim(lower(mstr_mtrl.Material))
GROUP BY
  "BEHG",
  "6000200",
  behg_accural.Company_Code,
  behg_accural.FCB_Plant,
  behg_accural.FCB_Material,
  behg_accural.Source_ID,
  behg_accural.OPFLAG
HAVING
  (behg_accural.Company_Code <> "empty")
""")

# COMMAND ----------

# DBTITLE 1,604c:  [108b: BEHG_accrual] LEFT JOIN [202: MASTER-Plant]
df_604c = spark.sql(f"""
SELECT
  "BEHG" AS Source,
  behg_accural.Company_Code AS Company,
  '310001' AS Profit_Center,
  'C1' AS Contribution_Level,
  "6365020" AS Account,
  'CO' AS Functional_Area,
  behg_accural.FCB_Plant,
  '' AS Partner_Plant,
  behg_accural.FCB_Material,
  '' AS Partner_Material,
  '#' AS Customer_Vendor_Number, 
  '' AS Movement_Type, 
  behg_accural.Source_ID,
  behg_accural.OPFLAG,
  Sum((behg_accural.Amount_in_LC) * -1) AS Amount_FiFo_LC,
  0 AS Quantity_KG,
  0 AS Volume_L15,
  0 AS Amount_CCS_LC
FROM
  (
    `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_behg_accrual behg_accural
    LEFT JOIN `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_plant mstr_plnt
    ON trim(lower(behg_accural.FCB_Plant)) = trim(lower(mstr_plnt.Plant))
  )
  LEFT JOIN `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_material mstr_mtrl
  ON trim(lower(behg_accural.FCB_Material)) = trim(lower(mstr_mtrl.Material))
GROUP BY
  "BEHG",
  "6365020",
  behg_accural.Company_Code,
  behg_accural.FCB_Plant,
  behg_accural.FCB_Material,
  behg_accural.Source_ID,
  behg_accural.OPFLAG
HAVING
  (behg_accural.Company_Code <> "empty")
""")

# COMMAND ----------

# DBTITLE 1,412: 302_ CSO on purchase calc
df_412 = spark.sql("""
SELECT
  "CSO Purchase" AS Source,
  purchase_CSO_rate_302.303_Company_Code AS Company,
  purchase_CSO_rate_302.303_Profit_Center_Key AS Profit_Center,
  purchase_CSO_rate_302.303_Cont_Level AS Contribution_Level,
  purchase_CSO_rate_302.303_GL_Account_Key AS Account,
  purchase_CSO_rate_302.303_Functional_Area AS Functional_Area,
  purchase_CSO_rate_302.303_FCB_Plant AS FCB_Plant,
  '' AS Partner_Plant,
  purchase_CSO_rate_302.303_FCB_Material AS FCB_Material,
  '' AS Partner_Material,
  purchase_CSO_rate_302.303_Customer_Vendor_Number AS Customer_Vendor_Number, 
  '' AS Movement_Type,
  purchase_CSO_rate_302.Source_ID,
  purchase_CSO_rate_302.OPFLAG,
  Sum(-(purchase_CSO_rate_302.CSO_Amount)) AS Amount_FiFo_LC,
  0 AS Quantity_KG,
  0 AS Volume_L15,
  0 AS Amount_CCS_LC  
FROM
  purchase_CSO_rate_106_1_df purchase_CSO_rate_302
GROUP BY
  purchase_CSO_rate_302.303_Company_Code,
  purchase_CSO_rate_302.303_Profit_Center_Key,
  purchase_CSO_rate_302.303_Cont_Level,
  purchase_CSO_rate_302.303_GL_Account_Key,
  purchase_CSO_rate_302.303_Functional_Area,
  purchase_CSO_rate_302.303_FCB_Plant,
  purchase_CSO_rate_302.303_FCB_Material,
  purchase_CSO_rate_302.303_Customer_Vendor_Number,
  purchase_CSO_rate_302.Source_ID,
  purchase_CSO_rate_302.OPFLAG
  """)

# COMMAND ----------

# DBTITLE 1,412a: 302a_ CSO on exchange calc
df_412a = spark.sql("""
SELECT
"CSO Exchange" AS Source,
  purchase_CSO_rate_302a.303_Company_Code AS Company,
  purchase_CSO_rate_302a.303_Profit_Center_Key AS Profit_Center,
  purchase_CSO_rate_302a.303_Cont_Level AS Contribution_Level,
  purchase_CSO_rate_302a.303_GL_Account_Key AS Account,
  purchase_CSO_rate_302a.303_Functional_Area AS Functional_Area,
  purchase_CSO_rate_302a.303_FCB_Plant AS FCB_Plant,
  '' AS Partner_Plant,
  purchase_CSO_rate_302a.303_FCB_Material AS FCB_Material,
  '' AS Partner_Material,
  purchase_CSO_rate_302a.303_Customer_Vendor_Number AS Customer_Vendor_Number, 
  '' AS Movement_Type, 
  purchase_CSO_rate_302a.Source_ID,
  purchase_CSO_rate_302a.OPFLAG,
  Sum(-(purchase_CSO_rate_302a.CSO_Amount)) AS Amount_FiFo_LC,
  0 AS Quantity_KG,
  0 AS Volume_L15,
  0 AS Amount_CCS_LC 
FROM
  purchase_CSO_rate_106_2_df purchase_CSO_rate_302a
GROUP BY
  purchase_CSO_rate_302a.303_Company_Code,
  purchase_CSO_rate_302a.303_Profit_Center_Key,
  purchase_CSO_rate_302a.303_Cont_Level,
  purchase_CSO_rate_302a.303_GL_Account_Key,
  purchase_CSO_rate_302a.303_Functional_Area,
  purchase_CSO_rate_302a.303_FCB_Plant,
  purchase_CSO_rate_302a.303_FCB_Material,
  purchase_CSO_rate_302a.303_Customer_Vendor_Number,
  purchase_CSO_rate_302a.Source_ID,
  purchase_CSO_rate_302a.OPFLAG
  """)

# COMMAND ----------

# DBTITLE 1,413: 302_ CSO on purchase calc
df_413 = spark.sql("""
SELECT
  "CSO Purchase" AS Source,
  purchase_CSO_rate_302.303_Company_Code AS Company,
  purchase_CSO_rate_302.303_Profit_Center_Key AS Profit_Center,
  purchase_CSO_rate_302.303_Cont_Level AS Contribution_Level,
  "7240550" AS Account,
  purchase_CSO_rate_302.303_Functional_Area AS Functional_Area,
  '#' AS FCB_Plant,
  '' AS Partner_Plant,
  '#' AS FCB_Material,
  '' AS Partner_Material,
  '#' AS Customer_Vendor_Number, 
  '' AS Movement_Type,
  purchase_CSO_rate_302.Source_ID,
  purchase_CSO_rate_302.OPFLAG,
  Sum(purchase_CSO_rate_302.CSO_Amount) AS Amount_FiFo_LC,
  0 AS Quantity_KG,
  0 AS Volume_L15,
  0 AS Amount_CCS_LC
FROM
  purchase_CSO_rate_106_1_df purchase_CSO_rate_302
GROUP BY
  purchase_CSO_rate_302.303_Company_Code,
  purchase_CSO_rate_302.303_Profit_Center_Key,
  purchase_CSO_rate_302.303_Cont_Level,
  purchase_CSO_rate_302.303_Functional_Area,
  purchase_CSO_rate_302.Source_ID,
  purchase_CSO_rate_302.OPFLAG
  """)

# COMMAND ----------

# DBTITLE 1,413a: 302a_ CSO on exchange calc
df_413a = spark.sql("""
SELECT
 "CSO Exchange" AS Source,
  purchase_CSO_rate_302a.303_Company_Code AS Company,
  purchase_CSO_rate_302a.303_Profit_Center_Key AS Profit_Center,
  purchase_CSO_rate_302a.303_Cont_Level AS Contribution_Level,
  "7240550" AS Account,
  purchase_CSO_rate_302a.303_Functional_Area AS Functional_Area,
  '#' AS FCB_Plant,
  '' AS Partner_Plant,
  '#' AS FCB_Material,
  '' AS Partner_Material,
  '#' AS Customer_Vendor_Number, 
  '' AS Movement_Type,
  purchase_CSO_rate_302a.Source_ID,
  purchase_CSO_rate_302a.OPFLAG,
  Sum(purchase_CSO_rate_302a.CSO_Amount) AS Amount_FiFo_LC,
  0 AS Quantity_KG,
  0 AS Volume_L15,
  0 AS Amount_CCS_LC
FROM
  purchase_CSO_rate_106_2_df purchase_CSO_rate_302a
GROUP BY
  purchase_CSO_rate_302a.303_Company_Code,
  purchase_CSO_rate_302a.303_Profit_Center_Key,
  purchase_CSO_rate_302a.303_Cont_Level,
  purchase_CSO_rate_302a.303_Functional_Area,
  purchase_CSO_rate_302a.Source_ID,
  purchase_CSO_rate_302a.OPFLAG
  """)

# COMMAND ----------

# DBTITLE 1,414: 303_correction
df_correction_303 = spark.sql(f"""
SELECT
  "CSO correction" AS Source,
  hana_c1_df_final_303.Company_Code,
  hana_c1_df_final_303.Profit_Center_Key,
  hana_c1_df_final_303.Cont_Level,
  hana_c1_df_final_303.GL_Account_Key,
  mstr_accts.Account_Name,
  If(hana_c1_df_final_303.Functional_Area = "SV58","Purchase",mstr_accts.LI_Bucket_1) AS LI_Bucket_1,
  If(hana_c1_df_final_303.Functional_Area = "SV58","TFP",mstr_accts.LI_Bucket_2) AS LI_Bucket_2,
  hana_c1_df_final_303.Functional_Area,
  hana_c1_df_final_303.FCB_Plant,
  hana_c1_df_final_303.FCB_Material,
  hana_c1_df_final_303.Customer_Vendor_Number,
  hana_c1_df_final_303.Movement_Type_FCB,
  hana_c1_df_final_303.Source_ID,
  hana_c1_df_final_303.OPFLAG,
  Sum(hana_c1_df_final_303.Amount_in_LC) AS SumOfAmount_in_LC
FROM
  `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_accounts mstr_accts
  RIGHT JOIN hana_c1_df_final_303
  ON trim(lower(mstr_accts.Account)) = trim(lower(hana_c1_df_final_303.GL_Account_Key))
GROUP BY
  hana_c1_df_final_303.Company_Code,
  hana_c1_df_final_303.Profit_Center_Key,
  hana_c1_df_final_303.Cont_Level,
  hana_c1_df_final_303.GL_Account_Key,
  mstr_accts.Account_Name,
  If(hana_c1_df_final_303.Functional_Area = "SV58","Purchase",mstr_accts.LI_Bucket_1),
  If(hana_c1_df_final_303.Functional_Area = "SV58","TFP",mstr_accts.LI_Bucket_2),
  hana_c1_df_final_303.Functional_Area,
  hana_c1_df_final_303.FCB_Plant,
  hana_c1_df_final_303.FCB_Material,
  hana_c1_df_final_303.Customer_Vendor_Number,
  hana_c1_df_final_303.Movement_Type_FCB,
  hana_c1_df_final_303.Source_ID,
  hana_c1_df_final_303.OPFLAG
HAVING
  (
    ((hana_c1_df_final_303.Company_Code) <> "CH01")
    AND ((If(hana_c1_df_final_303.Functional_Area = "SV58","TFP",mstr_accts.LI_Bucket_2)) = "CSO")
    --AND ((hana_c1_df_final_303.FCB_Material) <> "empty")
    AND ((hana_c1_df_final_303.FCB_Material) IS NOT NULL)
  )
  """)

df_correction_303.createOrReplaceTempView("df_correction_303")


df_414 = spark.sql("""
SELECT
  df_correction_303.Source,
  df_correction_303.Company_Code AS Company,
  df_correction_303.Profit_Center_Key AS Profit_Center,
  df_correction_303.Cont_Level AS Contribution_Level,
  "7240550" AS Account,
  df_correction_303.Functional_Area,
  df_correction_303.FCB_Plant,
  '' AS Partner_Plant,
  df_correction_303.FCB_Material,
  '' AS Partner_Material,
  df_correction_303.Customer_Vendor_Number,
  df_correction_303.Movement_Type_FCB AS Movement_Type,
  df_correction_303.Source_ID,
  df_correction_303.OPFLAG,
  sum(-(df_correction_303.SumOfAmount_in_LC)) AS Amount_FiFo_LC,
  0 AS Quantity_KG,
  0 AS Volume_L15,
  0 AS Amount_CCS_LC
FROM
  df_correction_303
GROUP BY
  df_correction_303.Source,
  df_correction_303.Company_Code,
  df_correction_303.Profit_Center_Key,
  df_correction_303.Cont_Level,
  df_correction_303.Functional_Area,
  df_correction_303.FCB_Plant,
  df_correction_303.FCB_Material,
  df_correction_303.Customer_Vendor_Number,
  df_correction_303.Movement_Type_FCB,
  df_correction_303.Source_ID,
  df_correction_303.OPFLAG
""")

# COMMAND ----------

# DBTITLE 1,415: 303_correction
df_415 = spark.sql("""
SELECT
  df_correction_303.Source,
  df_correction_303.Company_Code AS Company,
  df_correction_303.Profit_Center_Key AS Profit_Center,
  df_correction_303.Cont_Level AS Contribution_Level,
  "7240550" AS Account,
  df_correction_303.Functional_Area,
  '#' AS FCB_Plant,
  '' AS Partner_Plant,
  "#" AS FCB_Material,
  '' AS Partner_Material,
  df_correction_303.Customer_Vendor_Number AS Customer_Vendor_Number, 
  '' AS Movement_Type,
  df_correction_303.Source_ID,
  df_correction_303.OPFLAG,
  Sum(df_correction_303.SumOfAmount_in_LC) AS Amount_FiFo_LC,
  0 AS Quantity_KG,
  0 AS Volume_L15,
  0 AS Amount_CCS_LC
FROM
  df_correction_303
GROUP BY
  df_correction_303.Source,
  df_correction_303.Company_Code,
  df_correction_303.Profit_Center_Key,
  df_correction_303.Cont_Level,
  df_correction_303.Functional_Area,
  df_correction_303.Customer_Vendor_Number,
  df_correction_303.Source_ID,
  df_correction_303.OPFLAG
  """)

# COMMAND ----------

# DBTITLE 1,603b: [603a BEHG_TFP_Step1]
df_603b = spark.sql(f"""
  SELECT
  behg_step1.Company_Code,
  behg_step1.GL_Account_Key,
  behg_step1.GL_Account_Name,
  behg_step1.Functional_Area,
  behg_step1.FCB_Plant,
  behg_step1.MM_Plant,
  behg_step1.FCB_Material,
  behg_step1.MM_Material,
  behg_step1.Customer_Vendor_Number,
  behg_step1.Customer_Vendor_Name,
  behg_step1.BEHG_Plant,
  mstr_mtrl.Material_Group_2 AS Material_Group_BEHG,
  behg_step1.BEHG_Mat,
  behg_step1.Source_ID,
  behg_step1.OPFLAG,
  Sum(behg_step1.Weight_in_KG) AS Weight_in_KG
FROM
  `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_behg_tfp_step1 behg_step1
  LEFT JOIN `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_material mstr_mtrl
  ON trim(lower(behg_step1.BEHG_Mat)) = trim(lower(mstr_mtrl.Material))
GROUP BY
  behg_step1.Company_Code,
  behg_step1.GL_Account_Key,
  behg_step1.GL_Account_Name,
  behg_step1.Functional_Area,
  behg_step1.FCB_Plant,
  behg_step1.MM_Plant,
  behg_step1.FCB_Material,
  behg_step1.MM_Material,
  behg_step1.Customer_Vendor_Number,
  behg_step1.Customer_Vendor_Name,
  behg_step1.BEHG_Plant,
  mstr_mtrl.Material_Group_2,
  behg_step1.BEHG_Mat,
  behg_step1.Source_ID,
  behg_step1.OPFLAG    
""")

df_603b.createOrReplaceTempView("behg_step2")

# COMMAND ----------

# DBTITLE 1,603c: [603b: BEHG_sep2]
df_603c = spark.sql(f"""
SELECT
  behg_step2.Company_Code,
  behg_step2.GL_Account_Key,
  behg_step2.Functional_Area,
  "BEHG" AS Margin_Bucket,
  behg_step2.FCB_Plant,
  mstr_mtrl.Material_Group_Finance,
  behg_step2.FCB_Material,
  behg_step2.MM_Plant,
  behg_step2.MM_Material,
  behg_step2.Customer_Vendor_Number,
  behg_step2.Customer_Vendor_Name,
  behg_step2.BEHG_Plant,
  behg_step2.BEHG_Mat,
  behg_step2.Material_Group_BEHG,
  mstr_accts.LI_Bucket_1,
  mstr_accts.LI_Bucket_2,
  108a_tfp.BEHG_to,
  behg_step2.Source_ID,
  behg_step2.OPFLAG,
  Sum(behg_step2.Weight_in_KG) AS SumOfWeight_in_KG,
  Sum(behg_step2.Weight_in_KG) / -1000 * BEHG_to AS BEHG_Amount_LC
FROM
  (
    (
      behg_step2
      LEFT JOIN `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_108a_tfp 108a_tfp
      ON (trim(lower(behg_step2.Company_Code)) = trim(lower(108a_tfp.Country_code))
      AND trim(lower(behg_step2.BEHG_Plant)) = trim(lower(108a_tfp.Plant_GSAP))
      AND trim(lower(behg_step2.Material_Group_BEHG)) = trim(lower(108a_tfp.MatGrp_DACHBook)))
    )
    LEFT JOIN `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_accounts mstr_accts
    ON trim(lower(behg_step2.GL_Account_Key)) = trim(lower(mstr_accts.Account))
  )
  LEFT JOIN `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_material mstr_mtrl
  ON trim(lower(behg_step2.FCB_Material)) = trim(lower(mstr_mtrl.Material))
GROUP BY
  behg_step2.Company_Code,
  behg_step2.GL_Account_Key,
  behg_step2.Functional_Area,
  behg_step2.FCB_Plant,
  mstr_mtrl.Material_Group_Finance,
  behg_step2.FCB_Material,
  behg_step2.MM_Plant,
  behg_step2.MM_Material,
  behg_step2.Customer_Vendor_Number,
  behg_step2.Customer_Vendor_Name,
  behg_step2.BEHG_Plant,
  behg_step2.BEHG_Mat,
  behg_step2.Material_Group_BEHG,
  mstr_accts.LI_Bucket_1,
  mstr_accts.LI_Bucket_2,
  108a_tfp.BEHG_to,
  behg_step2.Source_ID,
  behg_step2.OPFLAG
  """)

df_603c.createOrReplaceTempView("behg_step3")

# COMMAND ----------

# DBTITLE 1,603d: [603c: BEHG_step3]
df_603d = spark.sql(f"""
SELECT
 "BEHG TFP" AS Source,
  behg_step3.Company_Code AS Company,
  '310001' AS Profit_Center,
  'C1' AS Contribution_Level, 
  behg_step3.GL_Account_Key AS Account,
  behg_step3.Functional_Area,
  behg_step3.FCB_Plant,
  behg_step3.MM_Plant AS Partner_Plant,
  behg_step3.FCB_Material,  
  behg_step3.MM_Material AS Partner_Material,
  behg_step3.Customer_Vendor_Number,
  '' AS Movement_Type,
  behg_step3.Source_ID,
  behg_step3.OPFLAG,
  Sum(behg_step3.BEHG_Amount_LC) * -1 AS Amount_FiFo_LC,
  0 AS Quantity_KG,
  0 AS Volume_L15,
  0 AS Amount_CCS_LC
FROM
  behg_step3
GROUP BY
  behg_step3.Company_Code,
  behg_step3.GL_Account_Key,
  behg_step3.Functional_Area,
  behg_step3.FCB_Plant,
  behg_step3.FCB_Material,
  behg_step3.MM_Plant,
  behg_step3.MM_Material,
  behg_step3.Customer_Vendor_Number,
  behg_step3.Source_ID,
  behg_step3.OPFLAG
  """)

# COMMAND ----------

# DBTITLE 1,603e: [603c: BEHG_step3]
df_603e = spark.sql(f"""
SELECT
  "BEHG TFP" AS Source,
  behg_step3.Company_Code AS Company, 
  '310001' AS Profit_Center,
  'C1' AS Contribution_Level, 
  "BEHG_TFP" AS Account,
  behg_step3.Functional_Area,
  behg_step3.FCB_Plant,
  behg_step3.FCB_Material,
  behg_step3.MM_Plant AS Partner_Plant,
  behg_step3.MM_Material AS Partner_Material,
  behg_step3.Customer_Vendor_Number,
  '' AS Movement_Type,
  behg_step3.Source_ID,
  behg_step3.OPFLAG,
  Sum(behg_step3.BEHG_Amount_LC) AS Amount_FiFo_LC,
  0 AS Quantity_KG,
  0 AS Volume_L15,
  0 AS Amount_CCS_LC
FROM
  behg_step3
GROUP BY
  behg_step3.Company_Code,
  "BEHG TFP",
  "BEHG_TFP",
  behg_step3.Functional_Area,
  behg_step3.FCB_Plant,
  behg_step3.FCB_Material,
  behg_step3.MM_Plant,
  behg_step3.MM_Material,
  behg_step3.Customer_Vendor_Number,
  behg_step3.Source_ID,
  behg_step3.OPFLAG
  """)

# COMMAND ----------

# MAGIC %md
# MAGIC                                                   **DATA DUMP OUTPUT**

# COMMAND ----------

# DBTITLE 1,417_303_Vendor_Master
df_303_Vendor_Master = spark.sql("""SELECT
  hana_c1_df_final_303.Company_Code,
  hana_c1_df_final_303.Customer_Vendor_Number,
  hana_c1_df_final_303.Customer_Vendor_Name
FROM
  hana_c1_df_final_303
GROUP BY
  hana_c1_df_final_303.Company_Code,
  hana_c1_df_final_303.Customer_Vendor_Number,
  hana_c1_df_final_303.Customer_Vendor_Name
 
  """)

df_303_Vendor_Master.createOrReplaceTempView("customer_vendor_master")

# COMMAND ----------

# DBTITLE 1,417_203: MASTER-Accounts

# # -- "INSERT INTO 603A: BEHG_TFP_Step1 ( Company_Code, GL_Account_Key, GL_Account_Name, Functional_Area, FCB_Plant, Plant, MM_Plant, FCB_Material, Material_Key, MM_Material, Customer_Vendor_Number, Customer_Vendor_Name, BEHG_Plant, BEHG_Mat, Weight_in_KG, LI_Bucket_1, LI_Bucket_2 )
# df_603a = spark.sql(f"""
# SELECT
#   hana_c1_df_final_303.Company_Code,
#   hana_c1_df_final_303.GL_Account_Key,
#   hana_c1_df_final_303.GL_Account_Name,
#   hana_c1_df_final_303.Functional_Area,
#   hana_c1_df_final_303.FCB_Plant,
#   hana_c1_df_final_303.Plant,
#   hana_c1_df_final_303.MM_Plant,
#   hana_c1_df_final_303.FCB_Material,
#   hana_c1_df_final_303.Material_Key,
#   hana_c1_df_final_303.MM_Material,
#   hana_c1_df_final_303.Customer_Vendor_Number,
#   hana_c1_df_final_303.Customer_Vendor_Name,
#   If(LI_Bucket_1 = "Purchase",MM_Plant,FCB_Plant) AS BEHG_Plant,
#   If(FCB_Material <> MM_Material,If(Functional_Area = "SV30",MM_Material,FCB_Material),FCB_Material) AS BEHG_Mat,
#   Sum(hana_c1_df_final_303.Weight_in_KG) AS SumOfWeight_in_KG,
#   mstr_accts.LI_Bucket_1,
#   mstr_accts.LI_Bucket_2
# FROM
#   hana_c1_df_final_303
#   LEFT JOIN `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_accounts mstr_accts
#   ON hana_c1_df_final_303.GL_Account_Key = mstr_accts.Account
# GROUP BY
#   hana_c1_df_final_303.Company_Code,
#   hana_c1_df_final_303.GL_Account_Key,
#   hana_c1_df_final_303.GL_Account_Name,
#   hana_c1_df_final_303.Functional_Area,
#   hana_c1_df_final_303.FCB_Plant,
#   hana_c1_df_final_303.Plant,
#   hana_c1_df_final_303.MM_Plant,
#   hana_c1_df_final_303.FCB_Material,
#   hana_c1_df_final_303.Material_Key,
#   hana_c1_df_final_303.MM_Material,
#   hana_c1_df_final_303.Customer_Vendor_Number,
#   hana_c1_df_final_303.Customer_Vendor_Name,
#   If(LI_Bucket_1 = "Purchase",MM_Plant,FCB_Plant),
#   If(FCB_Material <> MM_Material,If(Functional_Area = "SV30",MM_Material,FCB_Material),FCB_Material),
#   mstr_accts.LI_Bucket_1,
#   mstr_accts.LI_Bucket_2,
#   If(hana_c1_df_final_303.FCB_Plant = hana_c1_df_final_303.MM_Plant,If(hana_c1_df_final_303.FCB_Material <> hana_c1_df_final_303.MM_Material,"MatBlend", "no"),"no")
# HAVING
#   (
#     ((hana_c1_df_final_303.Company_Code) = "DE01") AND ((Sum(hana_c1_df_final_303.Weight_in_KG)) <> 0)
#     AND (
#       (mstr_accts.LI_Bucket_1) = "Sales"
#       Or (mstr_accts.LI_Bucket_1) = "Purchase"
#       Or (mstr_accts.LI_Bucket_1) = "Stock change"
#     )
#     AND (
#       (mstr_accts.LI_Bucket_2) = "TFP"
#       Or (mstr_accts.LI_Bucket_2) = "TFP cust"
#     )
#     AND (
#       (If(hana_c1_df_final_303.FCB_Plant = hana_c1_df_final_303.MM_Plant,
#           If(hana_c1_df_final_303.FCB_Material <> hana_c1_df_final_303.MM_Material, "MatBlend", "no"),"no")) = "no"
#         )
#   )
# """)

# COMMAND ----------

# DBTITLE 1,union: datadump_collection
df_output = df_402.unionByName(df_402a).unionByName(df_403).unionByName(df_404).unionByName(df_405).unionByName(df_406).unionByName(df_407).unionByName(df_412).unionByName(df_412a).unionByName(df_413).unionByName(df_413a).unionByName(df_414).unionByName(df_415).unionByName(df_604b).unionByName(df_604c).unionByName(df_603d).unionByName(df_603e)
df_output = df_output.withColumn("Year",lit(prev_1st_year))\
                     .withColumn("Month",lit(prev_1st_month))

df_output.createOrReplaceTempView("data_dump_collection")

# COMMAND ----------

# DBTITLE 1,Update queries: 409-411 & 419-422
df_ddc = spark.sql(f"""
SELECT 
Source,
Company,
COALESCE(CASE WHEN TRIM(Profit_Center) = '' THEN NULL ELSE Profit_Center END,'310001') AS Profit_Center,
COALESCE(CASE WHEN TRIM(Contribution_Level) = '' THEN NULL ELSE Contribution_Level END,'C1') AS Contribution_Level,
COALESCE(CASE WHEN TRIM(Account) = '' THEN NULL ELSE Account END,'#/#') AS Account,
COALESCE(CASE WHEN TRIM(Functional_Area) = '' THEN NULL ELSE Functional_Area END,'CO') AS Functional_Area,
COALESCE(FCB_Plant,'#') AS FCB_Plant,
Partner_Plant,
COALESCE(CASE WHEN TRIM(FCB_Material) = '' THEN NULL ELSE TRIM(FCB_Material) END,'#') AS FCB_Material,
Partner_Material,
COALESCE(CASE WHEN TRIM(Customer_Vendor_Number) = '' THEN NULL ELSE Customer_Vendor_Number END,'#') AS Customer_Vendor_Number,
Movement_Type,
Source_ID,
OPFLAG,
Amount_FiFo_LC,
Quantity_KG,
Volume_L15,
Amount_CCS_LC,
Year,
Month,
CURRENT_TIMESTAMP() AS ingested_at
FROM data_dump_collection
""")

df_ddc = df_ddc.withColumn("Source_ID", when(df_ddc.Source_ID == "Flat_000",concat(lit(ddo_source_system_id),lit('_110'))).otherwise(df_ddc.Source_ID))

df_ddc.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{ddc_table_path}/")
df_ddc.createOrReplaceTempView("data_dump_collection")
print("DDC init written")

# COMMAND ----------

#function to check if file exists nned to delete added by Sudha
def file_exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as e:
        if 'java.io.FileNotFoundException' in str(e):
            return False
        else:
            raise

# COMMAND ----------

# DBTITLE 1,function to check if file exists
#function to check if file exists
def file_exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as e:
        if 'java.io.FileNotFoundException' in str(e):
            return False
        else:
            raise

# method to get table
def ddc_table_name(path,uc_catalog_name,uc_curated_schema,data_layer, table_list1):
    df_schema_table = spark.sql(f"SHOW TABLES in `{uc_catalog_name}`.`{uc_curated_schema}`").select("tableName")
    table_list2 = [row[0] for row in df_schema_table.select('tableName').collect()]
    table_list = [i for i in table_list1 if i in table_list2]
    file_list = []

    for i in dbutils.fs.ls(path):
        table_name = i[0]
        file_list.append(table_name)

    final_file_list = [i for i in file_list if (i.split("/")[-2]).lower() in table_list1]    
    for i in final_file_list:
        table_name = (i.split("/")[-2]).lower()
        table_path = i
        if file_exists(table_path) is True:
            if table_name in table_list:
                print("Table already exists in UC : ", table_name )
                table_name = f'`{uc_catalog_name}`.`{uc_curated_schema}`.{table_name}'
                return table_name
            else:
                print("TABLE DOESN'T EXIST" ,table_name)
                temp_view = "data_dump_collection"
                return temp_view

ddc_table_list = ['use_case_mars_data_dump_collection']
ddc_table = ddc_table_name(curated_folder_path,uc_catalog_name,uc_curated_schema,"curated", ddc_table_list)
print(ddc_table)

# COMMAND ----------

# DBTITLE 1,417: 301a_Datadump output
df_417 = spark.sql(f"""
SELECT
data_dump_collection.Source AS Source,
data_dump_collection.Company AS Company,
data_dump_collection.Profit_Center AS Profit_Center,
data_dump_collection.Year AS `Year`,
data_dump_collection.Month AS `Month`,
data_dump_collection.Contribution_Level AS Contribution_Level,
data_dump_collection.Account AS Account,
mstr_accts.Account_Name AS Account_Name,
mstr_accts.LI_bucket_1 AS LI_bucket_1,
mstr_accts.LI_bucket_2 AS LI_bucket_2,
data_dump_collection.FCB_Plant AS FCB_Plant,
mstr_plnt.Plant_Name AS FCB_Plant_Name,
data_dump_collection.Partner_Plant AS Partner_Plant,
mstr_plnt_1.Plant_Name AS Partner_Plant_Name,
data_dump_collection.FCB_Material AS FCB_Material,
mstr_mtrl.Material_Name AS Material_Name,
mstr_mtrl.Material_Group_Finance AS Material_Group,
mstr_mtrl.Bio AS Bio_group_FCB_Material,
data_dump_collection.Partner_Material AS Partner_Material,
mstr_mtrl_1.Material_Name AS Partner_Material_Name,
mstr_mtrl_1.Material_Group_Finance AS Partner_Material_Group,
mstr_mtrl_1.Bio AS Bio_group_Partner_Material,
data_dump_collection.Functional_Area AS Functional_Area,
master_functional_area.FA_Grouping AS Channel,
data_dump_collection.Customer_Vendor_Number AS Customer_Vendor_Number,
customer_vendor_master.Customer_Vendor_Name AS Customer_Vendor_Name,
data_dump_collection.Movement_Type AS Movement_Type,
mstr_mvttyp.Manual_Grouping AS Movement_Type_Group,
data_dump_collection.Source_ID,
data_dump_collection.OPFLAG,
data_dump_collection.Quantity_KG AS Quantity_KG,
data_dump_collection.Volume_L15 AS Volume_L15,
Sum(data_dump_collection.Amount_FiFo_LC) AS Amount_FiFo_LC,
Sum(data_dump_collection.Amount_CCS_LC) AS Amount_CCS_LC

FROM
  (
    customer_vendor_master
    RIGHT JOIN (
      `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_fa master_functional_area
      RIGHT JOIN (
        (
          (
            (
              (
                {ddc_table} data_dump_collection
                LEFT JOIN `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_accounts mstr_accts
                ON trim(lower(data_dump_collection.Account)) = trim(lower(mstr_accts.Account))
              )
              LEFT JOIN `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_plant mstr_plnt
              ON trim(lower(data_dump_collection.FCB_Plant)) = trim(lower(mstr_plnt.Plant))
            )
            LEFT JOIN `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_plant AS mstr_plnt_1
            ON trim(lower(data_dump_collection.Partner_Plant)) = trim(lower(mstr_plnt_1.Plant))
          )
          LEFT JOIN `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_material mstr_mtrl
          ON trim(lower(data_dump_collection.FCB_Material)) = trim(lower(mstr_mtrl.Material))
        )
        LEFT JOIN `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_material AS mstr_mtrl_1
        ON trim(lower(data_dump_collection.Partner_Material)) = trim(lower(mstr_mtrl_1.Material))
      )ON trim(lower(master_functional_area.Functional_Area)) = trim(lower(data_dump_collection.Functional_Area))
    ) ON (
      trim(lower(customer_vendor_master.Customer_Vendor_Number)) = trim(lower(data_dump_collection.Customer_Vendor_Number))
    )
    AND (
      trim(lower(customer_vendor_master.Company_Code)) = trim(lower(data_dump_collection.Company))
    )
  )
  LEFT JOIN `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_mvt_type mstr_mvttyp
  ON trim(lower(data_dump_collection.Movement_Type)) = trim(lower(mstr_mvttyp.MvT))
GROUP BY
  data_dump_collection.Source,
  data_dump_collection.Company,
  data_dump_collection.Profit_Center,
  data_dump_collection.Year,
  data_dump_collection.Month,
  data_dump_collection.Contribution_Level,
  data_dump_collection.Account,
  mstr_accts.Account_Name,
  mstr_accts.LI_bucket_1,
  mstr_accts.LI_bucket_2,
  data_dump_collection.FCB_Plant,
  mstr_plnt.Plant_Name,
  data_dump_collection.Partner_Plant,
  mstr_plnt_1.Plant_Name,
  data_dump_collection.FCB_Material,
  mstr_mtrl.Material_Name,
  mstr_mtrl.Material_Group_Finance,
  mstr_mtrl.Bio,
  data_dump_collection.Partner_Material,
  mstr_mtrl_1.Material_Name,
  mstr_mtrl_1.Material_Group_Finance,
  mstr_mtrl_1.Bio,
  data_dump_collection.Functional_Area,
  master_functional_area.FA_Grouping,
  data_dump_collection.Customer_Vendor_Number,
  customer_vendor_master.Customer_Vendor_Name,
  data_dump_collection.Movement_Type,
  mstr_mvttyp.Manual_Grouping,
  data_dump_collection.Source_ID,
  data_dump_collection.OPFLAG,
  data_dump_collection.Quantity_KG,
  data_dump_collection.Volume_L15,
  data_dump_collection.Amount_FiFo_LC""")
# HAVING ((data_dump_collection.Amount_FiFo_LC)<> 0)""")

df_417.createOrReplaceTempView("data_dump_output")

# COMMAND ----------

# DBTITLE 1,Update queries DDO
df_ddo = spark.sql(
    f"""
SELECT
Source,
Company,
Profit_Center,
Year,
Month,
Contribution_Level,
Account,
Account_Name,
LI_bucket_1,
LI_bucket_2,
FCB_Plant,
FCB_Plant_Name,
Partner_Plant,
Partner_Plant_Name,
FCB_Material,
Material_Name,
Material_Group,
Bio_group_FCB_Material,
Partner_Material,
Partner_Material_Name,
Partner_Material_Group,
Bio_group_Partner_Material,
Functional_Area,
Channel,
Customer_Vendor_Number,
Customer_Vendor_Name,
Movement_Type,
Movement_Type_Group,
Source_ID,
OPFLAG,
CASE WHEN LI_bucket_2 = 'ED' OR LI_bucket_2 = 'CSO' THEN 0 ELSE Quantity_KG END AS Quantity_KG,
CASE WHEN LI_bucket_2 = 'ED' OR LI_bucket_2 = 'CSO' THEN 0 ELSE Volume_L15 END AS Volume_L15,
Amount_FiFo_LC,
Amount_CCS_LC,
CURRENT_TIMESTAMP() AS ingested_at
FROM data_dump_output
""")

# COMMAND ----------

# DBTITLE 1,Text tables
#Company_Name_Text
comp_md_df = spark.sql(f"""select `Company_Code`, `Name_Comp_Code`  AS `Company_Name` from `{uc_catalog_name}`.`{uc_eh_md_schema}`.md_fi_comp_code""")

#Profit_Center_Text
profit_center_df = spark.sql(f"""select `Profit_Center` AS `Profit_Center_md`, `Profit_Center_Text` from `{uc_catalog_name}`.`{uc_eh_md_schema}`.md_fi_profit_center""")
profit_center_df = profit_center_df.withColumn("Profit_Center_md",regexp_replace(col("Profit_Center_md"), r'^[0]*', ''))

# COMMAND ----------

# DBTITLE 1,Concat of Attribute and Text
#Companycode and Company_Name_Text
df_ddo_cmp_text = df_ddo.join(comp_md_df, trim(lower(df_ddo.Company)) == trim(lower(comp_md_df.Company_Code)), "inner")  
df_ddo_cmp_text = df_ddo_cmp_text.withColumn('Company_Code-Text',concat_ws('-', df_ddo_cmp_text.Company,df_ddo_cmp_text.Company_Name))                  
df_ddo_cmp_text = df_ddo_cmp_text.select(*[col(column_name) for column_name in df_ddo_cmp_text.columns if column_name not in {'Company_Code','Company_Name'}])

#Profit_Center and Profit_Center_Text
df_ddo_pc_text = df_ddo_cmp_text.join(profit_center_df, trim(lower(df_ddo_cmp_text.Profit_Center)) == trim(lower(profit_center_df.Profit_Center_md)), "leftouter")  
df_ddo_pc_text = df_ddo_pc_text.withColumn('Profit_Center-Text',concat_ws('-', df_ddo_pc_text.Profit_Center,df_ddo_pc_text.Profit_Center_Text))                  
df_ddo_pc_text = df_ddo_pc_text.select(*[col(column_name) for column_name in df_ddo_pc_text.columns if column_name not in {'Profit_Center_md','Profit_Center_Text'}])

#Account and Account_Text
df_ddo_ac_text = df_ddo_pc_text.withColumn('Account-Text',concat_ws('-', df_ddo_pc_text.Account,df_ddo_pc_text.Account_Name))  

#Plant and Plant_Text
df_ddo_final = df_ddo_ac_text.withColumn('Plant-Text',concat_ws('-', df_ddo_ac_text.FCB_Plant,df_ddo_ac_text.FCB_Plant_Name))

# COMMAND ----------

# DBTITLE 1,delete current month's data if exists
def ddo_duplicate_cleanup(path,uc_catalog_name,uc_curated_schema,data_layer, table_list1):
    print("")
    print("uc_catalog_name : ",uc_catalog_name)
    print("uc_curated_schema : ",uc_curated_schema)
    print("inside1")
    df_schema_table = spark.sql(f"SHOW TABLES in `{uc_catalog_name}`.`{uc_curated_schema}`").select("tableName")
    print("inside")
    table_list2 = [row[0] for row in df_schema_table.select('tableName').collect()]
    table_list = [i for i in table_list1 if i in table_list2]
    file_list = []
    for i in dbutils.fs.ls(path):

        print("")
        table_name = i[0]
        file_list.append(table_name)

    final_file_list = [i for i in file_list if (i.split("/")[-2]).lower() in table_list1]
    print(table_list1)
    print("final_file_list : ", final_file_list)
    
    for i in final_file_list:

        print("")
        table_name = (i.split("/")[-2]).lower()
        table_path = i
        print("table_name : " , table_name)
        print("table_path : " , table_path)
        if file_exists(table_path) is True:
            print(table_path)
            if table_name in table_list:
                print("Table already exists in UC : ", table_name )
                spark.sql(f"""DELETE FROM `{uc_catalog_name}`.`{uc_curated_schema}`.{ddo_table_name}
                              WHERE Year = {prev_1st_year} AND Month = '{prev_1st_month}'
                          """)
            else:
                print("TABLE DOESN'T EXIST" ,table_name)

ddo_table_list = ['use_case_mars_data_dump_output']
ddo_duplicate_cleanup(curated_folder_path,uc_catalog_name,uc_curated_schema,"curated", ddo_table_list)

# COMMAND ----------

df_ddo_final = df_ddo_final.withColumn("Source_ID", when(df_ddo_final.Source_ID == "Flat_000",concat(lit(ddo_source_system_id),lit('_110'))).otherwise(df_ddo_final.Source_ID))

# COMMAND ----------

# DBTITLE 1,Writing DDO to abfss
df_ddo_final.write.format("delta").mode("append").option("mergeSchema", "true").save(f"{ddo_table_path}/")
print("DDO init written")
df_ddo_final.createOrReplaceTempView("df_ddo_final")

# COMMAND ----------

# DBTITLE 1,delete data older than retention months
def ddo_cleanup(path,uc_catalog_name,uc_curated_schema,data_layer, table_list1):
    print("")
    print("uc_catalog_name : ",uc_catalog_name)
    print("uc_curated_schema : ",uc_curated_schema)
    print("inside1")
    df_schema_table = spark.sql(f"SHOW TABLES in `{uc_catalog_name}`.`{uc_curated_schema}`").select("tableName")
    print("inside")
    table_list2 = [row[0] for row in df_schema_table.select('tableName').collect()]
    table_list = [i for i in table_list1 if i in table_list2]
    file_list = []
    for i in dbutils.fs.ls(path):

        print("")
        table_name = i[0]
        file_list.append(table_name)

    final_file_list = [i for i in file_list if (i.split("/")[-2]).lower() in table_list1]
    print(table_list1)
    print("final_file_list : ", final_file_list)
    
    for i in final_file_list:

        print("")
        table_name = (i.split("/")[-2]).lower()
        table_path = i
        print("table_name : " , table_name)
        print("table_path : " , table_path)
        if file_exists(table_path) is True:
            print(table_path)
            if table_name in table_list:
                print("Table already exists in UC : ", table_name )
                spark.sql(f"""DELETE FROM `{uc_catalog_name}`.`{uc_curated_schema}`.{ddo_table_name} 
                              WHERE CONCAT(Year,'_',Month) <= 
                              (SELECT CONCAT(year(add_months(current_date(), -No_of_Months))
                                             ,'_'
                                             ,LPAD(month(add_months(current_date(), -No_of_Months)), 3, '0')
                                            ) AS Year_Month
                               FROM `{uc_catalog_name}`.`{uc_curated_schema}`.mars_data_retention_period
                               ORDER BY ingested_at DESC
                               LIMIT 1
                               )
                               AND CONCAT(Year, '_', Month) NOT IN (SELECT CONCAT(Year, '_', Month)
                               FROM `{uc_catalog_name}`.`{uc_curated_schema}`.mars_year_month_control_table)
                            """)
                
            else:
                print("TABLE DOESN'T EXIST" ,table_name)
                
ddo_table_list = ['use_case_mars_data_dump_output']
ddo_cleanup(curated_folder_path,uc_catalog_name,uc_curated_schema,"curated", ddo_table_list)
