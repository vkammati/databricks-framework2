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
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp,date_format
from pyspark.sql.functions import col, trim, lit, regexp_replace, col, concat, concat_ws
from datetime import datetime
from delta.tables import *
spark = SparkSession.getActiveSession()

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession()

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

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

mbc_source_system_id = mars_source_system_id()

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

df_year_month = spark.sql(f"""SELECT DISTINCT year,month
                             FROM `{uc_catalog_name}`.`{uc_curated_schema}`.mars_year_month_control_table
                             WHERE UPPER(Active_Flag) = 'Y'
                         """)
cnt = df_year_month.count()
print(cnt)

if cnt == 1:
    prev_1st_month = df_year_month.collect()[0][1]
    prev_1st_year = df_year_month.collect()[0][0]

print(prev_1st_year,prev_1st_month)

# COMMAND ----------

# filter 'column' with the language key
def language_filter(df):
    filtered_df = df.filter(df.SPRAS=="E") 
    return filtered_df

# filter 'column' with the client value 
def client_filter(df):
    filtered_df = df.filter(df.MANDT==110) 
    return filtered_df

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
def ddo_table_name(path,uc_catalog_name,uc_curated_schema,data_layer, table_list1):
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
                temp_view = "df_ddo_final"
                return temp_view

ddo_table_list = ['use_case_mars_data_dump_output']
ddo_table = ddo_table_name(curated_folder_path,uc_catalog_name,uc_curated_schema,"curated", ddo_table_list)
print(ddo_table)

# COMMAND ----------

mbc_table_name="USE_CASE_MARS_MARGIN_BUCKET_COLLECTION"
mbc_table_path = curated_folder_path+"/"+mbc_table_name
print("MBC",mbc_table_name)
print("MBC_path",mbc_table_path)

comp_md_df = spark.sql(f"""select `Company_Code`, `Name_Comp_Code`  AS `Company_Name` from `{uc_catalog_name}`.`{uc_eh_md_schema}`.md_fi_comp_code""")

# COMMAND ----------

# DBTITLE 1,360 sales logistics
df_logistics_360 = spark.sql(f"""
SELECT ddo.Company, 
ddo.Contribution_Level, 
ddo.LI_bucket_1, 
ddo.FCB_Plant, 
ddo.FCB_Plant_Name, 
ddo.FCB_Material, 
ddo.Material_Name, 
mstr_mtrl.TFP_Logistics_Cost_Mapping, 
ddo.Functional_Area, 
ddo.Channel,
ddo.Source_ID,
ddo.OPFLAG,
sum(ddo.Quantity_KG) AS SumOfQuantity_KG, 
sum(ddo.Volume_L15) AS SumOfVolume_L15
FROM {ddo_table} ddo 
LEFT JOIN `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_material mstr_mtrl
ON trim(UPPER(ddo.FCB_Material)) = trim(UPPER(mstr_mtrl.Material))
WHERE ddo.Year = {prev_1st_year} AND ddo.Month = {prev_1st_month}
GROUP BY ddo.Company, 
ddo.Contribution_Level, 
ddo.LI_bucket_1, 
ddo.FCB_Plant, 
ddo.FCB_Plant_Name, 
ddo.FCB_Material, 
ddo.Material_Name, 
mstr_mtrl.TFP_Logistics_Cost_Mapping, 
ddo.Functional_Area, 
ddo.Channel,
ddo.Source_ID,
ddo.OPFLAG
HAVING (((ddo.LI_bucket_1)="Sales") AND ((ddo.Functional_Area) NOT IN ("SV30", "SV31") AND NVL(SumOfQuantity_KG,1) <> 0))                                          
""")

df_logistics_360.createOrReplaceTempView("df_logistics_360")

# COMMAND ----------

# DBTITLE 1,361_Logistics_FTP
df_Logistic_recovery_in_C1_361 = spark.sql(f"""
SELECT
  sales_for_logistics.Company AS Company,
  sales_for_logistics.FCB_Plant AS FCB_Plant,
  sales_for_logistics.FCB_Plant_Name AS FCB_Plant_Name,
  sales_for_logistics.FCB_Material AS FCB_Material,
  sales_for_logistics.Material_Name AS Material_Name,
  sales_for_logistics.Channel AS Channel,
  sum(sales_for_logistics.SumOfQuantity_KG) AS SumOfSumOfQuantity_KG,
  sum(sales_for_logistics.SumOfVolume_L15) AS SumOfSumOfVolume_L15,
  logistics_tfp.PT_Cost_LC_To AS PT_Cost_LC_To,
  logistics_tfp.SH_LC_To AS SH_LC_To,
  logistics_tfp.PT_Cost_LC_Cbm AS PT_Cost_LC_Cbm,
  logistics_tfp.SH_LC_Cbm AS SH_LC_Cbm,
  sales_for_logistics.Source_ID,
  sales_for_logistics.OPFLAG,
  (SumOfSumOfVolume_L15 / 1000 * (PT_Cost_LC_Cbm + SH_LC_Cbm)) AS Logistic_exp_VOL,
  (SumOfSumOfQuantity_KG / 1000 * (PT_Cost_LC_To + SH_LC_To)) AS Logistic_exp_WEIGHT,
  (Logistic_exp_VOL + Logistic_exp_WEIGHT) AS Expenses
FROM df_logistics_360 sales_for_logistics
  LEFT JOIN `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_logistics_tfp logistics_tfp
   ON trim(upper(sales_for_logistics.Company)) = trim(upper(logistics_tfp.Company_Code))
  AND trim(UPPER(sales_for_logistics.FCB_plant)) = trim(UPPER(logistics_tfp.FCB_plant))
  AND trim(upper(sales_for_logistics.TFP_Logistics_Cost_Mapping)) = trim(upper(logistics_tfp.TFP_Map_logistics))
GROUP BY
  sales_for_logistics.Company,
  sales_for_logistics.FCB_Plant,
  sales_for_logistics.FCB_Plant_Name,
  sales_for_logistics.FCB_Material,
  sales_for_logistics.Material_Name,
  sales_for_logistics.Channel,
  logistics_tfp.PT_Cost_LC_To,
  logistics_tfp.SH_LC_To,
  logistics_tfp.PT_Cost_LC_Cbm,
  logistics_tfp.SH_LC_Cbm,
  sales_for_logistics.Source_ID,
  sales_for_logistics.OPFLAG
HAVING
  sales_for_logistics.Channel NOT LIKE 'CN'                                           
""")

df_Logistic_recovery_in_C1_361.createOrReplaceTempView("df_Logistic_recovery_in_C1_361")

# COMMAND ----------

# DBTITLE 1,455:361 Logistics
df_logistics_455 = spark.sql(f"""
SELECT 
   'Logistics' AS Margin_Bucket, 
   df_Logistic_recovery_in_C1_361.Company AS Company, 
   df_Logistic_recovery_in_C1_361.FCB_Plant AS Plant, 
   mstr_plnt.Plant_Name AS Plant_Name, 
   df_Logistic_recovery_in_C1_361.FCB_Material AS FCB_Material, 
   mstr_mtrl.Material_Name AS Material_Name, 
   mstr_mtrl.Material_Group_Finance AS Material_Group_Finance, 
   mstr_mtrl.Bio AS Bio_Group,
   df_Logistic_recovery_in_C1_361.Source_ID,
   df_Logistic_recovery_in_C1_361.OPFLAG,
   sum(df_Logistic_recovery_in_C1_361.SumOfSumOfQuantity_KG) AS SumOfQuantity, 
   (-1 * sum(df_Logistic_recovery_in_C1_361.Expenses)) AS SumOfC1_FifO 
   FROM df_Logistic_recovery_in_C1_361
   LEFT JOIN `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_plant mstr_plnt
   ON trim(UPPER(df_Logistic_recovery_in_C1_361.FCB_Plant)) = trim(UPPER(mstr_plnt.Plant))
   LEFT JOIN `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_material mstr_mtrl
   ON trim(UPPER(df_Logistic_recovery_in_C1_361.FCB_Material)) = trim(UPPER(mstr_mtrl.Material))
   GROUP BY 
      df_Logistic_recovery_in_C1_361.Company, 
      df_Logistic_recovery_in_C1_361.FCB_Plant, 
      mstr_plnt.Plant_Name, 
      df_Logistic_recovery_in_C1_361.FCB_Material, 
      mstr_mtrl.Material_Name, 
      mstr_mtrl.Material_Group_Finance, 
      mstr_mtrl.Bio,
      df_Logistic_recovery_in_C1_361.Source_ID,
      df_Logistic_recovery_in_C1_361.OPFLAG
""")
df_logistics_455.createOrReplaceTempView("df_logistics_455")

# COMMAND ----------

# DBTITLE 1,418: price effect stock - create table
# df_418 = spark.sql(f"""
# INSERT INTO `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_stock_price_effect
# SELECT
#   ddo.Company,
#   ddo.FCB_Plant,
#   ddo.FCB_Material,
#   ddo.LI_bucket_1,
#   ddo.LI_bucket_2,
#   Sum(ddo.Quantity_KG) AS SumOfQuantity_KG,
#   Sum(ddo.Amount_FiFo_LC) AS SumOfAmount_FiFo_LC,
#   -(SumOfAmount_FiFo_LC / SumOfQuantity_KG * 1000) AS TFP 
# FROM {ddo_table} ddo
# WHERE ddo.Year = {prev_1st_year} AND ddo.Month = {prev_1st_month}
# GROUP BY
#   ddo.Company,
#   ddo.FCB_Plant,
#   ddo.FCB_Material,
#   ddo.LI_bucket_1,
#   ddo.LI_bucket_2
# HAVING
#   (((ddo.LI_bucket_2) = "TFP") AND ((Sum(ddo.Quantity_KG)) <> 0))
# """)

# COMMAND ----------

# DBTITLE 1,451: append C1 to margin view
df_451 = spark.sql(f"""
SELECT 
   'C1 FiFo' AS Margin_Bucket, 
   ddo.Company AS Company, 
   ddo.FCB_Plant AS Plant,
   ddo.FCB_Plant_Name AS Plant_Name,
   ddo.FCB_Material AS FCB_Material,
   ddo.Material_Name AS Material_Name,
   ddo.Material_Group AS Material_Group_Finance,
   ddo.Bio_group_FCB_Material AS Bio_Group,
   ddo.Source_ID,
   ddo.OPFLAG,
   SUM(ddo.Quantity_KG) AS SumOfQuantity,
   SUM(ddo.Amount_FiFo_LC) AS SumOfC1_Fifo
FROM {ddo_table} ddo
WHERE ddo.Year = {prev_1st_year} AND ddo.Month = {prev_1st_month}
GROUP BY 
   ddo.Company, 
   ddo.FCB_Plant,
   ddo.FCB_Plant_Name,
   ddo.FCB_Material,
   ddo.Material_Name,
   ddo.Material_Group,
   ddo.Bio_group_FCB_Material,
   ddo.LI_bucket_2,
   ddo.Source_ID,
   ddo.OPFLAG
HAVING ddo.Company IS NOT NULL 
   AND ROUND(SUM(ddo.Amount_FiFo_LC),4) <> 0
   AND UPPER(ddo.LI_bucket_2) NOT LIKE 'COSA'
""")

# COMMAND ----------

# DBTITLE 1,310 & 311
df_310 = spark.sql(f"""
SELECT 
   ddo.Company, 
   ddo.Account, 
   ddo.LI_bucket_2, 
   ddo.FCB_Plant, 
   ddo.FCB_Material, 
   ddo.Material_Name, 
   ddo.Material_Group, 
   ddo.Bio_group_FCB_Material,
   ddo.Source_ID,
   ddo.OPFLAG,
   Sum(ddo.Quantity_KG) AS SumOfQuantity_KG, 
   Sum(ddo.Amount_FiFo_LC) AS SumOfAmount_FiFo_LC, 
   (SumOfAmount_FiFo_LC/SumOfQuantity_KG) * 1000 AS price_per_to, 
   CASE 
    WHEN LI_Bucket_2 = 'Opening Stock' THEN SumOfQuantity_KG 
    ELSE 0 
   END AS Opening_Stock, 
   CASE 
    WHEN LI_Bucket_2 = 'Closing Stock' THEN -1 * (SumOfQuantity_KG) 
    ELSE 0 
   END AS Closing_Stock, 
   CASE 
    WHEN LI_Bucket_2 = 'Opening Stock' THEN -1 * (SumOfAmount_FiFo_LC) 
    ELSE 0 
   END AS FiFo_Amount_opening, 
   CASE 
    WHEN LI_Bucket_2 = 'Closing Stock' THEN SumOfAmount_FiFo_LC 
    ELSE 0 
   END AS FiFo_Amount_closing,
   FiFo_Amount_opening/Opening_Stock * 1000 AS FiFo_opening
FROM {ddo_table} ddo
WHERE ddo.Year = {prev_1st_year} AND ddo.Month = {prev_1st_month}
GROUP BY 
   ddo.Company, 
   ddo.Account, 
   ddo.LI_bucket_2, 
   ddo.FCB_Plant, 
   ddo.FCB_Material, 
   ddo.Material_Name, 
   ddo.Material_Group, 
   ddo.Bio_group_FCB_Material,
   ddo.Source_ID,
   ddo.OPFLAG
HAVING (LOWER(ddo.LI_bucket_2) = LOWER('Opening Stock')
       Or LOWER(ddo.LI_bucket_2) = LOWER('Closing Stock')) 
        AND (Sum(ddo.Quantity_KG)<>0)
""")
df_310.createOrReplaceTempView("df_310")

df_311 = spark.sql(f"""
SELECT 
   df_310.Company, 
   df_310.FCB_Plant, 
   mstr_plnt.Plant_Name, 
   mstr_plnt.CSO_depot, 
   df_310.FCB_Material, 
   df_310.Material_Name, 
   df_310.Material_Group, 
   df_310.Bio_group_FCB_Material,
   df_310.Source_ID,
   df_310.OPFLAG,
   Sum(df_310.Opening_Stock) AS SumOfOpening_Stock, 
   Sum(df_310.FiFo_Amount_opening) AS SumOfFiFo_Amount_opening, 
   Sum(df_310.Closing_Stock) AS SumOfClosing_Stock, 
   Sum(df_310.FiFo_Amount_closing) AS SumOfFiFo_Amount_closing, 
   If(Sum(df_310.Opening_Stock) = 0, 0, (Sum(df_310.FiFo_Amount_opening) / Sum(df_310.Opening_Stock) * 1000)) AS FiFo_opening,
   If(Sum(df_310.Closing_Stock) = 0, 0, (Sum(df_310.FiFo_Amount_closing) / Sum(df_310.Closing_Stock) * 1000)) AS FiFo_closing, 
   If(
    mstr_plnt.CSO_depot = "CSO", 0,
    If(
      If(Sum(df_310.Opening_Stock) = 0, 0, (Sum(df_310.FiFo_Amount_opening) / Sum(df_310.Opening_Stock) * 1000)) = 0, 0,
      If(If(Sum(df_310.Closing_Stock) = 0, 0, (Sum(df_310.FiFo_Amount_closing) / Sum(df_310.Closing_Stock) * 1000)) = 0, 0, 
        ((If(Sum(df_310.Closing_Stock) = 0, 0, (Sum(df_310.FiFo_Amount_closing) / Sum(df_310.Closing_Stock) * 1000)) - 
          If(Sum(df_310.Opening_Stock) = 0, 0, (Sum(df_310.FiFo_Amount_opening) / Sum(df_310.Opening_Stock) * 1000))) * 
          (Sum(df_310.Closing_Stock) / 1000))
      )
    )
  ) AS AL_carve_out
FROM df_310
LEFT JOIN `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_plant mstr_plnt
ON trim(upper(df_310.FCB_Plant)) = trim(upper(mstr_plnt.Plant))
GROUP BY 
   df_310.Company, 
   df_310.FCB_Plant, 
   mstr_plnt.Plant_Name, 
   mstr_plnt.CSO_depot, 
   df_310.FCB_Material, 
   df_310.Material_Name, 
   df_310.Material_Group, 
   df_310.Bio_group_FCB_Material,
   df_310.Source_ID,
   df_310.OPFLAG
HAVING Sum(df_310.Opening_Stock)<>0 
   AND Sum(df_310.Closing_Stock)<>0
""")

df_311.createOrReplaceTempView("df_311")

# COMMAND ----------

# DBTITLE 1,457: append AL to margin view
df_457 = spark.sql("""
SELECT 
   'Actual Length' AS Margin_Bucket, 
   df_311.Company AS Company, 
   df_311.FCB_Plant AS Plant, 
   df_311.Plant_Name AS Plant_Name, 
   df_311.FCB_Material AS FCB_Material, 
   df_311.Material_Name AS Material_Name, 
   df_311.Material_Group AS Material_Group_Finance, 
   df_311.Bio_group_FCB_Material AS Bio_Group,
   df_311.Source_ID,
   df_311.OPFLAG,
   -- Sum(df_311.SumOfOpening_Stock) AS SumOfQuantity,
   Sum(df_311.SumOfClosing_Stock) AS SumOfQuantity,
   Sum(df_311.AL_carve_out) AS SumOfC1_FifO
FROM df_311
GROUP BY 
   df_311.Company, 
   df_311.FCB_Plant, 
   df_311.Plant_Name, 
   df_311.FCB_Material, 
   df_311.Material_Name, 
   df_311.Material_Group,
   df_311.Bio_group_FCB_Material,
   df_311.Source_ID,
   df_311.OPFLAG
HAVING 
     (df_311.Material_Group NOT LIKE 'Other' 
     OR df_311.Material_Group NOT LIKE 'Others')
       AND Sum(df_311.AL_carve_out)<>0 
      --Removed AND and replaced with OR
""")

# COMMAND ----------

# DBTITLE 1,452: append OTHER-C1 to margin view
df_452 = spark.sql(f"""
SELECT 
   'Other C1' AS Margin_Bucket, 
   ddo.Company, 
   ddo.FCB_Plant AS Plant,
   ddo.FCB_Plant_Name AS Plant_Name,
   TRIM(ddo.FCB_Material) AS FCB_Material,
   ddo.Material_Name,
   ddo.Material_Group AS Material_Group_Finance,
   ddo.Bio_group_FCB_Material AS Bio_Group,
   ddo.Source_ID,
   ddo.OPFLAG,
   SUM(ddo.Quantity_KG) AS SumOfQuantity,
   SUM(Round(ddo.Amount_FiFo_LC,0)) AS SumOfC1_Fifo
FROM {ddo_table} ddo
WHERE ddo.Year = {prev_1st_year} AND ddo.Month = {prev_1st_month}
GROUP BY 
   ddo.Company, 
   ddo.FCB_Plant,
   ddo.FCB_Plant_Name,
   TRIM(ddo.FCB_Material),
   ddo.Material_Name,
   ddo.Material_Group,
   ddo.Bio_group_FCB_Material,
   ddo.LI_bucket_1,
   ddo.Source_ID,
   ddo.OPFLAG
HAVING ((SUM(Round(ddo.Amount_FiFo_LC,0))<>0) AND (ddo.LI_bucket_1 Like 'Other C1'))
""")

# COMMAND ----------

# DBTITLE 1,453: append PAPER to margin view
df_453 = spark.sql(f"""
SELECT 
   'Paper Result' AS Margin_Bucket,
   ddo.Company,
   ddo.FCB_Plant AS Plant,
   ddo.FCB_Plant_Name AS Plant_Name,
   ddo.FCB_Material,
   ddo.Material_Name,
   ddo.Material_Group AS Material_Group_Finance,
   ddo.Bio_group_FCB_Material AS Bio_Group,
   ddo.Source_ID,
   ddo.OPFLAG,
   SUM(ddo.Quantity_KG) AS SumOfQuantity,
   SUM(Round(ddo.Amount_FiFo_LC,0)) AS SumOfC1_Fifo
FROM {ddo_table} ddo
WHERE ddo.Year = {prev_1st_year} AND ddo.Month = {prev_1st_month}
GROUP BY 
   ddo.Company, 
   ddo.FCB_Plant,
   ddo.FCB_Plant_Name,
   ddo.FCB_Material,
   ddo.Material_Name,
   ddo.Material_Group,
   ddo.Bio_group_FCB_Material,
   ddo.LI_bucket_1,
   ddo.Source_ID,
   ddo.OPFLAG
HAVING SUM(Round(ddo.Amount_FiFo_LC,0))<>0
    AND ddo.LI_bucket_1 Like 'Hedge'
""")

# COMMAND ----------

# DBTITLE 1,454: append G&L to margin view
df_454 = spark.sql(f"""
SELECT 
   'G&L' AS Margin_Bucket,
   gandl_df_107.Company AS Company,
   gandl_df_107.Plant AS Plant, 
   mstr_plnt.Plant_Name AS Plant_Name, 
   gandl_df_107.Material AS FCB_Material, 
   mstr_mtrl.Material_Name AS Material_Name, 
   mstr_mtrl.Material_Group_Finance AS Material_Group_Finance, 
   mstr_mtrl.Bio AS Bio_Group,
   gandl_df_107.Source_ID,
   gandl_df_107.OPFLAG,
   sum(gandl_df_107.Weight_in_KG) AS SumOfQuantity, 
   (-1 * sum(gandl_df_107.Amount_in_LC)) AS SumOfC1_Fifo
FROM gandl_df_107
LEFT JOIN `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_plant mstr_plnt
ON trim(upper(gandl_df_107.Plant)) = trim(upper(mstr_plnt.Plant))
LEFT JOIN `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_material mstr_mtrl
ON trim(upper(gandl_df_107.Material)) = trim(upper(mstr_mtrl.Material))
GROUP BY 
   Margin_Bucket, 
   gandl_df_107.Company, 
   gandl_df_107.Plant, 
   mstr_plnt.Plant_Name, 
   gandl_df_107.Material, 
   mstr_mtrl.Material_Name, 
   mstr_mtrl.Material_Group_Finance, 
   mstr_mtrl.Bio,
   gandl_df_107.Source_ID,
   gandl_df_107.OPFLAG
HAVING (SumOfC1_Fifo <> 0)
""")

# COMMAND ----------

# DBTITLE 1,454a:append manual G&L to margin view
df_454a = spark.sql(f"""
SELECT 
   'G&L' AS Margin_Bucket, 
   ddo.Company, 
   ddo.FCB_Plant AS Plant,
   ddo.FCB_Plant_Name AS Plant_Name,
   ddo.FCB_Material,
   ddo.Material_Name,
   ddo.Material_Group AS Material_Group_Finance,
   ddo.Bio_group_FCB_Material AS Bio_Group,
   ddo.Source_ID,
   ddo.OPFLAG,
   SUM(ddo.Quantity_KG) AS SumOfQuantity, 
   SUM(ddo.Amount_FiFo_LC) AS SumOfC1_FifO
FROM {ddo_table} ddo
WHERE ddo.Year = {prev_1st_year} AND ddo.Month = {prev_1st_month}
GROUP BY 
   ddo.Company, 
   ddo.FCB_Plant,
   ddo.FCB_Plant_Name,
   ddo.FCB_Material,
   ddo.Material_Name,
   ddo.Material_Group,
   ddo.Bio_group_FCB_Material,
   ddo.Account,
   ddo.Source_ID,
   ddo.OPFLAG
HAVING (ddo.Account='6330202')
""")

# COMMAND ----------

# DBTITLE 1,455a: append Logistic (GSAP) C1 to margin view
# df_455a is not used in the final report 
df_455a = spark.sql(f"""
SELECT 
   'Logistics' AS Margin_Bucket, 
   logistics_df_108.Company_Code AS Company, 
   logistics_df_108.Supply_Point AS Plant, 
   mstr_plnt.Plant_Name AS Plant_Name, 
   logistics_df_108.Material AS FCB_Material, 
   mstr_mtrl.Material_Name AS Material_Name, 
   mstr_mtrl.Material_Group_Finance AS Material_Group_Finance, 
   mstr_mtrl.Bio AS Bio_Group,
   logistics_df_108.Source_ID,
   logistics_df_108.OPFLAG,
   sum(logistics_df_108.Weight_in_KG) AS SumOfQuantity, 
   (-1 * sum(logistics_df_108.Total)) AS SumOfC1_FifO 
   FROM logistics_df_108
   INNER JOIN `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_plant mstr_plnt
   ON trim(upper(logistics_df_108.Supply_Point)) = trim(upper(mstr_plnt.Plant))
   INNER JOIN `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_material mstr_mtrl
   ON trim(upper(logistics_df_108.Material)) = trim(upper(mstr_mtrl.Material))
   GROUP BY 
      logistics_df_108.Company_Code, 
      logistics_df_108.Supply_Point, 
      mstr_plnt.Plant_Name, 
      logistics_df_108.Material, 
      mstr_mtrl.Material_Name, 
      mstr_mtrl.Material_Group_Finance, 
      mstr_mtrl.Bio,
      logistics_df_108.Source_ID,
      logistics_df_108.OPFLAG
""")

# COMMAND ----------

# DBTITLE 1,461: append manual margin explanation (import)
df_461 = spark.sql(f""" 
SELECT 
   mrgn_exp.margin_driver AS Margin_Bucket, 
   mrgn_exp.company AS Company,   
   mrgn_exp.plant AS Plant, 
   mstr_plnt.plant_name AS Plant_Name, 
   mrgn_exp.Material AS FCB_Material, 
   mstr_mtrl.material_name AS Material_Name, 
   mstr_mtrl.material_group_finance AS Material_Group_Finance, 
   mstr_mtrl.bio AS Bio_Group,
   mrgn_exp.Source_ID,
   mrgn_exp.OPFLAG,
   0 AS SumOfQuantity, 
   sum(mrgn_exp.C1_Margin_FIFO) AS SumOfC1_Fifo
FROM (`{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_manual_margin_explanation mrgn_exp
LEFT JOIN `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_plant mstr_plnt
ON trim(upper(mrgn_exp.Plant)) = trim(upper(mstr_plnt.Plant)))
LEFT JOIN `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_material mstr_mtrl
ON trim(UPPER(mrgn_exp.Material)) = trim(UPPER(mstr_mtrl.Material))
GROUP BY 
   mrgn_exp.company, 
   mrgn_exp.margin_driver, 
   mrgn_exp.plant, 
   mstr_plnt.plant_name, 
   mrgn_exp.Material, 
   mstr_mtrl.material_name, 
   mstr_mtrl.material_group_finance, 
   mstr_mtrl.bio,
   mrgn_exp.Source_ID,
   mrgn_exp.OPFLAG
HAVING mrgn_exp.margin_driver <> 'Bio' 
AND SumOfC1_Fifo <> 0      -- uncommented SumofC1Fifo
""")

# COMMAND ----------

# DBTITLE 1,458a: append Bio component to margin view
df_458a = spark.sql(f"""
SELECT 
   'Bio' AS Margin_Bucket,
   ddo.Company, 
   ddo.FCB_Plant AS Plant,
   ddo.FCB_Plant_Name AS Plant_Name,
   ddo.FCB_Material,
   ddo.Material_Name,
   ddo.Material_Group AS Material_Group_Finance,
   ddo.Bio_group_FCB_Material AS Bio_Group,
   ddo.Source_ID,
   ddo.OPFLAG,
   CASE 
     WHEN LOWER(ddo.LI_bucket_2) = LOWER('TFP cust') THEN 0
     ELSE SUM(ddo.Quantity_KG)
   END AS SumOfQuantity,
   SUM(ddo.Amount_FiFo_LC) AS SumOfC1_FifO   
FROM {ddo_table} ddo
WHERE ddo.Year = {prev_1st_year} AND ddo.Month = {prev_1st_month}
GROUP BY 
   ddo.Company, 
   ddo.FCB_Plant,
   ddo.FCB_Plant_Name,
   ddo.FCB_Material,
   ddo.Material_Name,
   ddo.Material_Group,
   ddo.Bio_group_FCB_Material,
   ddo.LI_bucket_2,
   ddo.Source_ID,
   ddo.OPFLAG
HAVING ((LOWER(ddo.Bio_group_FCB_Material) = LOWER('component')
    AND (SUM(ddo.Amount_FiFo_LC)<>0)
    --AND ((CASE WHEN LOWER(ddo.LI_bucket_2) = LOWER('TFP cust') THEN 0 ELSE SUM(ddo.Quantity_KG) END) IS NOT NULL)
    AND (LOWER(ddo.LI_bucket_2) = LOWER('3rd party purchase') 
      OR LOWER(ddo.LI_bucket_2) = LOWER('TFP cust')
      OR LOWER(ddo.LI_bucket_2) = LOWER('TFP correction')
      OR LOWER(ddo.LI_bucket_2) = LOWER('IG sale') 
      OR LOWER(ddo.LI_bucket_2) = LOWER('3rd party sale')))
)
""")

# COMMAND ----------

# DBTITLE 1,352 - bio blend premia
df_352 = spark.sql(f"""
SELECT 
   ddo.Company, 
   ddo.FCB_Plant, 
   ddo.FCB_Plant_Name, 
   ddo.FCB_Material, 
   ddo.Material_Name, 
   ddo.Material_Group, 
   mstr_mtrl.Material_Group_2, 
   ddo.Bio_group_FCB_Material, 
   ddo.Partner_Material, 
   ddo.Partner_Material_Name, 
   ddo.Partner_Material_Group, 
   ddo.Bio_group_Partner_Material, 
   ddo.Functional_Area, 
   ddo.Source_ID,
   ddo.OPFLAG,
   Sum(ddo.Quantity_KG) AS SumOfQuantity_KG, 
   bio_premia.premia_per_to, 
   -1 * (SumOfQuantity_KG)/1000 * premia_per_to AS Bio_premia
FROM {ddo_table} ddo
LEFT JOIN `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_bio_premia bio_premia
ON trim(upper(ddo.Company)) = trim(upper(bio_premia.company))
AND trim(upper(ddo.Bio_group_Partner_Material)) = trim(upper(bio_premia.bio_material))
LEFT JOIN `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_material mstr_mtrl
ON trim(upper(ddo.FCB_Material)) = trim(upper(mstr_mtrl.Material))
WHERE ddo.Year = {prev_1st_year} AND ddo.Month = {prev_1st_month}
GROUP BY 
   ddo.Company, 
   ddo.FCB_Plant, 
   ddo.FCB_Plant_Name, 
   ddo.FCB_Material, 
   ddo.Material_Name, 
   ddo.Material_Group, 
   mstr_mtrl.Material_Group_2, 
   ddo.Bio_group_FCB_Material, 
   ddo.Partner_Material, 
   ddo.Partner_Material_Name, 
   ddo.Partner_Material_Group, 
   ddo.Bio_group_Partner_Material, 
   ddo.Functional_Area, 
   bio_premia.premia_per_to,
   ddo.Source_ID,
   ddo.OPFLAG
HAVING 
   (ddo.FCB_Plant<> 'D156' 
   OR ddo.FCB_Plant <> 'D158' 
   OR ddo.FCB_Plant <> 'D152' 
   OR ddo.FCB_Plant <> 'D154')
     AND LOWER(mstr_mtrl.Material_Group_2) Not Like LOWER('Additive')
     AND ddo.Bio_group_FCB_Material Is Null
     AND ddo.Bio_group_Partner_Material Is Not Null
     AND bio_premia.premia_per_to Is Not Null
""")
df_352.createOrReplaceTempView("df_352")

# COMMAND ----------

# DBTITLE 1,459: append Bio blend premia to margin view
df_459 = spark.sql("""
SELECT 
   'Bio' AS Margin_Bucket,
   df_352.Company, 
   df_352.FCB_Plant AS Plant, 
   df_352.FCB_Plant_Name AS Plant_Name, 
   df_352.Partner_Material AS FCB_Material, 
   df_352.Partner_Material_Name AS Material_Name, 
   df_352.Partner_Material_Group AS Material_Group_Finance, 
   df_352.Bio_group_Partner_Material AS Bio_Group,
   df_352.Source_ID,
   df_352.OPFLAG,
   Sum(-1 * (df_352.SumOfQuantity_KG)) AS SumOfQuantity,
   Sum(df_352.Bio_premia) AS SumOfC1_FifO   
FROM df_352
GROUP BY 
   df_352.Company, 
   df_352.FCB_Plant, 
   df_352.FCB_Plant_Name, 
   df_352.Partner_Material, 
   df_352.Partner_Material_Name, 
   df_352.Partner_Material_Group, 
   df_352.Bio_group_Partner_Material,
   df_352.Source_ID,
   df_352.OPFLAG
HAVING Sum(df_352.Bio_premia)<>0
""")

# COMMAND ----------

# DBTITLE 1,353 -  Bio_premia ex refinery
df_353 = spark.sql(f"""
SELECT 
   ddo.Company, 
   ddo.FCB_Plant, 
   ddo.FCB_Plant_Name, 
   'Rhineland' AS Refinery, 
   CASE
    WHEN Refinery = 'Rhineland' THEN FCB_Plant
    WHEN FCB_Plant IN ('D028', 'D029') THEN FCB_Plant
    ELSE Partner_Plant
   END AS Bio_Plant,
   CASE
    WHEN Refinery = 'Rhineland' THEN FCB_Plant_Name
    WHEN FCB_Plant IN ('D028', 'D029') THEN FCB_Plant_Name
    ELSE Partner_Plant_Name
   END AS Bio_Plant_Name, 
   ddo.Channel, 
   ddo.Partner_Plant, 
   ddo.Partner_Plant_Name, 
   ddo.FCB_Material, 
   ddo.Material_Name, 
   ddo.Material_Group, 
   ddo.Bio_group_FCB_Material, 
   ddo.Functional_Area,
   ddo.Source_ID,
   ddo.OPFLAG,
   Sum(ddo.Quantity_KG) AS SumOfQuantity_KG, 
   bio_premia.premia_per_to, 
   (SumOfQuantity_KG)/1000* (premia_per_to) AS Bio_premia
FROM {ddo_table} ddo
LEFT JOIN `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_bio_premia bio_premia
ON trim(upper(ddo.Company)) = trim(upper(bio_premia.Company))
AND trim(upper(ddo.Bio_group_FCB_Material)) = trim(upper(bio_premia.bio_material))
LEFT JOIN `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_material mstr_mtrl
ON trim(upper(ddo.FCB_Material)) = trim(upper(mstr_mtrl.Material))
WHERE ddo.Year = {prev_1st_year} AND ddo.Month = {prev_1st_month}
GROUP BY 
   ddo.Company, 
   ddo.FCB_Plant, 
   ddo.FCB_Plant_Name,
   ddo.Channel, 
   ddo.Partner_Plant, 
   ddo.Partner_Plant_Name, 
   ddo.FCB_Material, 
   ddo.Material_Name, 
   ddo.Material_Group, 
   ddo.Bio_group_FCB_Material, 
   ddo.Functional_Area, 
   bio_premia.premia_per_to,
   ddo.Source_ID,
   ddo.OPFLAG
HAVING 
   (ddo.FCB_Plant<> 'D156' 
   OR ddo.FCB_Plant<> 'D158' 
   OR ddo.FCB_Plant<> 'D152' 
   OR ddo.FCB_Plant<> 'D154' )
     AND LOWER(ddo.Channel) = 'manufacturing'
     AND bio_premia.premia_per_to IS NOT NULL -- uncommented this line
""")
df_353.createOrReplaceTempView("df_353")

# COMMAND ----------

# DBTITLE 1,460: append Bio premia ex refinery to margin view
df_460 = spark.sql(f"""
SELECT 
   'Bio' AS Margin_Bucket, 
   df_353.Company, 
   df_353.Bio_Plant AS Plant, 
   df_353.Bio_Plant_Name AS Plant_Name, 
   df_353.FCB_Material, 
   df_353.Material_Name, 
   df_353.Material_Group AS Material_Group_Finance, 
   df_353.Bio_group_FCB_Material AS Bio_group,
   df_353.Source_ID,
   df_353.OPFLAG,
   Sum(df_353.SumOfQuantity_KG) AS SumOfQuantity,
   Sum(df_353.Bio_premia) AS SumOfC1_FifO   
FROM df_353
GROUP BY 
   df_353.Company, 
   df_353.Bio_Plant, 
   df_353.Bio_Plant_Name, 
   df_353.FCB_Material, 
   df_353.Material_Name,
   df_353.Material_Group, 
   df_353.Bio_group_FCB_Material,
   df_353.Source_ID,
   df_353.OPFLAG
HAVING (Sum(df_353.Bio_premia)<>0)
""")

# COMMAND ----------

# DBTITLE 1,461a: append manual Bio margin explanation (import)
df_461a = spark.sql(f""" 
SELECT 
    mrgn_expln.margin_driver AS Margin_Bucket, 
    mrgn_expln.company AS Company,     
    mrgn_expln.plant AS Plant, 
    mstr_plnt.plant_name AS Plant_Name, 
    mrgn_expln.Material AS FCB_Material, 
    mstr_mtrl.material_name AS Material_Name, 
    mstr_mtrl.material_group_finance AS Material_Group_Finance, 
    mstr_mtrl.bio AS Bio_Group,
    mrgn_expln.Source_ID,
    mrgn_expln.OPFLAG,
    0 AS SumOfQuantity,
    sum(mrgn_expln.C1_Margin_FIFO) AS SumOfC1_FifO
FROM (`{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_manual_margin_explanation mrgn_expln
LEFT JOIN `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_plant mstr_plnt
ON trim(upper(mrgn_expln.Plant)) = trim(upper(mstr_plnt.Plant)))
LEFT JOIN `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_material mstr_mtrl
ON trim(UPPER(mrgn_expln.Material)) = trim(UPPER(mstr_mtrl.Material))
GROUP BY 
   mrgn_expln.company, 
   mrgn_expln.margin_driver, 
   mrgn_expln.plant, 
   mstr_plnt.plant_name, 
   mrgn_expln.Material, 
   mstr_mtrl.material_name, 
   mstr_mtrl.material_group_finance, 
   mstr_mtrl.bio,
   mrgn_expln.Source_ID,
   mrgn_expln.OPFLAG
HAVING (mrgn_expln.margin_driver = 'Bio' AND SumOfC1_FifO IS NOT NULL -- To exclude all null records
)
""")

# COMMAND ----------

df_mbc_union_1 = df_451.unionByName(df_457).unionByName(df_452).unionByName(df_453).unionByName(df_454).unionByName(df_454a).unionByName(df_461).unionByName(df_458a).unionByName(df_459).unionByName(df_460).unionByName(df_461a)
# df_mbc_union_1.createOrReplaceTempView("df_mbc_union_1")

# COMMAND ----------

df_mbc_union_1 = df_mbc_union_1.groupBy("Margin_Bucket", "Company", "Plant", "Plant_Name", "FCB_Material", "Material_Name", "Material_Group_Finance", "Bio_Group", "Source_ID", "OPFLAG")\
                 .agg(F.sum("SumOfQuantity").alias("SumOfQuantity"),F.sum("SumOfC1_FifO").alias("SumOfC1_FifO"))
df_mbc_union_1.createOrReplaceTempView("df_mbc_union_1")

# COMMAND ----------

# DBTITLE 1,351: identify u+C19nexplained margin parts
df_351 = spark.sql(f"""
SELECT 
   df_mbc_union_1.Margin_Bucket, 
   df_mbc_union_1.Company, 
   df_mbc_union_1.Plant, 
   df_mbc_union_1.Plant_Name, 
   df_mbc_union_1.Material_Group_Finance, 
   df_mbc_union_1.FCB_Material, 
   df_mbc_union_1.Material_Name, 
   df_mbc_union_1.Bio_Group,
   df_mbc_union_1.Source_ID,
   df_mbc_union_1.OPFLAG,
   sum(df_mbc_union_1.SumOfQuantity) AS SumOfSumOfQuantity, 
   sum(df_mbc_union_1.SumOfC1_FIFO) AS SumOfSumOfC1_FIFO, 
   CASE 
    WHEN df_mbc_union_1.Margin_Bucket = 'C1 FiFo' THEN SumOfSumOfC1_FIFO ELSE -1 * (SumOfSumOfC1_FIFO) 
   END AS Unexplained_Margin
FROM df_mbc_union_1
GROUP BY 
   df_mbc_union_1.Margin_Bucket, 
   df_mbc_union_1.Company, 
   df_mbc_union_1.Plant, 
   df_mbc_union_1.Plant_Name, 
   df_mbc_union_1.Material_Group_Finance, 
   df_mbc_union_1.FCB_Material, 
   df_mbc_union_1.Material_Name, 
   df_mbc_union_1.Bio_Group,
   df_mbc_union_1.Source_ID,
   df_mbc_union_1.OPFLAG
HAVING df_mbc_union_1.Margin_Bucket NOT LIKE 'Unexplained' 
   AND sum(df_mbc_union_1.SumOfC1_FIFO) <> 0
""")
df_351.createOrReplaceTempView("df_351")

# COMMAND ----------

# DBTITLE 1,456: 351: identify unexplained margin parts
df_456 = spark.sql("""
SELECT 
   'Unexplained' AS Margin_Bucket, 
   df_351.Company, 
   df_351.Plant, 
   df_351.Plant_Name, 
   df_351.FCB_Material, 
   df_351.Material_Name, 
   df_351.Material_Group_Finance, 
   df_351.Bio_Group,
   df_351.Source_ID,
   df_351.OPFLAG,
   0 AS SumOfQuantity,
   Sum(df_351.Unexplained_Margin) AS SumOfC1_FifO
FROM df_351
GROUP BY 
   df_351.Company, 
   df_351.Plant, 
   df_351.Plant_Name, 
   df_351.FCB_Material, 
   df_351.Material_Name, 
   df_351.Material_Group_Finance, 
   df_351.Bio_Group,
   df_351.Source_ID,
   df_351.OPFLAG
HAVING ROUND(Sum(df_351.Unexplained_Margin),4)<>0
""")

# COMMAND ----------

# DBTITLE 1,458: 351:identify unexplained margin parts
df_458 = spark.sql("""
SELECT 
   'Bio' AS Margin_Bucket, 
   df_351.Company, 
   df_351.Plant, 
   df_351.Plant_Name, 
   df_351.FCB_Material, 
   df_351.Material_Name, 
   df_351.Material_Group_Finance, 
   df_351.Bio_Group,
   df_351.Source_ID,
   df_351.OPFLAG,
   Sum(df_351.SumOfSumOfQuantity) AS SumOfQuantity,
   -- Sum(df_351.Unexplained_Margin) AS SumOfC1_FifO  
   Sum(df_351.SumOfSumOfC1_FIFO) AS SumOfC1_FifO 
FROM df_351
WHERE Margin_Bucket = 'Bio'
GROUP BY 
   df_351.Company, 
   df_351.Plant, 
   df_351.Plant_Name, 
   df_351.FCB_Material, 
   df_351.Material_Name, 
   df_351.Material_Group_Finance, 
   df_351.Bio_Group,
   df_351.Source_ID,
   df_351.OPFLAG
HAVING LOWER(df_351.Bio_Group) = 'component'
--AND Sum(df_351.SumOfSumOfC1_FIFO)<>0
AND Sum(df_351.SumOfSumOfQuantity) IS NOT NULL
""")

# COMMAND ----------

# DBTITLE 1,465:351: Identify unexplained margin parts
df_465 = spark.sql("""
SELECT 
   'Unexplained' AS Margin_Bucket, 
   df_351.Company, 
   df_351.Plant, 
   df_351.Plant_Name,
   df_351.FCB_Material, 
   df_351.Material_Name, 
   df_351.Material_Group_Finance, 
   df_351.Bio_Group,
   df_351.Source_ID,
   df_351.OPFLAG,
   0 AS SumOfQuantity,
   Sum(-1 * (df_351.Unexplained_Margin)) AS SumOfC1_FifO
FROM df_351
GROUP BY 
   df_351.Company, 
   df_351.Plant, 
   df_351.Plant_Name, 
   df_351.FCB_Material, 
   df_351.Material_Name, 
   df_351.Material_Group_Finance, 
   df_351.Bio_Group,
   df_351.Source_ID,
   df_351.OPFLAG
HAVING ROUND(Sum(-1 * (df_351.Unexplained_Margin)),4) <>0
""")

# COMMAND ----------

# DBTITLE 1,Union 2
df_mbc_union_2 = df_mbc_union_1.unionByName(df_458)
df_mbc_union_2 = df_mbc_union_2.dropDuplicates()
df_mbc_union_2 = df_mbc_union_2.unionByName(df_456).unionByName(df_465).unionByName(df_logistics_455)
df_mbc_union_2.createOrReplaceTempView("df_mbc_union_2")

# COMMAND ----------

# DBTITLE 1,358: identify unexplained margin
df_358 = spark.sql(f"""
SELECT 
   df_mbc_union_2.Margin_Bucket, 
   df_mbc_union_2.Company, 
   df_mbc_union_2.Plant, 
   df_mbc_union_2.Plant_Name, 
   df_mbc_union_2.Material_Group_Finance, 
   df_mbc_union_2.FCB_Material, 
   df_mbc_union_2.Material_Name, 
   df_mbc_union_2.Bio_Group,
   df_mbc_union_2.Source_ID,
   df_mbc_union_2.OPFLAG,
   sum(df_mbc_union_2.SumOfQuantity) AS SumOfSumOfQuantity, 
   sum(df_mbc_union_2.SumOfC1_FIFO) AS SumOfSumOfC1_FIFO, 
   CASE 
    WHEN df_mbc_union_2.Margin_Bucket = 'C1 FiFo' 
    THEN SumOfSumOfC1_FIFO 
    ELSE -1 * (SumOfSumOfC1_FIFO) 
   END AS Unexplained_Margin
FROM df_mbc_union_2 
GROUP BY 
   df_mbc_union_2.Margin_Bucket, 
   df_mbc_union_2.Company, 
   df_mbc_union_2.Plant, 
   df_mbc_union_2.Plant_Name, 
   df_mbc_union_2.Material_Group_Finance, 
   df_mbc_union_2.FCB_Material, 
   df_mbc_union_2.Material_Name, 
   df_mbc_union_2.Bio_Group,
   df_mbc_union_2.Source_ID,
   df_mbc_union_2.OPFLAG
HAVING 
   df_mbc_union_2.Margin_Bucket NOT LIKE 'WIP' 
   AND sum(df_mbc_union_2.SumOfC1_FIFO) <> 0
""")
df_358.createOrReplaceTempView("df_358")

# COMMAND ----------

# DBTITLE 1,464: append final unexplained to margin view
df_464 = spark.sql("""
SELECT 
   'Unexplained' AS Margin_Bucket,
   df_358.Company,
   df_358.Plant,
   df_358.Plant_Name,
   df_358.FCB_Material,
   df_358.Material_Name,
   df_358.Material_Group_Finance, 
   df_358.Bio_Group,
   df_358.Source_ID,
   df_358.OPFLAG,
   0 AS SumOfQuantity,
   df_358.Unexplained_Margin AS SumOfC1_FifO
FROM df_358
WHERE ROUND(df_358.Unexplained_Margin,4) <>0
""")

# COMMAND ----------

# DBTITLE 1,310, 355 & 356
# Check with Narayana if 354 is reuired
# df_Stock_price_effect_354 = spark.sql(f"""
# SELECT 
#    df_margin_bucket_union.Margin_Bucket, 
#    df_margin_bucket_union.Company, 
#    df_margin_bucket_union.Plant, 
#    df_margin_bucket_union.Plant_Name, 
#    df_margin_bucket_union.Material_Group_Finance, 
#    df_margin_bucket_union.FCB_Material, 
#    df_margin_bucket_union.Material_Name, 
#    df_margin_bucket_union.Bio_Group, 
#    sum(df_margin_bucket_union.SumOfQuantity) AS SumOfSumOfQuantity, 
#    sum(df_margin_bucket_union.SumOfC1_FIFO) AS SumOfSumOfC1_FIFO, 
#    CASE 
#     WHEN df_margin_bucket_union.Margin_Bucket = 'C1 FiFo' 
#     THEN SumOfSumOfC1_FIFO 
#     ELSE -1 * (SumOfSumOfC1_FIFO) 
#    END AS Unexplained_Margin
# FROM df_margin_bucket_union
# GROUP BY 
#    df_margin_bucket_union.Margin_Bucket, 
#    df_margin_bucket_union.Company, 
#    df_margin_bucket_union.Plant, 
#    df_margin_bucket_union.Plant_Name, 
#    df_margin_bucket_union.Material_Group_Finance, 
#    df_margin_bucket_union.FCB_Material, 
#    df_margin_bucket_union.Material_Name, 
#    df_margin_bucket_union.Bio_Group
# HAVING df_margin_bucket_union.Margin_Bucket NOT LIKE 'component' 
#    AND (sumdf_margin_bucket_union.SumOfC1_FIFO) <> 0
# """)

# df_310 = spark.sql(f"""
# SELECT 
#    ddo.Company,
#    ddo.Account,
#    ddo.LI_bucket_2,
#    ddo.FCB_Plant,
#    ddo.FCB_Material,
#    ddo.Material_Name,
#    ddo.Material_Group,
#    ddo.Bio_group_FCB_Material,
#    Sum(ddo.Quantity_KG) AS SumOfQuantity_KG,
#    Sum(ddo.Amount_FiFo_LC) AS SumOfAmount_FiFo_LC,
#    (SumOfAmount_FiFo_LC/SumOfQuantity_KG) * 1000 AS price_per_to,
#    CASE 
#     WHEN LI_Bucket_2 = 'Opening Stock' THEN SumOfQuantity_KG 
#     ELSE 0 
#    END AS Opening_Stock, 
#    CASE 
#     WHEN LI_Bucket_2 = 'Closing Stock' THEN -1 * (SumOfQuantity_KG) 
#     ELSE 0 
#    END AS Closing_Stock, 
#    CASE 
#     WHEN LI_Bucket_2 = 'Opening Stock' THEN -1 * (SumOfAmount_FiFo_LC) 
#     ELSE 0 
#    END AS FiFo_Amount_opening, 
#    CASE 
#     WHEN LI_Bucket_2 = 'Closing Stock' THEN SumOfAmount_FiFo_LC 
#     ELSE 0 
#    END AS FiFo_Amount_closing,
#    FiFo_Amount_opening/Opening_Stock * 1000 AS FiFo_opening
# FROM {ddo_table} ddo
# WHERE Year = {prev_1st_year} AND Month = {prev_1st_month}
# GROUP BY 
#    ddo.Company, 
#    ddo.Account, 
#    ddo.LI_bucket_2, 
#    ddo.FCB_Plant, 
#    ddo.FCB_Material, 
#    ddo.Material_Name, 
#    ddo.Material_Group, 
#    ddo.Bio_group_FCB_Material
# HAVING (ddo.LI_bucket_2 = 'Opening Stock' 
#        Or ddo.LI_bucket_2 = 'Closing Stock' 
#         AND ((Sum(ddo.Quantity_KG)<>0)))
# """)
# df_310.createOrReplaceTempView("df_310")

df_355 = spark.sql(f"""
SELECT 
   stk_prc_efct.Company, 
   stk_prc_efct.FCB_Plant, 
   stk_prc_efct.FCB_Material, 
   stk_prc_efct.LI_bucket_1, 
   stk_prc_efct.LI_bucket_2,
   stk_prc_efct.Source_ID,
   stk_prc_efct.OPFLAG,
   Sum(df_310.FiFo_Amount_opening) AS SumOfFiFo_Amount_opening, 
   Sum(df_310.Opening_Stock) AS SumOfOpening_Stock,
   (SumOfFiFo_Amount_opening/SumOfOpening_Stock)*1000 AS FiFo_opening, 
   stk_prc_efct.SumOfQuantity_KG, 
   stk_prc_efct.TFP
FROM `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_stock_price_effect stk_prc_efct
LEFT JOIN df_310
ON trim(upper(stk_prc_efct.FCB_Material)) = trim(upper(df_310.FCB_Material))
AND trim(upper(stk_prc_efct.FCB_Plant)) = trim(upper(df_310.FCB_Plant))
AND trim(upper(stk_prc_efct.Company)) = trim(upper(df_310.Company))
GROUP BY 
   stk_prc_efct.Company, 
   stk_prc_efct.FCB_Plant, 
   stk_prc_efct.FCB_Material, 
   stk_prc_efct.LI_bucket_1, 
   stk_prc_efct.LI_bucket_2,
   stk_prc_efct.Source_ID,
   stk_prc_efct.OPFLAG,
   stk_prc_efct.SumOfQuantity_KG, 
   stk_prc_efct.TFP
HAVING ((SumOfOpening_Stock)<>0)
""")
df_355.createOrReplaceTempView("df_355")

df_356 = spark.sql(f"""
SELECT 
   df_355.Company, 
   df_355.FCB_Plant, 
   df_355.FCB_Material, 
   mstr_mtrl.Material_Group_Finance, 
   mstr_mtrl.Bio, 
   df_355.LI_bucket_1, 
   df_355.LI_bucket_2, 
   df_355.SumOfQuantity_KG, 
   df_355.FiFo_opening, 
   df_355.TFP,
   df_355.Source_ID,
   df_355.OPFLAG,
   ((FiFo_opening-TFP)*SumOfQuantity_KG/1000) AS Price_effect
FROM df_355
LEFT JOIN `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_material mstr_mtrl
ON trim(upper(df_355.FCB_Material)) =  trim(upper(mstr_mtrl.Material))
GROUP BY 
   df_355.Company, 
   df_355.FCB_Plant, 
   df_355.FCB_Material, 
   mstr_mtrl.Material_Group_Finance, 
   mstr_mtrl.Bio, 
   df_355.LI_bucket_1, 
   df_355.LI_bucket_2, 
   df_355.SumOfQuantity_KG, 
   df_355.FiFo_opening, 
   df_355.TFP,
   df_355.Source_ID,
   df_355.OPFLAG
""")
df_356.createOrReplaceTempView("df_356")

# COMMAND ----------

# DBTITLE 1,462: 356 Stock Calculate price effect
df_462 = spark.sql(f"""
SELECT 
   'Stock Price effect' AS Margin_Bucket, 
   df_356.Company, 
   df_356.FCB_Plant AS Plant,
   '' AS Plant_Name,
   df_356.FCB_Material,
   '' AS Material_Name,
   df_356.Material_Group_Finance,    
   df_356.Bio AS Bio_Group,
   df_356.Source_ID,
   df_356.OPFLAG,
   0 AS SumOfQuantity,
   Sum(df_356.Price_effect) AS SumOfC1_FifO 
FROM df_356
GROUP BY 
   'Stock Price effect', 
   df_356.Company, 
   df_356.FCB_Plant, 
   df_356.Material_Group_Finance, 
   df_356.FCB_Material, 
   df_356.Bio,
   df_356.Source_ID,
   df_356.OPFLAG
HAVING Sum(df_356.Price_effect)<>0
""")

# COMMAND ----------

# DBTITLE 1,Union final
df_mbc_union_final = df_mbc_union_2.union(df_464).union(df_462)
df_mbc_union_final = df_mbc_union_final.withColumn("Year",lit(prev_1st_year))\
                                       .withColumn("Month",lit(prev_1st_month))\
                                       .withColumn("ingested_at",lit(current_timestamp()))

# COMMAND ----------

# df_mbc_union_final = df_mbc_union_final.dropDuplicates()
df_mbc_union_final.createOrReplaceTempView("df_mbc_union_final")

# COMMAND ----------

# DBTITLE 1,Concat of Attribute and Text
#Companycode and Company_Name_Text
df_mbc_union_final = df_mbc_union_final.join(comp_md_df, trim(upper(df_mbc_union_final.Company)) == trim(upper(comp_md_df.Company_Code)), "inner")
df_mbc_union_final = df_mbc_union_final.withColumn('Company_Code-Text',concat_ws('-', df_mbc_union_final.Company,df_mbc_union_final.Company_Name))                  
df_mbc_union_final = df_mbc_union_final.select(*[col(column_name) for column_name in df_mbc_union_final.columns if column_name not in {'Company_Code','Company_Name'}])

#Plant and Plant_Text
df_mbc_union_final =df_mbc_union_final.withColumn('Plant-Text',concat_ws('-', df_mbc_union_final.Plant,df_mbc_union_final.Plant_Name))

# COMMAND ----------

# DBTITLE 1,delete current month's data if exists
def mbc_duplicate_cleanup(path,uc_catalog_name,uc_curated_schema,data_layer, table_list1):
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
                spark.sql(f"""DELETE FROM `{uc_catalog_name}`.`{uc_curated_schema}`.{mbc_table_name}
                              WHERE Year = {prev_1st_year} AND Month = '{prev_1st_month}'""")
            else:
                print("TABLE DOESN'T EXIST" ,table_name)

mbc_table_list = ['use_case_mars_margin_bucket_collection']
mbc_duplicate_cleanup(curated_folder_path,uc_catalog_name,uc_curated_schema,"curated", mbc_table_list)

# COMMAND ----------

df_mbc_union_final = df_mbc_union_final.withColumn("Source_ID", when(df_mbc_union_final.Source_ID == "Flat_000",concat(lit(mbc_source_system_id),lit('_110'))).otherwise(df_mbc_union_final.Source_ID))

# COMMAND ----------

df_mbc_union_final.write.format("delta").mode("append").option("mergeSchema", "true").save(f"{mbc_table_path}/")
print("MBC init written")

# COMMAND ----------

# DBTITLE 1,delete data older than retention months
def mbc_cleanup(path,uc_catalog_name,uc_curated_schema,data_layer, table_list1):
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
                spark.sql(f"""DELETE FROM `{uc_catalog_name}`.`{uc_curated_schema}`.{mbc_table_name} 
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
                
mbc_table_list = ['use_case_mars_margin_bucket_collection']
mbc_cleanup(curated_folder_path,uc_catalog_name,uc_curated_schema,"curated", mbc_table_list)

# COMMAND ----------

# DBTITLE 1,Resetting the flag in control table to N after successful history load completion
# Perform the SQL query
def mbc_reset(path,uc_catalog_name,uc_curated_schema,data_layer, table_list1):
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
                df = spark.sql(f"""
                               SELECT DISTINCT CONCAT(YEAR, '_', MONTH) as Year_mon
                               FROM `{uc_catalog_name}`.`{uc_curated_schema}`.{mbc_table_name} ddo
                               INNER JOIN (
                                   SELECT DISTINCT CONCAT(Year, '_', Month) as Year_mon
                                   FROM `{uc_catalog_name}`.`{uc_curated_schema}`.mars_year_month_control_table
                                   WHERE Active_Flag = 'Y'
                                   ) ctrl ON CONCAT(ddo.YEAR, '_', ddo.MONTH) = ctrl.Year_mon
                                   """)
                # Collect the distinct Year_mon values
                distinct_year_mon = df.select("Year_mon").collect()
                
                # Check if the previous 4th year is in the collected values
                if len(distinct_year_mon) > 0:
                    print("Resetting the control table Active Flags")
                    spark.sql(f""" UPDATE `{uc_catalog_name}`.`{uc_curated_schema}`.mars_year_month_control_table
                              SET Active_Flag = 'N'
                              WHERE Active_Flag = 'Y'
                              """)
                else:
                    print("TABLE DOESN'T EXIST" ,table_name)

mbc_table_list = ['use_case_mars_margin_bucket_collection']
mbc_reset(curated_folder_path,uc_catalog_name,uc_curated_schema,"curated", mbc_table_list)
