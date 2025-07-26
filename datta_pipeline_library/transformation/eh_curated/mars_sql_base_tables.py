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
from pyspark.sql.functions import col, trim
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

uc_eh_mm_schema = uc_eh_schema.replace("-finance", "-material_mgmt")
print("uc_eh_mm_schema : ",uc_eh_mm_schema)

print("load_type:", load_type)

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

print(prev_1st_year,prev_1st_month)


# COMMAND ----------

def language_filter(df):
    filtered_df = df.filter(df.SPRAS=="E") # filter 'column' with the language key
    return filtered_df

def client_filter(df):
    filtered_df = df.filter(df.MANDT==110) # filter 'column' with the client value 
    return filtered_df

# COMMAND ----------

# DBTITLE 1,Detail report data creation for HANA & HANA Dummy
detail_report_df1 = spark.sql(f"""
SELECT
  Fiscal_Year,
  Posting_Period,
  Company_Code AS `Company Code`,
  CL_Profit_Center AS `Profit Center Key`,
  CL_GL_Account_Key AS `G/L Account Key`,
  GL_Account_Long_Text AS `G/L Account Name`,
  Functional_Area AS `Functional Area`,
  MM_Receiving_Plant AS `MM Receiving Plant`,
  Plant AS `Plant`,
  T001W_Name AS `Plant Name`,
  MM_Plant AS `MM Plant`,
  REPLACE(LTRIM(REPLACE(Material_Key ,'0',' ')),' ','0') AS `Material Key`,
  Material_Description AS `Material Name`,
  REPLACE(LTRIM(REPLACE(MM_Material ,'0',' ')),' ','0') AS `MM Material`,
  REPLACE(LTRIM(REPLACE(Material_Number ,'0',' ')),' ','0') AS `Material`,
  REPLACE(LTRIM(REPLACE(MM_Receiving_Material ,'0',' ')),' ','0') AS `MM Receiving Material`,
  REPLACE(LTRIM(REPLACE(Customer_Number ,'0',' ')),' ','0') AS `Customer Number`,
  REPLACE(LTRIM(REPLACE(Account_No_Supplier ,'0',' ')),' ','0') AS `Vendor Number`,
  KNA1_Name_1 AS `Customer Name`,
  Name_1 AS `Vendor Name`,
  MM_Mvmt_Type AS `MM Movement Type`,
  Mvmt_Type AS `Movement Type`,
  Source_ID,
  OPFLAG,
  SUM(Amt_Comp_Code_Curr) AS `R/3 Amount`,
  SUM(Weight_Qty) AS `Weight in KG`,
  SUM(Volume_Qty) AS `Volume in L15`
 FROM `{uc_catalog_name}`.`{uc_curated_schema}`.{ fcb_dynamic_view_name }
 WHERE Fiscal_Year = '{prev_1st_year}'
   AND Posting_period = '{prev_1st_month}'
   AND Company_Code IN (SELECT Company_Code FROM `{uc_catalog_name}`.`{uc_curated_schema}`.mars_comp_code_control_table)
   AND Profit_Center IN (SELECT Profit_Center FROM `{uc_catalog_name}`.`{uc_curated_schema}`.mars_pctr_control_table)
GROUP BY
   Fiscal_Year,
  Posting_Period,
  `Company Code`,
  `Profit Center Key`,
  `G/L Account Key`,
  `G/L Account Name`,
  `Functional Area`,
  `MM Receiving Plant`,
  `Plant`,
  `Plant Name`,
  `MM Plant`,
  `Material Key`,
  `Material Name`,
  `MM Material`,
  `MM Receiving Material`,
  `Customer Number`,
  `Vendor Number`,
  `Customer Name`,
  `Vendor Name`,
  `MM Movement Type`,
  `Source_ID`,
  `OPFLAG`,
  `Movement Type`,
  `Material_Number`
""")

# COMMAND ----------

# display(detail_report_df1)

# COMMAND ----------

# DBTITLE 1,C1 mapping table
c1_mapping_df = spark.sql(f"""
SELECT
  GL_Account AS C1_GL_Account,
  IF(UPPER(Cont_Level) ='YES','C1',Cont_Level) AS Cont_Level  
FROM `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_c1_mapping
WHERE UPPER(Cont_Level) != 'NO'
  """)

hana_c1_mapping_join = detail_report_df1.join(c1_mapping_df, trim(lower(detail_report_df1['G/L Account Key'])) == trim(lower(c1_mapping_df['C1_GL_Account'])), "inner")

# COMMAND ----------

# DBTITLE 1,Dummy C1 mapping table
dummy_c1_mapping_df = spark.sql(f"""
SELECT
  GL_Account AS C1_GL_Account,
  'DUMM' AS Cont_Level 
FROM `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_c1_mapping
WHERE UPPER(Cont_Level) = 'NO'
  """)

hana_dummy_c1_mapping_join = detail_report_df1.join(dummy_c1_mapping_df, trim(lower(detail_report_df1['G/L Account Key'])) == trim(lower(dummy_c1_mapping_df['C1_GL_Account'])), "inner")

# COMMAND ----------

# display(dummy_c1_mapping_df)

# COMMAND ----------

# display(hana_dummy_c1_mapping_join)

# COMMAND ----------

# DBTITLE 1,Detail report data creation for G&L
detail_report_df3 = spark.sql(f"""
 SELECT
  Company_Code AS `Company Code`,
  CL_Profit_Center AS `Profit Center`,
  CL_GL_Account_Key AS `G/L Account Key`,
  Plant AS `Plant`,
  T001W_Dist_Prof_Plant AS `Plant Type`,
  REPLACE(LTRIM(REPLACE(Material_Key ,'0',' ')),' ','0') AS `Material Key`,
  REPLACE(LTRIM(REPLACE(Material_Number ,'0',' ')),' ','0') AS `Material`,
  Mvmt_Type AS `Movement Type`,
  Source_ID,
  OPFLAG,
  SUM(Amt_Comp_Code_Curr) AS `R/3 Amount`,
  SUM(Weight_Qty) AS `Weight in KG`,
  SUM(Volume_Qty) AS `Volume in L15`
FROM `{uc_catalog_name}`.`{uc_curated_schema}`.{ fcb_dynamic_view_name }
WHERE Fiscal_Year = '{prev_1st_year}'
AND Posting_Period = '{prev_1st_month}'
AND Company_Code IN (SELECT Company_Code FROM `{uc_catalog_name}`.`{uc_curated_schema}`.mars_comp_code_control_table)
AND Profit_Center IN (SELECT Profit_Center FROM `{uc_catalog_name}`.`{uc_curated_schema}`.mars_pctr_control_table)
GROUP BY
  `Company Code`,
  `Profit Center`,
  `G/L Account Key`,
  `Plant`,
  `Plant Type`,
  `Material Key`,
  `Movement Type`,
  `Source_ID`,
  `OPFLAG`,
  `Material`
""")

# COMMAND ----------

# display(detail_report_df3)

# COMMAND ----------

# DBTITLE 1,Detail report data creation for Logistics
from pyspark.sql.functions import col,lit

detail_report_df4 = spark.sql(f"""
SELECT
  CL_Profit_Center AS `Profit Center`,
  Company_Code AS `Company Code`,
  Fiscal_Year AS `Fiscal Year`,
  Posting_Period AS `Month`,
  CL_GL_Account_Key AS `G/L Account Key`,
  GL_Account_Long_Text AS `G/L Account Name`,
  Functional_Area AS `Functional Area`,
  Plant AS `Supply Point`,
  REPLACE(LTRIM(REPLACE(Material_Key ,'0',' ')),' ','0') AS `Material Key`,
  REPLACE(LTRIM(REPLACE(Material_Number ,'0',' ')),' ','0') AS `Material`,
  Material_Description AS `Material Name`,
  REPLACE(LTRIM(REPLACE(Customer_Number ,'0',' ')),' ','0') AS `Customer Number`,
  KNA1_Name_1 AS `Customer Name`,
  Source_ID,
  OPFLAG,
  SUM(Amt_Comp_Code_Curr) AS `R/3 Amount`,
  SUM(Weight_Qty) AS `Weight in KG`
FROM
  `{uc_catalog_name}`.`{uc_curated_schema}`.{ fcb_dynamic_view_name }
  WHERE Fiscal_Year = '{prev_1st_year}'
  AND Posting_Period = '{prev_1st_month}'
  AND Company_Code IN (SELECT Company_Code FROM `{uc_catalog_name}`.`{uc_curated_schema}`.mars_comp_code_control_table)
  AND Profit_Center IN (SELECT Profit_Center FROM `{uc_catalog_name}`.`{uc_curated_schema}`.mars_pctr_control_table)
GROUP BY
  `Profit Center`,
  `Company Code`,
  `Fiscal Year`,
  `Month`,
  `G/L Account Key`,
  `G/L Account Name`,
  `Functional Area`,
  `Supply Point`,
  `Material Key`,
  `Material Name`,
  `Customer Number`,
  `Customer Name`,
  `Source_ID`,
  `OPFLAG`,
  `Material`
""")
detail_report_df4 = detail_report_df4.withColumn('R/3 Amount', detail_report_df4['R/3 Amount'] * lit(-1))

# COMMAND ----------

# DBTITLE 1,Renaming the columns with final standards
import pyspark.sql.functions as f
def select_and_rename_columns(df,columns):
    return df.select([f.col(c).alias(columns[c]) for c in columns.keys()])

hana_c1_Columns = {'Company Code':'Company_Code','Profit Center Key':'Profit_Center_Key','Cont_Level':'Cont_Level','G/L Account Key':'GL_Account_Key','G/L Account Name':'GL_Account_Name','Functional Area':'Functional_Area','FCB Plant':'FCB_Plant','Plant':'Plant','Plant Name':'Plant_Name','MM Plant':'MM_Plant','FCB Material':'FCB_Material','Material Key':'Material_Key','Material Name':'Material_Name','MM Material':'MM_Material','Customer_Vendor Number':'Customer_Vendor_Number','Customer_Vendor Name':'Customer_Vendor_Name','Movement Type FCB':'Movement_Type_FCB', 'Source_ID':'Source_ID', 'OPFLAG':'OPFLAG', 'Amount in LC':'Amount_in_LC','R/3 Amount':'R/3_Amount','Weight in KG':'Weight_in_KG','Volume in L15':'Volume_in_L15'}

# COMMAND ----------

# DBTITLE 1,HANA logic
from pyspark.sql.functions import when, concat, lit, substring
    
hana_c1_df = hana_c1_mapping_join

hana_c1_df = hana_c1_df.withColumn('FCB Plant', when(hana_c1_df["MM Receiving Plant"].isNull(), hana_c1_df["Plant"]).otherwise(hana_c1_df["MM Receiving Plant"])).withColumn('FCB Material', when(hana_c1_df["MM Receiving Material"].isNull(), hana_c1_df["Material"]).otherwise(hana_c1_df["MM Receiving Material"])).withColumn('Customer_Vendor Number', concat(hana_c1_df["Customer Number"], hana_c1_df["Vendor Number"])).withColumn('Customer_Vendor Name', when(hana_c1_df["Customer Name"].isNull(), hana_c1_df["Vendor Name"]).otherwise(hana_c1_df["Customer Name"])).withColumn('Movement Type FCB', when(hana_c1_df["Movement Type"].isNull(), hana_c1_df["MM Movement Type"]).otherwise(hana_c1_df["Movement Type"])).withColumn('Amount in LC', hana_c1_df['R/3 Amount'] * lit(-1)).withColumn('R/3 Amount', hana_c1_df['R/3 Amount'] * lit(-1))

# hana_c1_df = hana_c1_df.withColumn('FCB Plant', hana_c1_df["Plant"]).withColumn('FCB Material', hana_c1_df["Material Key"]).withColumn('Customer_Vendor Number', concat(hana_c1_df["Customer Number"], hana_c1_df["Vendor Number"])).withColumn('Customer_Vendor Name', when(hana_c1_df["Customer Name"].isNull(), hana_c1_df["Vendor Name"]).otherwise(hana_c1_df["Customer Name"])).withColumn('Movement Type FCB', when(hana_c1_df["Movement Type"].isNull(), hana_c1_df["MM Movement Type"]).otherwise(hana_c1_df["Movement Type"])).withColumn('Amount in LC', hana_c1_df['R/3 Amount'] * lit(-1)).withColumn('R/3 Amount', hana_c1_df['R/3 Amount'] * lit(-1))

hana_c1_df_final_303 = hana_c1_df.select('Company Code', 'Profit Center Key', 'Cont_Level', 'G/L Account Key', 'G/L Account Name', 'Functional Area', 'FCB Plant', 'Plant', 'Plant Name', 'MM Plant', substring('FCB Material', -9, 9).alias('FCB Material'), 'Material Key', 'Material Name', substring('MM Material', -9, 9).alias('MM Material'), substring('Customer_Vendor Number', -8, 8).alias('Customer_Vendor Number'), 'Customer_Vendor Name', 'Movement Type FCB', 'Source_ID', 'OPFLAG', 'Amount in LC', 'R/3 Amount', 'Weight in KG', 'Volume in L15')

hana_c1_df_final_303 = select_and_rename_columns(hana_c1_df_final_303, hana_c1_Columns)
hana_c1_df_final_303.createOrReplaceTempView("hana_c1_df_final_303")


# COMMAND ----------

# DBTITLE 1,HANA Dummy Logic

hana_dummy_df = hana_dummy_c1_mapping_join.filter(hana_dummy_c1_mapping_join['G/L Account Key'].isin('6341160', '6341170'))

hana_dummy_df = hana_dummy_df.withColumn('FCB Plant', when(hana_dummy_df["MM Receiving Plant"].isNull(), hana_dummy_df["Plant"]).otherwise(hana_dummy_df["MM Receiving Plant"])).withColumn('FCB Material', when(hana_dummy_df["MM Receiving Material"].isNull(), hana_dummy_df["Material"]).otherwise(hana_dummy_df["MM Receiving Material"])).withColumn('Customer_Vendor Number', concat(hana_dummy_df["Customer Number"], hana_dummy_df["Vendor Number"])).withColumn('Customer_Vendor Name', when(hana_dummy_df["Customer Name"].isNull(), hana_dummy_df["Vendor Name"]).otherwise(hana_dummy_df["Customer Name"])).withColumn('Movement Type FCB', when(hana_dummy_df["Movement Type"].isNull(), hana_dummy_df["MM Movement Type"]).otherwise(hana_dummy_df["Movement Type"])).withColumn('Amount in LC', hana_dummy_df['R/3 Amount'] * lit(-1)).withColumn('R/3 Amount', hana_dummy_df['R/3 Amount'] * lit(-1))

hana_dummy_df_final = hana_dummy_df.select('Company Code', 'Cont_Level', 'Profit Center Key', 'G/L Account Key', 'G/L Account Name', 'Functional Area', 'FCB Plant', 'Plant', 'Plant Name', 'MM Plant', substring('FCB Material', -9, 9).alias('FCB Material'), 'Material Key', 'Material Name', substring('MM Material', -9, 9).alias('MM Material'), substring('Customer_Vendor Number', -8, 8).alias('Customer_Vendor Number'), 'Customer_Vendor Name', 'Movement Type FCB', 'Source_ID', 'OPFLAG', 'Amount in LC', 'R/3 Amount', 'Weight in KG', 'Volume in L15')

hana_dummy_df_final = select_and_rename_columns(hana_dummy_df_final,hana_c1_Columns)

hana_dummy_df_final.createOrReplaceTempView("hana_dummy_df_final")

# COMMAND ----------

# display(hana_dummy_df_final)

# COMMAND ----------

# DBTITLE 1,G&L Logic
gandl_df = detail_report_df3.filter(detail_report_df3['G/L Account Key'].isin('6330020', '6330030', '6330040', '6330202', '6330203', '6330220', '6330230', '6330160', '6330220', '74803300') & ~detail_report_df3['Plant Type'].isin('ZTS', 'ZLI') & detail_report_df3['Movement Type'].isin('551','552', '951', '952', '701', '702', 'Y51', 'Y52', 'Y53', 'Y54', 'Y15', 'Y16', 'Y20', 'Y21', '911', '912', 'Y03', 'Y04', 'Y05', 'Y06', 'Y07', 'Y08', '915', '916')).select(col('Company Code').alias('Company'), substring('Profit Center',-6,6).alias('Profit_Center'), substring(col('G/L Account Key'),-7,7).alias('GL_Account'), col('Plant'), substring(col('Material'),-9,9).alias('Material'), 'Source_ID', 'OPFLAG', col('R/3 Amount').alias('Amount_in_LC'), col('Weight in KG').alias('Weight_in_KG'), col('Volume in L15').alias('Weight_in_L15'))

gandl_df.createOrReplaceTempView("gandl_df_107")

# COMMAND ----------

# display(gandl_df)

# COMMAND ----------

# DBTITLE 1,Logistics Logic
logistics_df = detail_report_df4.filter(detail_report_df4['G/L Account Key'].isin('6355025', '6355050', '6355070', '6355075') & detail_report_df4['G/L Account Name'].isNotNull()).select(col('Profit Center').alias('Profit_Center'), col('Company Code').alias('Company_Code'), col('Fiscal Year').alias('Fiscal_Year'), 'Month', col('G/L Account Key').alias('GL_Account'), col('G/L Account Name').alias('GL_Account_Name'),  col('Functional Area').alias('Functional_Area'), col('Material').alias('Material'), col('Material Name').alias('Material_Name'), col('Supply Point').alias('Supply_Point'), substring('Customer Number',-8,8).alias('Sold_to_party'), col('Customer Name').alias('Customer_Name'), 'Source_ID', 'OPFLAG',col('R/3 Amount').alias('Total'),col('Weight in KG').alias('Weight_in_KG'))

logistics_df.createOrReplaceTempView("logistics_df_108")

# COMMAND ----------

# DBTITLE 1,C1Jet_consignment
C1Jet_consignment_df = spark.sql(f"""
SELECT
  REPLACE(REPLACE(REPLACE(REPLACE(`Source_ID`, 'DD4_', 'D94_'), 'CD4_', 'C94_'), 'ZD4_', 'Z94_'), 'PD4_', 'P94_') AS `Source_ID`,
  OPFLAG,
  `Company_Code`,
  `Plant`,
  REPLACE(LTRIM(REPLACE(`Material`,'0',' ')),' ','0') AS `Material`,  
  `Receiving_Plant`,
  REPLACE(LTRIM(REPLACE(`Receiving_Issueing_Material`,'0',' ')),' ','0') AS Receiving_Material,
  `Material_Doc_Number` AS `Material_Document`,
  `Material_Item_Number` AS `Material_Document_Item`,
  `Movement_Type`,
  `Mode_of_Transport`,
  `ASTM_Quantity__L15` AS `Volume in L15`,
  `ASTM_Quantity__KG` AS `Quantity_KG`,
  `Amount_In_local_Currency` AS `Amount_in_LC`
FROM `{uc_catalog_name}`.`{uc_eh_mm_schema}`.`fact_hm_daily_movement_summary`
WHERE substr(Document_Date,1,4) = {prev_1st_year}
AND concat('0',substr(Document_Date,5,2)) = {prev_1st_month}
AND `Movement_Type` in (SELECT Movement_Type FROM `{uc_catalog_name}`.`{uc_curated_schema}`.mars_mvmt_type_control_table)
-- AND INFOPROV IN ('YFHM0120', 'YFHM0160', 'YFHM0170', 'YFHM0150')
AND `Company_Code` IN (SELECT Company_Code FROM `{uc_catalog_name}`.`{uc_curated_schema}`.mars_comp_code_control_table)
AND REPLACE(LTRIM(REPLACE(`Material`,'0',' ')),' ','0') IN (SELECT Material_Key FROM `{uc_catalog_name}`.`{uc_curated_schema}`.mars_material_control_table)
AND REPLACE(LTRIM(REPLACE(`Receiving_Issueing_Material`,'0',' ')),' ','0') IN (SELECT Material_Key FROM `{uc_catalog_name}`.`{uc_curated_schema}`.mars_material_control_table)
 """)

  #  VALUE_LC AS `Amount_in_LC`,
  # `/BIC/ZASTML15` AS `Volume in L15`,
  # `/BIC/ZASTMKG` AS `Quantity_KG`


# COMMAND ----------

# DBTITLE 1,Text_fields_for_C1Jet_consignment_df

#Company_Name_Text
comp_md_df = spark.sql(f"""select `Company_Code` AS `Company_Code_md`, `Name_Comp_Code`  AS `Company_Name` from `{uc_catalog_name}`.`{uc_eh_md_schema}`.md_fi_comp_code""")

#Plant_Name_Text
plant_md_df = spark.sql(f"""select `Plant` AS `Plant_md`, `Name`  AS `Plant_Name` from `{uc_catalog_name}`.`{uc_eh_md_schema}`.md_mm_plant""")

#Material_Name_Text
material_md_df = spark.sql(f"""select `Material_Number` AS `Material_Number_md`, `Material_Description`  AS `Material_Name` from `{uc_catalog_name}`.`{uc_eh_md_schema}`.md_mm_material""")

#Receiving_Plant_Name_Text
rec_plant_md_df = spark.sql(f"""select `Plant` AS `Plant_rec`, `Name`  AS `Rec_Plant_Name` from `{uc_catalog_name}`.`{uc_eh_md_schema}`.md_mm_plant""")

#Movement_Type_Text
md_mm_mov_type_df = spark.sql(f"""select `Mvmt_Type` AS `Mvmt_Type_md`, `Mvmt_Type_Text`  AS `Movement_Type_Text` from `{uc_catalog_name}`.`{uc_eh_md_schema}`.md_mm_mov_type""")

#Mode_of_Transport_Text
df_tvtrt = spark.read.table(f"`{uc_catalog_name}`.`{uc_euh_schema}`.tvtrt")
df_tvtrt = client_filter(df_tvtrt)
df_tvtrt = df_tvtrt.select(trim("SPRAS").alias('SPRAS'), col('VKTRA').alias('MOT_md'), col('BEZEI').alias('Mod_of_Transport_Name'))
mod_of_transport_df = language_filter(df_tvtrt)
mod_of_transport_df = mod_of_transport_df.select(col('MOT_md'), col('Mod_of_Transport_Name'))



# COMMAND ----------

# DBTITLE 1,C1Jet_consignment_df_final
#Company_Name_Text
C1Jet_consignment_df_cmp_pl_md = C1Jet_consignment_df.join(comp_md_df,trim(lower(C1Jet_consignment_df["Company_Code"]))==trim(lower(comp_md_df["Company_Code_md"])),"leftouter").select(C1Jet_consignment_df["*"],comp_md_df["Company_Name"]) 

#Plant_Name_Text
C1Jet_consignment_df_pl_md = C1Jet_consignment_df_cmp_pl_md.join(plant_md_df,trim(lower(C1Jet_consignment_df_cmp_pl_md["Plant"]))==trim(lower(plant_md_df["Plant_md"])),"leftouter").select(C1Jet_consignment_df_cmp_pl_md["*"],plant_md_df["Plant_Name"]) 

#Material_Name_Text
C1Jet_consignment_df_pl_material_md = C1Jet_consignment_df_pl_md.join(material_md_df,trim(lower(C1Jet_consignment_df_pl_md["Material"]))==trim(lower(material_md_df["Material_Number_md"])),"leftouter").select(C1Jet_consignment_df_pl_md["*"],material_md_df["Material_Name"]) 

#Receiving_Plant_Name_Text
C1Jet_consignment_df_pl_material_rec_md = C1Jet_consignment_df_pl_material_md.join(rec_plant_md_df,trim(lower(C1Jet_consignment_df_pl_material_md["Receiving_Plant"]))==trim(lower(rec_plant_md_df["Plant_rec"])),"leftouter").select(C1Jet_consignment_df_pl_material_md["*"],rec_plant_md_df["Rec_Plant_Name"]) 

#Movement_Type_Text
C1Jet_consignment_df_pl_material_rec_mov_md = C1Jet_consignment_df_pl_material_rec_md.join(md_mm_mov_type_df,trim(lower(C1Jet_consignment_df_pl_material_rec_md["Movement_Type"]))==trim(lower(md_mm_mov_type_df["Mvmt_Type_md"])),"leftouter").select(C1Jet_consignment_df_pl_material_rec_md["*"],md_mm_mov_type_df["Movement_Type_Text"]) 

#Mode_of_Transport_Text
C1Jet_consignment_df_final = C1Jet_consignment_df_pl_material_rec_mov_md.join(mod_of_transport_df,trim(lower(C1Jet_consignment_df_pl_material_rec_mov_md["Mode_of_Transport"]))==trim(lower(mod_of_transport_df["MOT_md"])),"leftouter").select(C1Jet_consignment_df_pl_material_rec_mov_md["*"],mod_of_transport_df["Mod_of_Transport_Name"])

C1Jet_consignment_df_final.createOrReplaceTempView("C1Jet_consignment_102")

# COMMAND ----------

# DBTITLE 1,103-BCS Reversal
bcs_reversal_df = spark.sql(f"""
SELECT
  "BCS reversal" AS Source,
  Company,
  Account,
  Customer_Or_Vendor,
  Plant,
  Material,
  IF(Account = "6380201", "OP37", Functional_Area) AS Functional_Area,
  sum(Weight_Kg) AS SumOfWeight_kg,
  sum(C1_Margin_FIFO) AS SumOfC1_Margin_FIFO
FROM
  `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_bps_reversal
GROUP BY
  Source,
  Company,
  Account,
  Customer_Or_Vendor,
  Plant,
  Material,
  Functional_Area
  """)

# COMMAND ----------

# DBTITLE 1,104 - BPS New
bps_new_df = spark.sql(f"""SELECT
  "BCS new" AS Source,
  Company,
  Account,
  Customer_Or_Vendor,
  Plant,
  Material,
  Functional_Area,
  sum(Weight_Kg) AS SumOfWeight_Kg,
  sum(C1_Margin_FIFO) AS SumOfC1_Margin_FIFO
FROM
  `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_bps_new
GROUP BY
  Source,
  Company,
  Account,
  Customer_Or_Vendor,
  Plant,
  Material,
  Functional_Area""")

# COMMAND ----------

# DBTITLE 1,105 - NA Allocation
na_allocation_df = spark.sql(f"""
SELECT
  "NA allocation" AS Source,
  Company,
  Account,
  Cust_Or_Ven_Number,
  Plant,
  TRIM(Material) AS Material,
  SUM(Weight_Kg) AS SumOfWeight_Kg,
  SUM(C1_Margin_FIFO) AS SumOfC1_Margin_FIFO
FROM
  `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_na_allocation
GROUP BY
  Source,
  Company,
  Account,
  Cust_Or_Ven_Number,
  Plant,
  Material""")

# COMMAND ----------

# DBTITLE 1,106 - Purchase CSO rate - 1st
purchase_CSO_rate_106_1_df = spark.sql(f"""
 SELECT
  hana_c1_df_final_303.Company_Code AS 303_Company_Code,
  hana_c1_df_final_303.Profit_Center_Key AS 303_Profit_Center_Key,
  hana_c1_df_final_303.Cont_Level AS 303_Cont_Level,
  hana_c1_df_final_303.GL_Account_Key AS 303_GL_Account_Key,
  hana_c1_df_final_303.Functional_Area AS 303_Functional_Area,
  hana_c1_df_final_303.FCB_Plant AS 303_FCB_Plant,
  hana_c1_df_final_303.FCB_Material AS 303_FCB_Material,
  hana_c1_df_final_303.Customer_Vendor_Number AS 303_Customer_Vendor_Number,
  hana_c1_df_final_303.Source_ID,
  hana_c1_df_final_303.OPFLAG,
  sum(hana_c1_df_final_303.Weight_in_KG) AS SumOfWeight_in_KG,
  custom_uc_mars_purchase_cso.CSO_fee_per_ton AS 106_CSO_fee_per_ton,
  (
    -1 * SumOfWeight_in_KG * 106_CSO_fee_per_ton / 1000
  ) AS CSO_Amount
FROM
  hana_c1_df_final_303
  LEFT JOIN `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_purchase_cso custom_uc_mars_purchase_cso ON (
    trim(lower(hana_c1_df_final_303.Company_Code)) = trim(lower(custom_uc_mars_purchase_cso.Company))
    AND trim(lower(REPLACE(LTRIM(REPLACE(hana_c1_df_final_303.FCB_Material ,'0',' ')),' ','0'))) = trim(lower(custom_uc_mars_purchase_cso.Material))
  )
GROUP BY
  303_Company_Code,
  303_Profit_Center_Key,
  303_Cont_Level,
  303_GL_Account_Key,
  303_Functional_Area,
  303_FCB_Plant,
  303_FCB_Material,
  303_Customer_Vendor_Number,
  hana_c1_df_final_303.Source_ID,
  hana_c1_df_final_303.OPFLAG,
  106_CSO_fee_per_ton
HAVING
  (
    (
      (303_GL_Account_Key = "6340010")
      AND (
        (303_FCB_Plant) <> "D081"
        OR (303_FCB_Plant) <> "Z022"
        OR (303_FCB_Plant) <> "Z023"
        OR (303_FCB_Plant) <> "Z012"
        Or (303_FCB_Plant) <> "Z026"
      )
      AND (
        (303_Customer_Vendor_Number) Like "6%"
        OR (303_Customer_Vendor_Number) Like "Z1415"
      )
      AND ((SumOfWeight_in_KG)) <> 0
    )
    AND ((106_CSO_fee_per_ton) IS NOT NULL)
  )
 """)
purchase_CSO_rate_106_1_df.createOrReplaceTempView("purchase_CSO_rate_106_1_df")

# COMMAND ----------

# display(purchase_CSO_rate_106_1_df)

# COMMAND ----------

# DBTITLE 1,106 - Purchase CSO rate - 2nd
purchase_CSO_rate_106_2_df = spark.sql(f"""
SELECT
  hana_c1_df_final_303.Company_Code AS 303_Company_Code,
  hana_c1_df_final_303.Profit_Center_Key AS 303_Profit_Center_Key,
  hana_c1_df_final_303.Cont_Level AS 303_Cont_Level,
  hana_c1_df_final_303.GL_Account_Key AS 303_GL_Account_Key,
  hana_c1_df_final_303.Functional_Area AS 303_Functional_Area,
  hana_c1_df_final_303.FCB_Plant AS 303_FCB_Plant,
  hana_c1_df_final_303.FCB_Material AS 303_FCB_Material,
  hana_c1_df_final_303.Customer_Vendor_Number AS 303_Customer_Vendor_Number,
  hana_c1_df_final_303.Source_ID,
  hana_c1_df_final_303.OPFLAG,
  sum(hana_c1_df_final_303.Weight_in_KG) AS SumOfWeight_in_KG,
  custom_uc_mars_purchase_cso.CSO_fee_per_ton AS 106_CSO_fee_per_ton,
  (
    -1 * SumOfWeight_in_KG * 106_CSO_fee_per_ton / 1000
  ) AS CSO_Amount
FROM
  hana_c1_df_final_303
  LEFT JOIN `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_purchase_cso custom_uc_mars_purchase_cso ON (
    trim(lower(hana_c1_df_final_303.Company_Code)) = trim(lower(custom_uc_mars_purchase_cso.Company))
    AND trim(lower(REPLACE(LTRIM(REPLACE(hana_c1_df_final_303.FCB_Material ,'0',' ')),' ','0'))) = trim(lower(custom_uc_mars_purchase_cso.Material))
  )
GROUP BY
  303_Company_Code,
  303_Profit_Center_Key,
  303_Cont_Level,
  303_GL_Account_Key,
  303_Functional_Area,
  303_FCB_Plant,
  303_FCB_Material,
  303_Customer_Vendor_Number,
  hana_c1_df_final_303.Source_ID,
  hana_c1_df_final_303.OPFLAG,
  106_CSO_fee_per_ton
HAVING
  (
    (
      (303_GL_Account_Key = "6002220")
      AND ((SumOfWeight_in_KG)) <> 0
    )
    AND ((106_CSO_fee_per_ton) IS NOT NULL)
  )
  """)
purchase_CSO_rate_106_2_df.createOrReplaceTempView("purchase_CSO_rate_106_2_df")

# COMMAND ----------

# display(purchase_CSO_rate_106_2_df)

# COMMAND ----------

# DBTITLE 1,107: G&L
GnL_107_df = spark.sql(f"""
SELECT
  "G&L" AS Margin_Bucket,
  gandl_df_107.Company AS Company,
  gandl_df_107.Plant AS Plant,
  custom_uc_mars_master_plant.Plant_Name AS Plant_Name,
  gandl_df_107.Material AS Material,
  custom_uc_mars_master_material.Material_Name AS Material_Name,
  custom_uc_mars_master_material.Material_Group_Finance AS Material_Group_Finance,
  custom_uc_mars_master_material.Bio AS Bio,
  gandl_df_107.Source_ID,
  gandl_df_107.OPFLAG,
  sum(gandl_df_107.Weight_in_KG) AS SumOfWeight_in_KG,
  (-1 * sum(gandl_df_107.Amount_in_LC)) AS Amount_LC
FROM
  gandl_df_107
  LEFT JOIN `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_plant custom_uc_mars_master_plant ON trim(lower(gandl_df_107.Plant)) = trim(lower(custom_uc_mars_master_plant.Plant))
  LEFT JOIN `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_material custom_uc_mars_master_material
  ON trim(lower((REPLACE(LTRIM(REPLACE(gandl_df_107.Material ,'0',' ')),' ','0')))) = trim(lower(custom_uc_mars_master_material.Material))
GROUP BY
  Margin_Bucket,
  gandl_df_107.Company,
  gandl_df_107.Plant,
  custom_uc_mars_master_plant.Plant_Name,
  gandl_df_107.Material,
  custom_uc_mars_master_material.Material_Name,
  custom_uc_mars_master_material.Material_Group_Finance,
  custom_uc_mars_master_material.Bio,
  gandl_df_107.Source_ID,
  gandl_df_107.OPFLAG
HAVING
  Amount_LC <> 0""")

# COMMAND ----------

# DBTITLE 1,108:455a:Logistics recovery C1
logistics_recovery_c1_455_df = spark.sql(f"""
SELECT
  "Logistics" AS Margin_Bucket,
  logistics_df_108.Company_Code AS Company_Code,
  logistics_df_108.Supply_Point AS Supply_Point,
  custom_uc_mars_master_plant.Plant_Name AS Plant_Name,
  logistics_df_108.Material AS Material,
  custom_uc_mars_master_material.Material_Name AS Material_Name,
  custom_uc_mars_master_material.Material_Group_Finance AS Material_Group_Finance,
  custom_uc_mars_master_material.Bio,
  logistics_df_108.Source_ID,
  logistics_df_108.OPFLAG,
  sum(logistics_df_108.Weight_in_KG) AS SumOfWeight_in_KG,
  ((-1 * sum(logistics_df_108.Total)) * -1) AS Amount
FROM
  logistics_df_108
  INNER JOIN `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_plant custom_uc_mars_master_plant ON trim(lower(logistics_df_108.Supply_Point)) = trim(lower(custom_uc_mars_master_plant.Plant))
  INNER JOIN `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_material custom_uc_mars_master_material ON trim(lower((REPLACE(LTRIM(REPLACE(logistics_df_108.Material ,'0',' ')),' ','0')))) = trim(lower(custom_uc_mars_master_material.Material))
GROUP BY
  Margin_Bucket,
  logistics_df_108.Company_Code,
  logistics_df_108.Supply_Point,
  custom_uc_mars_master_plant.Plant_Name,
  logistics_df_108.Material,
  custom_uc_mars_master_material.Material_Name,
  custom_uc_mars_master_material.Material_Group_Finance,
  custom_uc_mars_master_material.Bio,
  logistics_df_108.Source_ID,
  logistics_df_108.OPFLAG
  """)

# COMMAND ----------

# DBTITLE 1,108:602:Logistics recovery C1
logistics_recovery_c1_602_df = spark.sql(f"""
SELECT
  "Logistics" AS Margin_Bucket,
  hana_c1_df_final_303.Company_Code AS Company_Code,
  custom_uc_mars_master_fa.FA_Grouping AS FA_Grouping,
  hana_c1_df_final_303.Customer_Vendor_Number AS Customer_Vendor_Number,
  hana_c1_df_final_303.Customer_Vendor_Name AS Customer_Vendor_Name,
  hana_c1_df_final_303.FCB_Plant AS FCB_Plant,
  custom_uc_mars_master_plant.Plant_Name AS Plant_Name,
  custom_uc_mars_master_material.Material_Group_Finance AS Material_Group_Finance,
  hana_c1_df_final_303.FCB_Material AS FCB_Material,
  custom_uc_mars_master_material.Material_Name AS Material_Name,
  hana_c1_df_final_303.Source_ID AS Source_ID,
  hana_c1_df_final_303.OPFLAG,
  sum(hana_c1_df_final_303.Weight_in_KG) AS SumOfWeight_in_KG,
  (-1 * sum(logistics_df_108.Total)) AS Logistic_recovery,
  (-1 * sum(hana_c1_df_final_303.Amount_in_LC)) AS Margin,
  (Logistic_recovery / SumOfWeight_in_KG * 1000) AS Logistic_per_MT
FROM
  `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_material custom_uc_mars_master_material
  RIGHT JOIN (
    `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_plant custom_uc_mars_master_plant
    RIGHT JOIN (
      logistics_df_108
      RIGHT JOIN hana_c1_df_final_303 ON (
        trim(lower(logistics_df_108.Sold_to_party)) = trim(lower(hana_c1_df_final_303.Customer_Vendor_Number))
      )
      AND (
        trim(lower(logistics_df_108.Supply_Point)) = trim(lower(hana_c1_df_final_303.FCB_Plant))
      )
      AND (
        trim(lower(logistics_df_108.Material)) = trim(lower(hana_c1_df_final_303.FCB_Material))
      )
      AND (
        trim(lower(logistics_df_108.Company_Code)) = trim(lower(hana_c1_df_final_303.Company_Code))
      )
    ) ON trim(lower(custom_uc_mars_master_plant.Plant)) = trim(lower(hana_c1_df_final_303.FCB_Plant))
  ) ON (
    trim(lower(custom_uc_mars_master_material.Material)) = trim(lower((REPLACE(LTRIM(REPLACE(hana_c1_df_final_303.FCB_Material ,'0',' ')),' ','0')))) 
  )
  LEFT JOIN `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_fa custom_uc_mars_master_fa ON trim(lower(hana_c1_df_final_303.Functional_Area)) = trim(lower(custom_uc_mars_master_fa.Functional_Area))
GROUP BY
  Margin_Bucket,
  hana_c1_df_final_303.Company_Code,
  custom_uc_mars_master_fa.FA_Grouping,
  hana_c1_df_final_303.Customer_Vendor_Number,
  hana_c1_df_final_303.Customer_Vendor_Name,
  hana_c1_df_final_303.FCB_Plant,
  custom_uc_mars_master_plant.Plant_Name,
  custom_uc_mars_master_material.Material_Group_Finance,
  hana_c1_df_final_303.FCB_Material,
  custom_uc_mars_master_material.Material_Name,
  hana_c1_df_final_303.Source_ID,
  hana_c1_df_final_303.OPFLAG
HAVING
  hana_c1_df_final_303.Customer_Vendor_Number IS NOT NULL                                        
""")
logistics_recovery_c1_602_df.createOrReplaceTempView("logistics_recovery_c1_602_205_df")

# COMMAND ----------

# DBTITLE 1,110:461: manual margin explanation
manual_margin_explanation_461_df = spark.sql(f"""
SELECT
  custom_uc_mars_manual_margin_explanation.company AS Company,
  custom_uc_mars_manual_margin_explanation.margin_driver AS Margin_Driver,
  custom_uc_mars_manual_margin_explanation.plant AS Plant,
  custom_uc_mars_master_plant.plant_name AS Pant_Name,
  custom_uc_mars_manual_margin_explanation.Material AS Material,
  custom_uc_mars_master_material.material_name AS Material_Name,
  custom_uc_mars_master_material.material_group_finance AS Material_Group_Finance,
  custom_uc_mars_master_material.bio AS Bio,
  sum(
    custom_uc_mars_manual_margin_explanation.C1_Margin_FIFO
  ) AS SumOfC1_Margin_FIFO
FROM
  (
    `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_manual_margin_explanation custom_uc_mars_manual_margin_explanation
    LEFT JOIN `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_plant custom_uc_mars_master_plant ON trim(lower(custom_uc_mars_manual_margin_explanation.Plant)) = trim(lower(custom_uc_mars_master_plant.Plant))
  )
  LEFT JOIN `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_material custom_uc_mars_master_material ON trim(lower(custom_uc_mars_manual_margin_explanation.Material)) = trim(lower(custom_uc_mars_master_material.Material))
GROUP BY
  custom_uc_mars_manual_margin_explanation.company,
  custom_uc_mars_manual_margin_explanation.margin_driver,
  custom_uc_mars_manual_margin_explanation.plant,
  custom_uc_mars_master_plant.plant_name,
  custom_uc_mars_manual_margin_explanation.Material,
  custom_uc_mars_master_material.material_name,
  custom_uc_mars_master_material.material_group_finance,
  custom_uc_mars_master_material.bio
HAVING
  custom_uc_mars_manual_margin_explanation.margin_driver <> "Bio"
  AND SumOfC1_Margin_FIFO <> 0
""")

# COMMAND ----------

# DBTITLE 1,110:461a: manual margin explanation
manual_margin_explanation_461a_df = spark.sql(f"""
SELECT
  custom_uc_mars_manual_margin_explanation.company AS Company,
  custom_uc_mars_manual_margin_explanation.margin_driver AS Margin_Driver,
  custom_uc_mars_manual_margin_explanation.plant AS Plant,
  custom_uc_mars_master_plant.plant_name AS Pant_Name,
  custom_uc_mars_manual_margin_explanation.Material AS Material,
  custom_uc_mars_master_material.material_name AS Material_Name,
  custom_uc_mars_master_material.material_group_finance AS Material_Group_Finance,
  custom_uc_mars_master_material.bio AS Bio,
  sum(
    custom_uc_mars_manual_margin_explanation.C1_Margin_FIFO
  ) AS SumOfC1_Margin_FIFO
FROM
  (
    `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_manual_margin_explanation custom_uc_mars_manual_margin_explanation
    LEFT JOIN `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_plant custom_uc_mars_master_plant ON trim(lower(custom_uc_mars_manual_margin_explanation.Plant)) = trim(lower(custom_uc_mars_master_plant.Plant))
  )
  LEFT JOIN `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_master_material custom_uc_mars_master_material ON trim(lower(custom_uc_mars_manual_margin_explanation.Material)) = trim(lower(custom_uc_mars_master_material.Material))
GROUP BY
  custom_uc_mars_manual_margin_explanation.company,
  custom_uc_mars_manual_margin_explanation.margin_driver,
  custom_uc_mars_manual_margin_explanation.plant,
  custom_uc_mars_master_plant.plant_name,
  custom_uc_mars_manual_margin_explanation.Material,
  custom_uc_mars_master_material.material_name,
  custom_uc_mars_master_material.material_group_finance,
  custom_uc_mars_master_material.bio
HAVING
  custom_uc_mars_manual_margin_explanation.margin_driver = "Bio"
  AND SumOfC1_Margin_FIFO <> 0
""")

# COMMAND ----------

# DBTITLE 1,112:361_Logistics_FTP
logistics_tfp_361_df = spark.sql(f"""
SELECT
  custom_uc_mars_sales_for_logistics.company AS Company,
  custom_uc_mars_sales_for_logistics.fcb_plant AS FCB_Plant,
  custom_uc_mars_sales_for_logistics.fcb_plant_name AS FCB_Plant_Name_Pl,
  custom_uc_mars_sales_for_logistics.fcb_material AS FCB_Material,
  custom_uc_mars_sales_for_logistics.material_name AS Material_Name,
  custom_uc_mars_sales_for_logistics.channel AS Channel,
  sum(
    custom_uc_mars_sales_for_logistics.SumOfQuantity_KG
  ) AS SumOfSumOfQuantity_KG,
  sum(
    custom_uc_mars_sales_for_logistics.SumOfVolume_L15
  ) AS SumOfSumOfVolume_L15,
  custom_uc_mars_logistics_tfp.PT_Cost_LC_To AS PT_Cost_LC_To,
  custom_uc_mars_logistics_tfp.SH_LC_To AS SH_LC_To,
  custom_uc_mars_logistics_tfp.PT_Cost_LC_Cbm AS PT_Cost_LC_Cbm,
  custom_uc_mars_logistics_tfp.SH_LC_Cbm AS PT_Cost_LC_Cbm,
  (SumOfSumOfVolume_L15 / 1000 * (PT_Cost_LC_Cbm + SH_LC_Cbm)) AS Logistic_exp_VOL,
  (SumOfSumOfQuantity_KG / 1000 * (PT_Cost_LC_To + SH_LC_To)) AS Logistic_exp_WEIGHT,
  ((SumOfSumOfVolume_L15 / 1000 * (PT_Cost_LC_Cbm + SH_LC_Cbm)) + (SumOfSumOfQuantity_KG / 1000 * (PT_Cost_LC_To + SH_LC_To))) AS Expenses
FROM
  `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_sales_for_logistics custom_uc_mars_sales_for_logistics
  LEFT JOIN `{uc_catalog_name}`.`{uc_curated_schema}`.custom_uc_mars_logistics_tfp custom_uc_mars_logistics_tfp ON (
    trim(lower(custom_uc_mars_sales_for_logistics.company)) = trim(lower(custom_uc_mars_logistics_tfp.company_code))
  )
  AND (
    custom_uc_mars_sales_for_logistics.fcb_plant = custom_uc_mars_logistics_tfp.fcb_plant
  )
  AND (
    custom_uc_mars_sales_for_logistics.tfp_logistics_cost_mapping = custom_uc_mars_logistics_tfp.tfp_map_logistics
  )
GROUP BY
  custom_uc_mars_sales_for_logistics.company,
  custom_uc_mars_sales_for_logistics.fcb_plant,
  custom_uc_mars_sales_for_logistics.fcb_plant_name,
  custom_uc_mars_sales_for_logistics.fcb_material,
  custom_uc_mars_sales_for_logistics.material_name,
  custom_uc_mars_sales_for_logistics.channel,
  custom_uc_mars_logistics_tfp.PT_Cost_LC_To,
  custom_uc_mars_logistics_tfp.SH_LC_To,
  custom_uc_mars_logistics_tfp.PT_Cost_LC_Cbm,
  custom_uc_mars_logistics_tfp.SH_LC_Cbm
HAVING
  custom_uc_mars_sales_for_logistics.channel NOT LIKE 'CN'
""")
