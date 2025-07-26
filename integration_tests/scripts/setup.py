# Databricks notebook source
# MAGIC %pip install --upgrade pip

# COMMAND ----------

# MAGIC %pip install -r ../../requirements.txt

# COMMAND ----------

import os
os.environ["pipeline"] = "databricks"

# COMMAND ----------

from datta_pipeline_library.core.base_config import (
    BaseConfig,
    CommonConfig,
    EnvConfig,
    GreatExpectationsConfig,
)
from datta_pipeline_library.helpers.adls import configure_spark_to_use_spn_to_write_to_adls_gen2
from datta_pipeline_library.helpers.spn import AzureSPN

# COMMAND ----------

# DBTITLE 1,Parameters
unique_repo_branch_id = dbutils.widgets.get(name="unique_repo_branch_id")
unique_repo_branch_id_schema = dbutils.widgets.get(name="unique_repo_branch_id_schema")
repos_path = dbutils.widgets.get(name="repos_path")
env = dbutils.widgets.get(name="env")

common_conf = CommonConfig.from_file("../../conf/common/common_conf.json")
env_conf = EnvConfig.from_file(f"../../conf/{env}/conf.json")

kv = env_conf.kv_key

# values from key vault
tenant_id = dbutils.secrets.get(scope=kv, key="AZ-AS-SPN-DATTA-TENANT-ID")
spn_client_id = dbutils.secrets.get(scope=kv, key=env_conf.spn_client_id_key)
spn_client_secret = dbutils.secrets.get(scope=kv, key=env_conf.spn_client_secret_key)

spn = AzureSPN(tenant_id, spn_client_id, spn_client_secret)

#gx_config = GreatExpectationsConfig(azure_conn_str)
#base_config = BaseConfig.from_confs(env_conf, common_conf, gx_config)
base_config = BaseConfig.from_confs(env_conf, common_conf)
base_config.set_unique_id(unique_repo_branch_id)
base_config.set_unique_id_schema(unique_repo_branch_id_schema)

# COMMAND ----------

configure_spark_to_use_spn_to_write_to_adls_gen2(env_conf.storage_account, spn)

# COMMAND ----------

# DBTITLE 1,Configuration
uc_catalog = base_config.get_uc_catalog_name()
eh_schema = base_config.get_uc_eh_schema()
curated_schema = base_config.get_uc_curated_schema()

eh_folder_path = base_config.get_eh_folder_path()
curated_folder_path = base_config.get_curated_folder_path()

print("unity catalog: ", uc_catalog)
print("curated schema: ", curated_schema)
print("curated folder path: ", curated_folder_path)

eh_fi_schema = eh_schema
eh_md_schema = eh_schema.replace("-finance-", "-masterdata-")

eh_fi_folder_path=eh_folder_path
eh_md_folder_path=eh_folder_path.replace("/finance_dev", "/masterdata_dev")

print("eh fi schema: ", eh_fi_schema)
print("eh fi folder path: ", eh_fi_folder_path)
print("eh schema: ", eh_md_schema)
print("eh folder path: ", eh_md_folder_path)

# COMMAND ----------

# MAGIC %md ## Delete UC schemas and tables

# COMMAND ----------

eh_md_table_list = ['md_fi_comp_code','md_fi_gl_account','md_mm_material','md_mm_vendor','md_mm_zmaterial','md_sd_customer','md_mm_plant', 'md_mm_mov_type', 'md_sd_dist_channel', 'md_fi_profit_center']
if env == "dev":
    for table_name in eh_md_table_list:
        spark.sql(f"DROP TABLE IF EXISTS `{uc_catalog}`.`{eh_md_schema}`.table_name")

# COMMAND ----------

eh_fi_table_list = ['fact_fi_act_line_item']
if env == "dev":
    for table_name in eh_fi_table_list:
        spark.sql(f"DROP TABLE IF EXISTS `{uc_catalog}`.`{eh_fi_schema}`.table_name")

# COMMAND ----------

curated_table_list = ['use_case_fcb_dn_supply_margin']
if env == "dev":
    for table_name in curated_table_list:
        spark.sql(f"DROP TABLE IF EXISTS `{uc_catalog}`.`{curated_schema}`.table_name")

# COMMAND ----------

if env == "dev":
    spark.sql(f"DROP SCHEMA IF EXISTS `{uc_catalog}`.`{eh_md_schema}` CASCADE")
    spark.sql(f"DROP SCHEMA IF EXISTS `{uc_catalog}`.`{eh_fi_schema}` CASCADE")

# COMMAND ----------

if env == "dev":
    spark.sql(f"DROP SCHEMA IF EXISTS `{uc_catalog}`.`{curated_schema}` CASCADE")

# COMMAND ----------

# MAGIC %md ## Delete ADLS folders

# COMMAND ----------

# DBTITLE 1,Delete FI EH folder
if env == "dev":
    dbutils.fs.rm(eh_fi_folder_path, recurse=True)

# COMMAND ----------

# DBTITLE 1,Delete MD EH folder
if env == "dev":
    dbutils.fs.rm(eh_md_folder_path, recurse=True)

# COMMAND ----------

# DBTITLE 1,Delete CURATED folder
if env == "dev":
    dbutils.fs.rm(curated_folder_path, recurse=True)

# COMMAND ----------

# MAGIC %md ## Create Schemas

# COMMAND ----------

# DBTITLE 1,Create UC schemas
if env == "dev":
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{uc_catalog}`.`{eh_fi_schema}`")

# COMMAND ----------

if env == "dev":
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{uc_catalog}`.`{eh_md_schema}`")

# COMMAND ----------

if env == "dev":
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{uc_catalog}`.`{curated_schema}`")

# COMMAND ----------

# DBTITLE 1,md_mm_zmaterial create statement
md_mm_zmaterial_table_path = eh_md_folder_path + "/MD_MM_ZMATERIAL"
spark.sql(f"""CREATE TABLE `{uc_catalog}`.`{eh_md_schema}`.md_mm_zmaterial (
  Material STRING,
  DEX_Code STRING,
  Commodity_Code STRING,
  Medium_Description STRING,
  Commodity_Code_Text STRING,
  ingested_at TIMESTAMP,
  Source_ID STRING,
  OPFLAG STRING)
USING delta
LOCATION '{md_mm_zmaterial_table_path}'""")

# COMMAND ----------

# DBTITLE 1,md_sd_customer create statement
md_sd_customer_table_path = eh_md_folder_path + "/MD_SD_CUSTOMER"
spark.sql(f"""CREATE TABLE `{uc_catalog}`.`{eh_md_schema}`.md_sd_customer (
  Customer_Number STRING,
  Name_1 STRING,
  ingested_at TIMESTAMP,
  Source_ID STRING,
  OPFLAG STRING)
USING delta
LOCATION '{md_sd_customer_table_path}'""")

# COMMAND ----------

# DBTITLE 1,md_mm_vendor create statement
md_mm_vendor_table_path = eh_md_folder_path + "/MD_MM_VENDOR"
spark.sql(f"""CREATE TABLE `{uc_catalog}`.`{eh_md_schema}`.md_mm_vendor (
  Account_No_Supplier STRING,
  Name_1 STRING,
  ingested_at TIMESTAMP,
  Source_ID STRING,
  OPFLAG STRING)
USING delta
LOCATION '{md_mm_vendor_table_path}'""")

# COMMAND ----------

# DBTITLE 1,md_mm_plant create statement
md_mm_plant_table_path = eh_md_folder_path + "/MD_MM_PLANT"
spark.sql(f"""CREATE TABLE `{uc_catalog}`.`{eh_md_schema}`.md_mm_plant (
  Plant STRING,
  Name STRING,
  Sales_District STRING,
  Dist_Profile_Plant STRING,
  Description STRING,
  ingested_at TIMESTAMP,
  Source_ID STRING,
  OPFLAG STRING)
USING delta
LOCATION '{md_mm_plant_table_path}'""")

# COMMAND ----------

# DBTITLE 1,md_mm_material create statement
md_mm_material_table_path = eh_md_folder_path + "/MD_MM_MATERIAL"
spark.sql(f"""CREATE TABLE `{uc_catalog}`.`{eh_md_schema}`.md_mm_material (
  Material_Description STRING,
  Material_Group STRING,
  Material_Number STRING,
  Product_Group_Code STRING,
  Product_Group_Description STRING,
  Product_Sub_Class STRING,
  Product_Sub_Class_Description STRING,
  Material_Group_Description STRING,
  ingested_at TIMESTAMP,
  Source_ID STRING,
  OPFLAG STRING)
USING delta
LOCATION '{md_mm_material_table_path}'""")

# COMMAND ----------

# DBTITLE 1,md_fi_gl_account create statement
md_fi_gl_account_table_path = eh_md_folder_path + "/MD_FI_GL_ACCOUNT"
spark.sql(f"""CREATE TABLE `{uc_catalog}`.`{eh_md_schema}`.md_fi_gl_account (
  Language_Key STRING,
  Chart_of_Accounts STRING,
  GL_Account_No STRING,
  GL_Account_Long_Text STRING,
  ingested_at TIMESTAMP,
  Source_ID STRING,
  OPFLAG STRING)
USING delta
LOCATION '{md_fi_gl_account_table_path}'""")

# COMMAND ----------

# DBTITLE 1,md_fi_comp_code create statement
md_fi_comp_code_table_path = eh_md_folder_path + "/MD_FI_COMP_CODE"
spark.sql(f"""CREATE TABLE `{uc_catalog}`.`{eh_md_schema}`.md_fi_comp_code (
  Company_Code STRING,
  Currency_Key STRING,
  Name_Comp_Code STRING,
  ingested_at TIMESTAMP,
  Source_ID STRING,
  OPFLAG STRING)
USING delta
LOCATION '{md_fi_comp_code_table_path}'""")

# COMMAND ----------

# DBTITLE 1,md_mm_mov_type create statement
md_mm_mov_type_table_path = eh_md_folder_path + "/MD_MM_MOV_TYPE"
spark.sql(f"""CREATE TABLE `{uc_catalog}`.`{eh_md_schema}`.md_mm_mov_type (
  Mvmt_Type STRING,
  Mvmt_Type_Text STRING,
  ingested_at TIMESTAMP,
  Source_ID STRING,
  OPFLAG STRING)
USING delta
LOCATION '{md_mm_mov_type_table_path}'""")

# COMMAND ----------

# DBTITLE 1,md_sd_dist_channel create statement
md_sd_dist_channel_table_path = eh_md_folder_path + "/MD_SD_DIST_CHANNEL"
spark.sql(f"""CREATE TABLE `{uc_catalog}`.`{eh_md_schema}`.md_sd_dist_channel (
  Distribution_Channel STRING,
  Distribution_Channel_Text STRING,
  ingested_at TIMESTAMP,
  Source_ID STRING,
  OPFLAG STRING)
USING delta
LOCATION '{md_sd_dist_channel_table_path}'""")

# COMMAND ----------

# DBTITLE 1,md_fi_profit_center create statement
md_fi_profit_center_table_path = eh_md_folder_path + "/MD_FI_PROFIT_CENTER"
spark.sql(f"""CREATE TABLE `{uc_catalog}`.`{eh_md_schema}`.md_fi_profit_center (
  Profit_Center STRING,
  Profit_Center_Text STRING,
  ingested_at TIMESTAMP,
  Source_ID STRING,
  OPFLAG STRING)
USING delta
LOCATION '{md_fi_profit_center_table_path}'""")

# COMMAND ----------

# DBTITLE 1,fact_fi_act_line_item create statement
fact_fi_act_line_item_table_path = eh_fi_folder_path + "/FACT_FI_ACT_LINE_ITEM"
spark.sql(f"""CREATE TABLE `{uc_catalog}`.`{eh_fi_schema}`.fact_fi_act_line_item (
  Client STRING,
  Rec_No_Line_Itm_Rec STRING,
  Ledger STRING,
  Fiscal_Year STRING,
  Currency_Key STRING,
  Debit_Credit_Indicator STRING,
  Posting_Period STRING,
  Accounting_Document_No STRING,
  Document_Line STRING,
  Company_Code STRING,
  Profit_Center STRING,
  Functional_Area STRING,
  Account_No STRING,
  Partner_Profit_Center STRING,
  Amt_Comp_Code_Curr DECIMAL(15,2),
  Amt_PC_Local_Curr DECIMAL(15,2),
  Document_Date DATE,
  Document_Posting_Date DATE,
  Ref_Document_No STRING,
  Ref_Fiscal_Year STRING,
  Line_Itm_Account_Doc_No STRING,
  Ref_Document_Type STRING,
  Plant STRING,
  Material_Number STRING,
  Customer_Number STRING,
  Account_No_Supplier STRING,
  Purchasing_Document_No STRING,
  Sales_Order_Doc_No STRING,
  Distribution_Channel STRING,
  Division STRING,
  Mvmt_Type STRING,
  Document_Type STRING,
  Receiving_Issue_Plant STRING,
  Ext_Mode_Transport STRING,
  Weight_Qty DECIMAL(13,3),
  Volume_Unit_L15 STRING,
  Volume_Qty DECIMAL(13,3),
  Volume_GAL DECIMAL(13,3),
  Volume_BBL DECIMAL(13,3),
  No_Principal_Prchg_Agmt STRING,
  Doc_No_Ref_Doc STRING,
  Parcel_ID STRING,
  Smart_Contract_No STRING,
  OIL_TSW_Deal_Number STRING,
  Document_Date_In_Doc DATE,
  External_Bill_Of_Lading STRING,
  Endur_Cargo_ID STRING,
  BGI_Delivery_ID STRING,
  Endur_Parcel_ID STRING,
  Volume_Qty_GAL DECIMAL(13,3),
  Volume_Unit_GAL STRING,
  Volume_Qty_BB6 DECIMAL(13,3),
  Volume_Unit_BB6 STRING,
  CL_LT_Reference_Doc_No STRING,
  CL_RT_Reference_Doc_No STRING,
  Material_Document_Number STRING,
  Material_Document_Year STRING,
  Material_Document_Itm STRING,
  MM_Mvmt_Type STRING,
  Debit_Or_Credit_Indicator STRING,
  MM_Plant STRING,
  MM_Receiving_Plant STRING,
  MM_Material STRING,
  MM_Receiving_Material STRING,
  Add_Oil_Gas_Qty DOUBLE,
  MM_Volume_L15 DOUBLE,
  CL_Account_No_Functional_Area STRING,
  MM_Material_Key STRING,
  CL_Receiving_Issuing_Mat_Key STRING,
  CL_UOM_In_GAL STRING,
  CL_Lcl_Currency STRING,
  CL_Mvmt_Type_Plus STRING,
  CL_Profit_Center STRING,
  CL_Mvmt_Group STRING,
  CL_GL_Account_Key STRING,
  Material_Key STRING,
  ingested_at TIMESTAMP,
  Source_ID STRING,
  OPFLAG STRING)
USING delta
LOCATION '{fact_fi_act_line_item_table_path}'""")

# COMMAND ----------

# DBTITLE 1,Load sample data into EH tables
if env == "dev":
    # md_mm_zmaterial
    spark.sql(f"""INSERT INTO `{uc_catalog}`.`{eh_md_schema}`.md_mm_zmaterial VALUES 
                ('1104','','','GO ProBrakePC_12*19oz_A0II','',current_timestamp(),'D94_110','I'),
                ('100029041','','','DIFLLN CL150 PN6 CS PL PFA DN200','',current_timestamp(),'D94_110','I'),
                ('100029042','','','GLSLND MOFL CL2500 316 1/2in DN15','',current_timestamp(),'D94_110','I'),
                ('100029043','','','GAFLWF BLT CL600 304H 304H/ST6 DN250','',current_timestamp(),'D94_110','I'),
                ('100029044','','','GAFLWF BLT CL600 304H 304H/ST6 DN300','',current_timestamp(),'D94_110','I'),
                ('100029045','','','GLSLND MOFL CL2500 316 1/2in DN20','',current_timestamp(),'D94_110','I'),
                ('100029046','','','GAFLWF BLT CL900 304H 304H/ST6 DN150','',current_timestamp(),'D94_110','I'),
                ('100029047','','','TERB,WPL6,LT,80, DN450X250','',current_timestamp(),'D94_110','I'),
                ('100029048','','','GLSLND MOFL CL2500 316 1/2in DN25','',current_timestamp(),'D94_110','I'),
                ('100029049','','','GLSLND MOFL CL2500 316 1/2in DN40','',current_timestamp(),'D94_110','I')""")
    # md_mm_vendor
    spark.sql(f"""INSERT INTO `{uc_catalog}`.`{eh_md_schema}`.md_mm_vendor VALUES 
                ('68000042','PROMO DIREKT KFT.',current_timestamp(),'D94_110','I'),
                ('68000102','MÃV NOSZTALGIA KFT.',current_timestamp(),'D94_110','I'),
                ('68000116','REFLEX KFT.',current_timestamp(),'D94_110','I'),
                ('68000161','DÃ‰L-KANDELÃBER KFT.',current_timestamp(),'D94_110','I'),
                ('68000191','ALL LOCK VÃ‰DELEM KFT.',current_timestamp(),'D94_110','I'),
                ('68000200','FÃœRED HOLDING KFT.',current_timestamp(),'D94_110','I'),
                ('68000302','CAREPLAY BT.',current_timestamp(),'D94_110','I'),
                ('68000532','DR. ANGYAL GABRIELLA KÃ–ZJEGYZÅ',current_timestamp(),'D94_110','I'),
                ('68000554','SCHENKER KFT.',current_timestamp(),'D94_110','I'),
                ('68000561','ECONO-PRESS KFT.',current_timestamp(),'D94_110','I')""")
    # md_mm_plant
    spark.sql(f"""INSERT INTO `{uc_catalog}`.`{eh_md_schema}`.md_mm_plant VALUES 
                ('A003','AT1002 Linz depot','ATT01','ZSH','Shell stock at Shell location',current_timestamp(),'D94_110','I'),
                ('A004','AT1003 Salzburg TBG depot','ATT01','ZST','Shell stock 3rd party location',current_timestamp(),'D94_110','I'),
                ('A008','AT1011 Zirl ENI Depot','ATT01','ZST','Shell stock 3rd party location',current_timestamp(),'D94_110','I'),
                ('A010','AT1009 St.Valentin OMV depot','ATT01','ZST','Shell stock 3rd party location',current_timestamp(),'D94_110','I'),
                ('A012','AT3003 Shell/OMV Lustenau','ATT01','ZTP','non-Shell stock 3rd party location',current_timestamp(),'D94_110','I'),
                ('A013','AT3004 Shell/Agip Zirl','ATT01','ZTP','non-Shell stock 3rd party location',current_timestamp(),'D94_110','I'),
                ('A015','AT4000 AT01 ex 3rd party DE','ATT01','ZTP','non-Shell stock 3rd party location',current_timestamp(),'D94_110','I'),
                ('A019','AT4004 AT01 ex 3rd party HU','ATT01','ZTP','non-Shell stock 3rd party location',current_timestamp(),'D94_110','I'),
                ('A021','AT4006 AT01 ex 3rd party CZ','ATT01','ZTP','non-Shell stock 3rd party location',current_timestamp(),'D94_110','I'),
                ('A022','AT4007 AT01 ex 3rd party SI','ATT01','ZTP','non-Shell stock 3rd party location',current_timestamp(),'D94_110','I')""")
    # md_mm_material
    spark.sql(f"""INSERT INTO `{uc_catalog}`.`{eh_md_schema}`.md_mm_material VALUES 
                ('PZL Mar FSC_12*12oz_A0II','ZPDC207R','1110','2062','Auto Chemicals','207','Car Care','Car Care Chemicals',current_timestamp(),'D94_110','I'),
                ('Adhezolith EP 2_1*400lb_A0II','ZPDC204P','1307','2305','Other Greases','204','Greases (excluding Food)','Greases - Packed',current_timestamp(),'D94_110','I'),
                ('PennzbellAWXMVArcti_1*55ugl_A0II','ZPDC202P','1618','2005','Factory Plant Maintenance Lubricants','202','Industrial Lubricants','Industrial Lub. Pack',current_timestamp(),'D94_110','I'),
                ('Pzl Synchromesh Fld_12*1qt_A0II','ZPDC201P','3501','2002','Transmission Oils (Automotive)','201','Automotive Lubricants','Automotive Lub Pack',current_timestamp(),'D94_110','I'),
                ('Pzl Motor Oil 20W-50_1*55ugl_A0II','ZPDC201P','3566','2001','Engine Oils (automotive)','201','Automotive Lubricants','Automotive Lub Pack',current_timestamp(),'D94_110','I'),
                ('Pzl Motor Oil 20W-50_12*1qt_A0II','ZPDC201P','3569','2001','Engine Oils (automotive)','201','Automotive Lubricants','Automotive Lub Pack',current_timestamp(),'D94_110','I'),
                ('Pzl GT Perf Rac 60_BULK_A1SJ','ZPDC201B','3590','2001','Engine Oils (automotive)','201','Automotive Lubricants','Automotive Lub Bulk',current_timestamp(),'D94_110','I'),
                ('Pzl Long Life 10WCF_BULK_A1SJ','ZPDC201B','3720','2001','Engine Oils (automotive)','201','Automotive Lubricants','Automotive Lub Bulk',current_timestamp(),'D94_110','I'),
                ('PzlLlife30CF4CF2CFSJ_1*5ugl_A0II','ZPDC201P','3744','2001','Engine Oils (automotive)','201','Automotive Lubricants','Automotive Lub Pack',current_timestamp(),'D94_110','I'),
                ('PzlLongLife40CF42_1*55ugl_A0II','ZPDC201P','3756','2001','Engine Oils (automotive)','201','Automotive Lubricants','Automotive Lub Pack',current_timestamp(),'D94_110','I')""")
    # md_fi_gl_account
    spark.sql(f"""INSERT INTO `{uc_catalog}`.`{eh_md_schema}`.md_fi_gl_account VALUES 
                ('E','OP01','1850061','NG MtM Assets Physical contracts > 1 Year DIE',current_timestamp(),'D94_110','I'),
                ('E','OP01','1850300','Loans to JVA > 1 year',current_timestamp(),'D94_110','I'),
                ('E','OP01','1850350','Loans to JVA > 1 year DIE',current_timestamp(),'D94_110','I'),
                ('E','OP01','2001010','Inventory - Crude Adjustment',current_timestamp(),'D94_110','I'),
                ('E','OP01','2006010','Inventory - Base Oils',current_timestamp(),'D94_110','I'),
                ('E','OP01','2007000','Inventory - Other',current_timestamp(),'D94_110','I'),
                ('E','OP01','2510300','Loans to JVA < 1 year',current_timestamp(),'D94_110','I'),
                ('E','OP01','2685200','Loans to Dealers',current_timestamp(),'D94_110','I'),
                ('E','OP01','2780550','Margin Account Financial Trades',current_timestamp(),'D94_110','I'),
                ('E','OP01','2810010','ST Securities Equity - DIE',current_timestamp(),'D94_110','I')""")
    # md_sd_customer
    spark.sql(f"""INSERT INTO `{uc_catalog}`.`{eh_md_schema}`.md_sd_customer VALUES 
                ('10000063','SHELL 328 PASIR PANJANG 646_01',current_timestamp(),'D94_110','I'),
                ('10000279','164 NOROTON AVENUE',current_timestamp(),'D94_110','I'),
                ('10000409','10960 BUSTLETON AVENUE',current_timestamp(),'D94_110','I'),
                ('10000470','88 N MOUNT JULIET RD',current_timestamp(),'D94_110','I'),
                ('10000487','1445 POWERS FERRY RD SE',current_timestamp(),'D94_110','I'),
                ('10000663','7988 GLADES RD',current_timestamp(),'D94_110','I'),
                ('10000709','6203 OLD WINTER GARDEN RD',current_timestamp(),'D94_110','I'),
                ('10000768','1801 S TAMIAMI TRL',current_timestamp(),'D94_110','I'),
                ('10000889','6141 SOUTH LOOP E',current_timestamp(),'D94_110','I'),
                ('10000965','5815 SOUTH PAN AM EXP',current_timestamp(),'D94_110','I')""")
    # md_fi_comp_code
    spark.sql(f"""INSERT INTO `{uc_catalog}`.`{eh_md_schema}`.md_fi_comp_code VALUES 
                ('1','EUR','SAP A.G.',current_timestamp(),'D94_110','I'),
                ('0MB1','EUR','IS-B Musterbank Deutschl.',current_timestamp(),'D94_110','I'),
                ('AR01','ARS','Country Template AR',current_timestamp(),'D94_110','I'),
                ('AR07','ARS','Lubricantes Pennzoil Arge',current_timestamp(),'D94_110','I'),
                ('ARG1','ARS','Country Template AR',current_timestamp(),'D94_110','I'),
                ('ARZ1','ARS','Raizen Argentina S.A.',current_timestamp(),'D94_110','I'),
                ('AT01','EUR','Shell Austria Gesellschaf',current_timestamp(),'D94_110','I'),
                ('AT02','EUR','Shell  Direct  Austria  G',current_timestamp(),'D94_110','I'),
                ('AT05','EUR','Shell China Holding GMBH',current_timestamp(),'D94_110','I'),
                ('AT06','EUR','Shell Middle East Holding',current_timestamp(),'D94_110','I')""")
    # md_mm_mov_type
    spark.sql(f"""INSERT INTO `{uc_catalog}`.`{eh_md_schema}`.md_mm_mov_type VALUES 
                ('101','WE Wareneingang     ',current_timestamp(),'D94_110','I'),
                ('103','WE in  Sperrbestand ',current_timestamp(),'D94_110','I'),
                ('104','WE in Sperrb Storno ',current_timestamp(),'D94_110','I'),
                ('105','WE aus Sperrbestand ',current_timestamp(),'D94_110','I'),
                ('106','WE aus Sperrb Storno',current_timestamp(),'D94_110','I'),
                ('121','WE Nachverrechnung  ',current_timestamp(),'D94_110','I'),
                ('124','WE RÃƒÂ¼ckl. Sperrb.   ',current_timestamp(),'D94_110','I'),
                ('125','WE RÃƒÂ¼ckl. Sperrb. St',current_timestamp(),'D94_110','I'),
                ('131','Wareneingang        ',current_timestamp(),'D94_110','I'),
                ('132','Wareneingang        ',current_timestamp(),'D94_110','I')""")
    # md_sd_dist_channel
    spark.sql(f"""INSERT INTO `{uc_catalog}`.`{eh_md_schema}`.md_sd_dist_channel VALUES 
                ('1','Distribtn Channel 01',current_timestamp(),'D94_110','I'),
                ('1','Distribtn Channel 01',current_timestamp(),'D94_110','I'),
                ('1','Distribtn Channel 01',current_timestamp(),'D94_110','I'),
                ('1','Distribtn Channel 01',current_timestamp(),'D94_110','I'),
                ('1','Vertriebsweg 01     ',current_timestamp(),'D94_110','I'),
                ('1','Distribtn Channel 01',current_timestamp(),'D94_110','I'),
                ('1','Distribtn Channel 01',current_timestamp(),'D94_110','I'),
                ('1','Distribtn Channel 01',current_timestamp(),'D94_110','I'),
                ('1','Distribtn Channel 01',current_timestamp(),'D94_110','I'),
                ('1','Distribtn Channel 01',current_timestamp(),'D94_110','I')""")
    # md_fi_profit_center
    spark.sql(f"""INSERT INTO `{uc_catalog}`.`{eh_md_schema}`.md_fi_profit_center VALUES 
                ('HU01-DUMMY','',current_timestamp(),'D94_110','I'),
                ('RECO-DUMMY','Dummy-PC fÃ¼r RECO',current_timestamp(),'D94_110','I'),
                ('SAP-DUMMY','',current_timestamp(),'D94_110','I'),
                ('SAP-DUMMY','',current_timestamp(),'D94_110','I'),
                ('SAP-DUMMY','',current_timestamp(),'D94_110','I'),
                ('SAP-DUMMY','',current_timestamp(),'D94_110','I'),
                ('SAP-DUMMY','',current_timestamp(),'D94_110','I'),
                ('SAP-DUMMY','',current_timestamp(),'D94_110','I'),
                ('SAP-DUMMY','',current_timestamp(),'D94_110','I'),
                ('SAP-DUMMY','',current_timestamp(),'D94_110','I')""")
    # fact_fi_act_line_item
    spark.sql(f"""INSERT INTO `{uc_catalog}`.`{eh_fi_schema}`.fact_fi_act_line_item VALUES
            ('110','000000021938829251','8A','2022','USD','H','002','1955053227','001','SG01','0000310001','SV31','0006380303','','-17174.36','-17174.36','44598','44598','4001575191','2022','001','W','S803','000000000400000851','','','','','','','','HC','','','-21082.661','L15','-26353.326','-26353.326','-26353.326','','','','','',null,'','','','','-7007.479','UG6','-166.845','BB6','3223099366','0005','3223099366','2022','0005','909','H','S803','S803','000000000400000851','000000000400000449','26539','-26539','0006380303SV31','400000851','400000449','GAL','Local Currency','','310001','','6380303','400000851',current_timestamp(),'D94_110','I'),
            ('110','000000021938829252','8A','2022','USD','S','002','1955053227','002','SG01','0000310001','SV30','0006380203','','17174.36','17174.36','44598','44598','4001575191','2022','002','W','S803','000000000400000851','','','','','','','','HC','','','21082.661','L15','26353.326','26353.326','26353.326','','','','','',null,'','','','','7007.479','UG6','166.845','BB6','3223099366','0005','3223099366','2022','0005','909','H','S803','S803','000000000400000449','000000000400000851','26539','26539','0006380203SV30','400000449','400000851','GAL','Local Currency','','310001','','6380203','400000851',current_timestamp(),'D94_110','I'),
            ('110','000000021938837414','8A','2022','USD','S','002','1955055446','002','SG01','0000310001','SV30','0006380203','','9894.33','9894.33','44598','44598','4001575321','2022','002','W','S803','000000000400010234','','','','','','','','HC','','','10878.39','L15','14456.333','14456.333','14456.333','','','','','',null,'','','','','3821.989','UG6','91','BB6','3223156830','0005','3223156830','2022','0005','909','H','S803','S803','000000000400009721','000000000400010234','14675','14675','0006380203SV30','400009721','400010234','GAL','Local Currency','','310001','','6380203','400010234',current_timestamp(),'D94_110','I'),
            ('110','000000021938837413','8A','2022','USD','H','002','1955055446','001','SG01','0000310001','SV31','0006380303','','-9894.33','-9894.33','44598','44598','4001575321','2022','001','W','S803','000000000400010234','','','','','','','','HC','','','-10878.39','L15','-14456.333','-14456.333','-14456.333','','','','','',null,'','','','','-3821.989','UG6','-91','BB6','3223156830','0005','3223156830','2022','0005','909','H','S803','S803','000000000400010234','000000000400009721','14675','-14675','0006380303SV31','400010234','400009721','GAL','Local Currency','','310001','','6380303','400010234',current_timestamp(),'D94_110','I'),
            ('110','000000021938941747','8A','2022','CAD','S','002','1955083654','002','CA48','0000310001','SV30','0006380203','','5175.09','4075.85','44598','44598','4013258239','2022','002','W','C316','000000000400004728','','','','','','','','HC','','','2373.033','L15','2823.909','2823.909','2823.909','','','','','',null,'','','','','746.64','UG6','17.777','BB6','3223157256','0003','3223157256','2022','0003','909','H','C316','C316','000000000400004800','000000000400004728','2816','2816','0006380203SV30','400004800','400004728','GAL','Local Currency','','310001','','6380203','400004728',current_timestamp(),'D94_110','I'),
            ('110','000000021938941746','8A','2022','CAD','H','002','1955083654','001','CA48','0000310001','SV31','0006380303','','-5175.09','-4075.85','44598','44598','4013258239','2022','001','W','C316','000000000400004728','','','','','','','','HC','','','-2373.033','L15','-2823.909','-2823.909','-2823.909','','','','','',null,'','','','','-746.64','UG6','-17.777','BB6','3223157256','0003','3223157256','2022','0003','909','H','C316','C316','000000000400004728','000000000400004800','2816','-2816','0006380303SV31','400004728','400004800','GAL','Local Currency','','310001','','6380303','400004728',current_timestamp(),'D94_110','I'),
            ('110','000000021938927176','8A','2022','CAD','H','002','1955078804','001','CA48','0000310001','SV31','0006380303','','-12343.3','-9721.46','44598','44598','4013257875','2022','001','W','C316','000000000400004728','','','','','','','','HC','','','-9415.035','L15','-11203.892','-11203.892','-11203.892','','','','','',null,'','','','','-2962.302','UG6','-70.531','BB6','3223158064','0001','3223158064','2022','0001','909','H','C316','C316','000000000400004728','000000000400004707','11363','-11363','0006380303SV31','400004728','400004707','GAL','Local Currency','','310001','','6380303','400004728',current_timestamp(),'D94_110','I'),
            ('110','000000021938927177','8A','2022','CAD','S','002','1955078804','002','CA48','0000310001','SV30','0006380203','','12343.3','9721.46','44598','44598','4013257875','2022','002','W','C316','000000000400004728','','','','','','','','HC','','','9415.035','L15','11203.892','11203.892','11203.892','','','','','',null,'','','','','2962.302','UG6','70.531','BB6','3223158064','0001','3223158064','2022','0001','909','H','C316','C316','000000000400004707','000000000400004728','11363','11363','0006380203SV30','400004707','400004728','GAL','Local Currency','','310001','','6380203','400004728',current_timestamp(),'D94_110','I'),
            ('110','000000021938887721','8A','2022','CAD','H','002','1955068994','001','CA48','0000310001','SV31','0006380303','','-7646.43','-6022.25','44598','44598','4013257133','2022','001','W','C169','000000000400004673','','','','','','','','HC','','','-5254.445','L15','-7004.175','-7004.175','-7004.175','','','','','',null,'','','','','-1851.992','UG6','-44.095','BB6','3223158073','0007','3223158073','2022','0007','909','H','C169','C169','000000000400004673','000000000400004670','7179.847000000001','-7179.847','0006380303SV31','400004673','400004670','GAL','Local Currency','','310001','','6380303','400004673',current_timestamp(),'D94_110','I'),
            ('110','000000021938887722','8A','2022','CAD','S','002','1955068994','002','CA48','0000310001','SV30','0006380203','','7646.43','6022.25','44598','44598','4013257133','2022','002','W','C169','000000000400004673','','','','','','','','HC','','','5254.445','L15','7004.175','7004.175','7004.175','','','','','',null,'','','','','1851.992','UG6','44.095','BB6','3223158073','0007','3223158073','2022','0007','909','H','C169','C169','000000000400004670','000000000400004673','7179.847000000001','7179.847000000001','0006380203SV30','400004670','400004673','GAL','Local Currency','','310001','','6380203','400004673',current_timestamp(),'D94_110','I')""")
