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
mars_ddo_security_function_name = dbutils.widgets.get("mars_ddo_security_function_name") # sf_mars_data_dump_output_report
mars_mbc_security_function_name = dbutils.widgets.get("mars_mbc_security_function_name") # sf_mars_margin_bucket_collection_report
mars_ddo_view_name = dbutils.widgets.get(name="mars_ddo_view_name") # vw_use_case_mars_data_dump_output
mars_mbc_view_name = dbutils.widgets.get(name="mars_mbc_view_name") # vw_use_case_mars_margin_bucket_collection
mars_grade_and_ed_check_security_function_name = dbutils.widgets.get("mars_grade_and_ed_check_security_function_name") # sf_mars_grade_and_ed_check_report
mars_grade_and_ed_check_account_security_function_name = dbutils.widgets.get("mars_grade_and_ed_check_account_security_function_name") # sf_mars_grade_and_ed_check_account_report
mars_trend_material_group_security_function_name = dbutils.widgets.get("mars_trend_material_group_security_function_name") # sf_mars_trend_material_group_report
mars_trend_li_bucket_security_function_name = dbutils.widgets.get("mars_trend_li_bucket_security_function_name") # sf_mars_trend_li_bucket_report
mars_country_wise_security_function_name = dbutils.widgets.get("mars_country_wise_security_function_name") # sf_mars_country_wise_report
mars_plant_and_material_security_function_name = dbutils.widgets.get("mars_plant_and_material_security_function_name") # sf_mars_plant_and_material_report
mars_mass_balance_check_source_security_function_name = dbutils.widgets.get("mars_mass_balance_check_source_security_function_name") # sf_mars_mass_balance_check_source_report
mars_mass_balance_check_account_security_function_name = dbutils.widgets.get("mars_mass_balance_check_account_security_function_name") # sf_mars_mass_balance_check_account_report
mars_detail_overview_security_function_name = dbutils.widgets.get("mars_detail_overview_security_function_name") # sf_mars_detail_overview_report
mars_stocks_security_function_name = dbutils.widgets.get("mars_stocks_security_function_name") # sf_mars_stocks_report
mars_margin_explanation_security_function_name = dbutils.widgets.get("mars_margin_explanation_security_function_name") # sf_mars_margin_explanation_report
mars_composite_nibiat_security_function_name = dbutils.widgets.get("mars_composite_nibiat_security_function_name") # sf_mars_composite_nibiat_report

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

# assignFunctionPermission: This function assigns Permission to the function created
def assignFunctionPermission(catalog,schema,function_name,tbl_owner,security_end_user_aad_grp, security_object_aad_group_name, security_functional_dev_aad_group, security_functional_readers_aad_group):
    spark.sql(f"""GRANT ALL PRIVILEGES ON FUNCTION `{catalog}`.`{schema}`.{function_name} TO `{tbl_owner}`""")
    print("All privileges access given to tbl owner ", tbl_owner)
    spark.sql(f"""GRANT ALL PRIVILEGES ON FUNCTION `{catalog}`.`{schema}`.{function_name} TO `{security_functional_dev_aad_group}`""")
    print("All privileges access given to tbl owner ", security_functional_dev_aad_group)
    spark.sql(f"""GRANT EXECUTE ON FUNCTION `{catalog}`.`{schema}`.{function_name} TO `{security_end_user_aad_grp}`""")
    print("Execute priviledge access given to ", security_end_user_aad_grp)
    spark.sql(f"""GRANT EXECUTE ON FUNCTION `{catalog}`.`{schema}`.{function_name} TO `{security_functional_readers_aad_group}`""")
    print("Execute priviledge access given to ", security_functional_readers_aad_group)
    spark.sql(f"""ALTER function `{catalog}`.`{schema}`.{function_name} owner to `{security_object_aad_group_name}`""")
    print("Table Owner is assigned to ", security_object_aad_group_name)

# COMMAND ----------

use_catalog = "USE CATALOG `"+uc_catalog_name+"`"
spark.sql(use_catalog)

# COMMAND ----------

# DBTITLE 1,MARS DDO - SQL function
spark.sql(f"""DROP FUNCTION IF EXISTS `{uc_catalog_name}`.`{uc_curated_schema}`.{mars_ddo_security_function_name}""")
spark.sql(f"""
CREATE OR REPLACE FUNCTION `{uc_catalog_name}`.`{uc_curated_schema}`.{mars_ddo_security_function_name} (
    Source STRING, 
    `Company_Code-Text` STRING, 
    `Account-Text` STRING, 
    `Plant-Text` STRING, 
    Material_Group STRING,  
    LI_bucket_1 STRING,
    LI_bucket_2 STRING, 
    Year STRING, 
    Month STRING
) 
RETURNS TABLE (
    Source STRING, 
    `Company_Code-Text` STRING, 
    `Profit_Center-Text` STRING, 
    `Year` STRING, 
    `Month` STRING,  
    `Contribution Level` STRING, 
    `Account` STRING, 
    `Account Name` STRING, 
    `LI bucket 1` STRING,
    `LI bucket 2` STRING, 
    `FCB Plant` STRING,  
    `FCB Plant Name` STRING, 
    `Partner Plant` STRING,  
    `Partner Plant Name` STRING, 
    `FCB Material` STRING, 
    `Material Name`  STRING, 
    `Material Group` STRING, 
    `Bio group FCB Material` STRING, 
    `Partner Material` STRING, 
    `Partner Material Name` STRING, 
    `Partner Material Group` STRING,
    `Bio group Partner Material` STRING, 
    `Functional Area` STRING, 
    `Channel` STRING, 
    `Customer Vendor Number` STRING, 
    `Customer Vendor Name` STRING, 
    `Movement Type` STRING, 
    `Movement Type Group` STRING, 
    `Quantity KG` double, 
    `Volume L15` decimal(33,3), 
    `Amount FiFo LC` double, 
    `Amount CCS LC` decimal(37,2)
) 
RETURN  
    with temp as (
    select 
        Source, 
        `Company_Code-Text`, 
        `Profit_Center-Text`, 
        `Year`, 
        `Month`, 
        Contribution_Level AS `Contribution Level`, 
        Account, 
        Account_Name AS `Account Name`, 
        LI_bucket_1 AS `LI bucket 1`, 
        LI_bucket_2 AS `LI bucket 2`, 
        FCB_Plant AS `FCB Plant`, 
        FCB_Plant_Name AS `FCB Plant Name`, 
        Partner_Plant AS `Partner Plant`, 
        Partner_Plant_Name AS `Partner Plant Name`, 
        FCB_Material AS `FCB Material`, 
        Material_Name AS `Material Name`, 
        Material_Group AS `Material Group`, 
        Bio_group_FCB_Material AS `Bio group FCB Material`, 
        Partner_Material AS `Partner Material`, 
        Partner_Material_Name AS `Partner Material Name`, 
        Partner_Material_Group AS `Partner Material Group`, 
        Bio_group_Partner_Material AS `Bio group Partner Material`, 
        Functional_Area AS `Functional Area`, 
        `Channel`, 
        Customer_Vendor_Number AS `Customer Vendor Number`, 
        Customer_Vendor_Name AS `Customer Vendor Name`, 
        Movement_Type AS `Movement Type`, 
        Movement_Type_Group AS `Movement Type Group`, 
        Quantity_KG AS `Quantity KG`, 
        Volume_L15 AS `Volume L15`, 
        Amount_FiFo_LC AS `Amount FiFo LC`, 
        Amount_CCS_LC AS `Amount CCS LC`
    from `{uc_catalog_name}`.`{uc_curated_schema}`.{mars_ddo_view_name} ddo
    WHERE
    (array_contains(SPLIT({mars_ddo_security_function_name}.`Source`,',') ,ddo.Source) OR ({mars_ddo_security_function_name}.Source='ALL')) AND
    (array_contains(SPLIT({mars_ddo_security_function_name}.`Company_Code-Text`,',') ,ddo.`Company_Code-Text`) OR ({mars_ddo_security_function_name}.`Company_Code-Text`='ALL')) AND
    (array_contains(SPLIT({mars_ddo_security_function_name}.`Account-Text`,',') ,ddo.`Account-Text`) OR ({mars_ddo_security_function_name}.`Account-Text`='ALL')) AND
    (array_contains(SPLIT({mars_ddo_security_function_name}.`Plant-Text`,',') ,ddo.`Plant-Text`) OR ({mars_ddo_security_function_name}.`Plant-Text`='ALL')) AND
    (array_contains(SPLIT({mars_ddo_security_function_name}.Material_Group,',') ,ddo.`Material_Group`) OR ({mars_ddo_security_function_name}.Material_Group='ALL')) AND
    (array_contains(SPLIT({mars_ddo_security_function_name}.LI_bucket_1,',') ,ddo.`LI_bucket_1`) OR ({mars_ddo_security_function_name}.LI_bucket_1='ALL')) AND
    (array_contains(SPLIT({mars_ddo_security_function_name}.LI_bucket_2,',') ,ddo.`LI_bucket_2`) OR ({mars_ddo_security_function_name}.LI_bucket_2='ALL')) AND
    (array_contains(SPLIT({mars_ddo_security_function_name}.Year,',') ,ddo.`Year`) OR ({mars_ddo_security_function_name}.Year='ALL')) AND
    (array_contains(SPLIT({mars_ddo_security_function_name}.Month,',') ,ddo.`Month`) OR ({mars_ddo_security_function_name}.Month='ALL')) AND
    ddo.OPFLAG != 'D'
)
SELECT 
    Source, 
    `Company_Code-Text`, 
    `Profit_Center-Text`,
    `Year`, 
    `Month`, 
    `Contribution Level`, 
    `Account`, 
    `Account Name`, 
    `LI bucket 1`, 
    `LI bucket 2`,
    `FCB Plant`, 
    `FCB Plant Name`, 
    `Partner Plant`, 
    `Partner Plant Name`, 
    `FCB Material`, 
    `Material Name`, 
    `Material Group`, 
    `Bio group FCB Material`, 
    `Partner Material`, 
    `Partner Material Name`, 
    `Partner Material Group`, 
    `Bio group Partner Material`, 
    `Functional Area`, 
    `Channel`,  
    `Customer Vendor Number`, 
    `Customer Vendor Name`, 
    `Movement Type`, 
    `Movement Type Group`, 
    `Quantity KG`, 
    `Volume L15`, 
    `Amount FiFo LC`, 
    `Amount CCS LC` 
FROM temp
""")

assignFunctionPermission(
    uc_catalog_name, 
    uc_curated_schema, 
    mars_ddo_security_function_name, 
    tbl_owner_grp,
    security_end_user_aad_group_name, 
    security_object_aad_group_name, 
    security_functional_dev_aad_group, 
    security_functional_readers_aad_group
)

print("mars data dump output function dropped and recreated in the schema")

# COMMAND ----------

# DBTITLE 1,MARS MBC - SQL Function
spark.sql(f"""DROP FUNCTION IF EXISTS `{uc_catalog_name}`.`{uc_curated_schema}`.mars_mbc_security_function_name""")
spark.sql(f"""
CREATE OR REPLACE FUNCTION `{uc_catalog_name}`.`{uc_curated_schema}`.`{mars_mbc_security_function_name}`(Margin_Bucket STRING,`Company_Code-Text` STRING,`Plant-Text` STRING, Material_Group_Finance STRING, FCB_Material STRING, Bio_Group STRING, Year STRING, Month STRING) 
RETURNS TABLE (`Margin Bucket` STRING,`Company_Code-Text` STRING,`Plant-Text` STRING, `Plant Name` STRING, `FCB Material` STRING, `Material Name` STRING, `Material Group Finance` STRING, `Bio Group` STRING, `SumOfQuantity` double, `SumOfC1 Fifo` double, `Year` STRING, `Month` STRING) 

RETURN  

    with temp as (
    select Margin_Bucket AS `Margin Bucket`, `Company_Code-Text` , `Plant-Text` , Plant_Name AS `Plant Name`, FCB_Material AS `FCB Material`, Material_Name AS `Material Name`, Material_Group_Finance AS `Material Group Finance`, Bio_Group AS `Bio Group`, SumOfQuantity, SumOfC1_Fifo AS `SumOfC1 Fifo`, Year AS `Year`, Month AS `Month`
    from `{uc_catalog_name}`.`{uc_curated_schema}`.{mars_mbc_view_name} mbc
    WHERE
    (array_contains(SPLIT({mars_mbc_security_function_name}.`Company_Code-Text`,',') ,mbc.`Company_Code-Text`) OR ({mars_mbc_security_function_name}.`Company_Code-Text`='ALL')) AND
    (array_contains(SPLIT({mars_mbc_security_function_name}.`Plant-Text`,',') ,mbc.`Plant-Text`) OR ({mars_mbc_security_function_name}.`Plant-Text`='ALL')) AND
    (array_contains(SPLIT({mars_mbc_security_function_name}.Margin_Bucket,',') ,mbc.Margin_Bucket) OR ({mars_mbc_security_function_name}.Margin_Bucket='ALL')) AND
    (array_contains(SPLIT({mars_mbc_security_function_name}.Material_Group_Finance,',') ,mbc.Material_Group_Finance) OR ({mars_mbc_security_function_name}.Material_Group_Finance='ALL')) AND
    (array_contains(SPLIT({mars_mbc_security_function_name}.FCB_Material,',') ,mbc.FCB_Material) OR ({mars_mbc_security_function_name}.FCB_Material='ALL')) AND
    (array_contains(SPLIT({mars_mbc_security_function_name}.Bio_Group,',') ,mbc.Bio_Group) OR ({mars_mbc_security_function_name}.Bio_Group='ALL'))AND
    (array_contains(SPLIT({mars_mbc_security_function_name}.Year,',') ,mbc.Year) OR ({mars_mbc_security_function_name}.Year='ALL')) AND
    (array_contains(SPLIT({mars_mbc_security_function_name}.Month,',') ,mbc.Month) OR ({mars_mbc_security_function_name}.Month='ALL')) AND
    mbc.OPFLAG != 'D')

SELECT `Margin Bucket`, `Company_Code-Text` , `Plant-Text` , `Plant Name`, `FCB Material`, `Material Name`, `Material Group Finance`, `Bio Group`, SumOfQuantity, `SumOfC1 Fifo`, `Year`, `Month`
FROM temp""")

assignFunctionPermission(uc_catalog_name, uc_curated_schema, mars_mbc_security_function_name, tbl_owner_grp,security_end_user_aad_group_name, security_object_aad_group_name, security_functional_dev_aad_group, security_functional_readers_aad_group)

print("mars margin bucket collection security function dropped and recreated in the schema")

# COMMAND ----------

# DBTITLE 1,MARS Grade and ED Check - SQL Function
spark.sql(f"""DROP FUNCTION IF EXISTS `{uc_catalog_name}`.`{uc_curated_schema}`.{mars_grade_and_ed_check_security_function_name}""")
spark.sql(f"""
CREATE OR REPLACE FUNCTION `{uc_catalog_name}`.`{uc_curated_schema}`.{mars_grade_and_ed_check_security_function_name}(Channel STRING, LI_bucket_2 STRING, Year STRING, Month STRING) 
RETURNS TABLE (Channel STRING,`LI bucket 2` STRING,`Company_Code-Text` STRING,`Material Group` STRING, `Sum of Amount FiFo_LC` double) 

RETURN  

    with temp as (
    select Channel, LI_bucket_2 AS `LI bucket 2` , `Company_Code-Text`, Material_Group AS `Material Group`, Amount_FiFo_LC AS `Amount FiFo LC`
    from `{uc_catalog_name}`.`{uc_curated_schema}`.{mars_ddo_view_name} ddo
    WHERE
    (array_contains(SPLIT({mars_grade_and_ed_check_security_function_name}.Channel,',') ,ddo.Channel) OR ({mars_grade_and_ed_check_security_function_name}.Channel='ALL')) AND
    (array_contains(SPLIT({mars_grade_and_ed_check_security_function_name}.LI_bucket_2,',') ,ddo.LI_bucket_2) OR ({mars_grade_and_ed_check_security_function_name}.LI_bucket_2='ALL')) AND
    (array_contains(SPLIT({mars_grade_and_ed_check_security_function_name}.Year,',') ,ddo.`Year`) OR ({mars_grade_and_ed_check_security_function_name}.Year='ALL')) AND
    (array_contains(SPLIT({mars_grade_and_ed_check_security_function_name}.Month,',') ,ddo.`Month`) OR ({mars_grade_and_ed_check_security_function_name}.Month='ALL')) AND
    ddo.OPFLAG != 'D'
    )

    SELECT Channel, `LI bucket 2`, `Company_Code-Text`, `Material Group`, sum(`Amount FiFo LC`) AS `Sum of Amount FiFo_LC` FROM temp GROUP BY Channel, `LI bucket 2`, `Company_Code-Text`, `Material Group`""")

assignFunctionPermission(uc_catalog_name, uc_curated_schema, mars_grade_and_ed_check_security_function_name, tbl_owner_grp,security_end_user_aad_group_name, security_object_aad_group_name, security_functional_dev_aad_group, security_functional_readers_aad_group)

print("mars grade and ed check security function dropped and recreated in the schema")

# COMMAND ----------

# DBTITLE 1,MARS Grade and ED Check Account - SQL Function
spark.sql(f"""DROP FUNCTION IF EXISTS `{uc_catalog_name}`.`{uc_curated_schema}`.{mars_grade_and_ed_check_account_security_function_name}""")
spark.sql(f"""
CREATE OR REPLACE FUNCTION `{uc_catalog_name}`.`{uc_curated_schema}`.{mars_grade_and_ed_check_account_security_function_name}(Channel STRING, LI_bucket_1 STRING, Material_Group STRING,Year STRING, Month STRING)
RETURNS TABLE (Channel STRING,`LI bucket 1` STRING,`LI bucket 2` STRING,`Company_Code-Text` STRING, `Material Group` STRING, Account STRING, `Sum of Amount FiFo_LC` double)

RETURN  

    with temp as (
    select Channel, LI_bucket_1 AS `LI bucket 1`, LI_bucket_2 AS `LI bucket 2` , `Company_Code-Text`, Material_Group AS `Material Group`, Account, Amount_FiFo_LC AS `Amount FiFo LC`
    from `{uc_catalog_name}`.`{uc_curated_schema}`.{mars_ddo_view_name} ddo
    WHERE
    (array_contains(SPLIT({mars_grade_and_ed_check_account_security_function_name}.Channel,',') ,ddo.Channel) OR ({mars_grade_and_ed_check_account_security_function_name}.Channel='ALL')) AND
    (array_contains(SPLIT({mars_grade_and_ed_check_account_security_function_name}.LI_bucket_1,',') ,ddo.LI_bucket_1) OR ({mars_grade_and_ed_check_account_security_function_name}.LI_bucket_1='ALL')) AND
    (array_contains(SPLIT({mars_grade_and_ed_check_account_security_function_name}.Material_Group,',') ,ddo.Material_Group) OR ({mars_grade_and_ed_check_account_security_function_name}.Material_Group='ALL')) AND
    (array_contains(SPLIT({mars_grade_and_ed_check_account_security_function_name}.Year,',') ,ddo.`Year`) OR ({mars_grade_and_ed_check_account_security_function_name}.Year='ALL')) AND
    (array_contains(SPLIT({mars_grade_and_ed_check_account_security_function_name}.Month,',') ,ddo.`Month`) OR ({mars_grade_and_ed_check_account_security_function_name}.Month='ALL')) AND
    ddo.OPFLAG != 'D'
    )

    SELECT Channel, `LI bucket 1`, `LI bucket 2`, `Company_Code-Text`, `Material Group`, Account, sum(`Amount FiFo LC`) AS `Sum of Amount FiFo_LC` FROM temp GROUP BY Channel, `LI bucket 1`, `LI bucket 2`, `Company_Code-Text`, `Material Group`, Account""")

assignFunctionPermission(uc_catalog_name, uc_curated_schema, mars_grade_and_ed_check_account_security_function_name, tbl_owner_grp,security_end_user_aad_group_name, security_object_aad_group_name, security_functional_dev_aad_group, security_functional_readers_aad_group)

print("mars grade and ed check account security function dropped and recreated in the schema")

# COMMAND ----------

# DBTITLE 1,MARS Trend Material Group - SQL Function
spark.sql(f"""DROP FUNCTION IF EXISTS `{uc_catalog_name}`.`{uc_curated_schema}`.{mars_trend_material_group_security_function_name}""")
spark.sql(f"""
CREATE OR REPLACE FUNCTION `{uc_catalog_name}`.`{uc_curated_schema}`.{mars_trend_material_group_security_function_name}(Channel STRING, LI_bucket_1 STRING, LI_bucket_2 STRING, `Company_Code-Text` STRING, Year STRING, Month STRING)
RETURNS TABLE (Channel STRING,`LI bucket 1` STRING,`LI bucket 2` STRING,`Company_Code-Text` STRING, `Material Group` STRING, `Year` STRING, `Month` STRING, `Sum of Amount FiFo_LC` double) 

RETURN  

    with temp as (
    select Channel, LI_bucket_1 AS `LI bucket 1`, LI_bucket_2 AS `LI bucket 2`, `Company_Code-Text`, Material_Group AS `Material Group`, `Year` AS Year, `Month` AS Month, Amount_FiFo_LC AS `Amount FiFo LC`
    from `{uc_catalog_name}`.`{uc_curated_schema}`.{mars_ddo_view_name} ddo
    WHERE
    (array_contains(SPLIT({mars_trend_material_group_security_function_name}.Channel,',') ,ddo.Channel) OR ({mars_trend_material_group_security_function_name}.Channel='ALL')) AND
    (array_contains(SPLIT({mars_trend_material_group_security_function_name}.LI_bucket_1,',') ,ddo.LI_bucket_1) OR ({mars_trend_material_group_security_function_name}.LI_bucket_1='ALL')) AND
    (array_contains(SPLIT({mars_trend_material_group_security_function_name}.LI_bucket_2,',') ,ddo.LI_bucket_2) OR ({mars_trend_material_group_security_function_name}.LI_bucket_2='ALL')) AND
    (array_contains(SPLIT({mars_trend_material_group_security_function_name}.`Company_Code-Text`,',') ,ddo.`Company_Code-Text`) OR ({mars_trend_material_group_security_function_name}.`Company_Code-Text`='ALL')) AND
    (array_contains(SPLIT({mars_trend_material_group_security_function_name}.Year,',') ,ddo.`Year`) OR ({mars_trend_material_group_security_function_name}.Year='ALL')) AND
    (array_contains(SPLIT({mars_trend_material_group_security_function_name}.Month,',') ,ddo.`Month`) OR ({mars_trend_material_group_security_function_name}.Month='ALL')) AND
    ddo.OPFLAG != 'D'
    )

    SELECT Channel, `LI bucket 1`, `LI bucket 2`, `Company_Code-Text`, `Material Group`, `Year`, `Month`, sum(`Amount FiFo LC`) AS `Sum of Amount FiFo_LC` FROM temp 
    GROUP BY Channel, `LI bucket 1`, `LI bucket 2`, `Company_Code-Text`, `Material Group`, `Year`, `Month`""")

assignFunctionPermission(uc_catalog_name, uc_curated_schema, mars_trend_material_group_security_function_name, tbl_owner_grp,security_end_user_aad_group_name, security_object_aad_group_name, security_functional_dev_aad_group, security_functional_readers_aad_group)

print("mars trend material group security function dropped and recreated in the schema")

# COMMAND ----------

# DBTITLE 1,MARS Trend LI Bucket - SQL Function
spark.sql(f"""DROP FUNCTION IF EXISTS `{uc_catalog_name}`.`{uc_curated_schema}`.{mars_trend_li_bucket_security_function_name}""")
spark.sql(f"""
CREATE OR REPLACE FUNCTION `{uc_catalog_name}`.`{uc_curated_schema}`.{mars_trend_li_bucket_security_function_name}(Channel STRING, `Company_Code-Text` STRING, Material_Group STRING,Year STRING, Month STRING)
RETURNS TABLE (Channel STRING, `Company_Code-Text` STRING, `LI bucket 1` STRING, `LI bucket 2` STRING, `Material Group` STRING, `Year` STRING, `Month` STRING, `Sum of Amount FiFo_LC` double) 

RETURN  

    with temp as (
    select Channel, `Company_Code-Text`, LI_bucket_1 AS `LI bucket 1`, LI_bucket_2 AS `LI bucket 2`, Material_Group AS `Material Group`, `Year` AS Year,`Month` AS Month, Amount_FiFo_LC AS `Amount FiFo LC`
    from `{uc_catalog_name}`.`{uc_curated_schema}`.{mars_ddo_view_name} ddo
    WHERE
    (array_contains(SPLIT({mars_trend_li_bucket_security_function_name}.Channel,',') ,ddo.Channel) OR ({mars_trend_li_bucket_security_function_name}.Channel='ALL')) AND
    (array_contains(SPLIT({mars_trend_li_bucket_security_function_name}.`Company_Code-Text`,',') ,ddo.`Company_Code-Text`) OR ({mars_trend_li_bucket_security_function_name}.`Company_Code-Text`='ALL')) AND
    (array_contains(SPLIT({mars_trend_li_bucket_security_function_name}.Material_Group,',') ,ddo.Material_Group) OR ({mars_trend_li_bucket_security_function_name}.Material_Group='ALL')) AND
    (array_contains(SPLIT({mars_trend_li_bucket_security_function_name}.Year,',') ,ddo.`Year`) OR ({mars_trend_li_bucket_security_function_name}.Year='ALL')) AND
    (array_contains(SPLIT({mars_trend_li_bucket_security_function_name}.Month,',') ,ddo.`Month`) OR ({mars_trend_li_bucket_security_function_name}.Month='ALL')) AND
    ddo.OPFLAG != 'D'
    )

    SELECT Channel, `Company_Code-Text`, `LI bucket 1`, `LI bucket 2`, `Material Group`, `Year`, `Month`, sum(`Amount FiFo LC`) AS `Sum of Amount FiFo_LC` FROM temp 
    GROUP BY Channel, `Company_Code-Text`, `LI bucket 1`, `LI bucket 2`, `Material Group`, `Year`, `Month`""")

assignFunctionPermission(uc_catalog_name, uc_curated_schema, mars_trend_li_bucket_security_function_name, tbl_owner_grp,security_end_user_aad_group_name, security_object_aad_group_name, security_functional_dev_aad_group, security_functional_readers_aad_group)

print("mars trend LI bucket security function dropped and recreated in the schema")

# COMMAND ----------

# DBTITLE 1,MARS Country Wise - SQL Function
spark.sql(f"""DROP FUNCTION IF EXISTS `{uc_catalog_name}`.`{uc_curated_schema}`.{mars_country_wise_security_function_name}""")
spark.sql(f"""
CREATE OR REPLACE FUNCTION `{uc_catalog_name}`.`{uc_curated_schema}`.{mars_country_wise_security_function_name}(Channel STRING, LI_bucket_1 STRING, LI_bucket_2 STRING, Material_Group STRING, Year STRING, Month STRING)
RETURNS TABLE (Channel STRING,`LI bucket 1` STRING,`LI bucket 2` STRING,`Company_Code-Text` STRING, `Material Group` STRING, `Year` STRING, `Month` STRING, `Sum of Amount FiFo_LC` double) 

RETURN  

    with temp as (
    select Channel, LI_bucket_1 AS `LI bucket 1`, LI_bucket_2 AS `LI bucket 2`, `Company_Code-Text`, Material_Group AS `Material Group`, `Year` AS Year, `Month` AS Month, Amount_FiFo_LC AS `Amount FiFo LC`
    from `{uc_catalog_name}`.`{uc_curated_schema}`.{mars_ddo_view_name} ddo
    WHERE
    (array_contains(SPLIT({mars_country_wise_security_function_name}.Channel,',') ,ddo.Channel) OR ({mars_country_wise_security_function_name}.Channel='ALL')) AND
    (array_contains(SPLIT({mars_country_wise_security_function_name}.LI_bucket_1,',') ,ddo.LI_bucket_1) OR ({mars_country_wise_security_function_name}.LI_bucket_1='ALL')) AND
    (array_contains(SPLIT({mars_country_wise_security_function_name}.LI_bucket_2,',') ,ddo.LI_bucket_2) OR ({mars_country_wise_security_function_name}.LI_bucket_2='ALL')) AND
    (array_contains(SPLIT({mars_country_wise_security_function_name}.Material_Group,',') ,ddo.Material_Group) OR ({mars_country_wise_security_function_name}.Material_Group='ALL')) AND
    (array_contains(SPLIT({mars_country_wise_security_function_name}.Year,',') ,ddo.`Year`) OR ({mars_country_wise_security_function_name}.Year='ALL')) AND
    (array_contains(SPLIT({mars_country_wise_security_function_name}.Month,',') ,ddo.`Month`) OR ({mars_country_wise_security_function_name}.Month='ALL')) AND
    ddo.OPFLAG != 'D'
    )

    SELECT Channel, `LI bucket 1`, `LI bucket 2`, `Company_Code-Text`, `Material Group`, `Year`, `Month`, sum(`Amount FiFo LC`) AS `Sum of Amount FiFo_LC` FROM temp 
    GROUP BY Channel, `LI bucket 1`, `LI bucket 2`, `Company_Code-Text`, `Material Group`, `Year`, `Month`""")

assignFunctionPermission(uc_catalog_name, uc_curated_schema, mars_country_wise_security_function_name, tbl_owner_grp,security_end_user_aad_group_name, security_object_aad_group_name, security_functional_dev_aad_group, security_functional_readers_aad_group)

print("mars country wise security function dropped and recreated in the schema")

# COMMAND ----------

# DBTITLE 1,MARS Plant and Material - SQL Function
spark.sql(f"""DROP FUNCTION IF EXISTS `{uc_catalog_name}`.`{uc_curated_schema}`.{mars_plant_and_material_security_function_name}""")
spark.sql(f"""
CREATE OR REPLACE FUNCTION `{uc_catalog_name}`.`{uc_curated_schema}`.{mars_plant_and_material_security_function_name}(Channel STRING, LI_bucket_1 STRING, LI_bucket_2 STRING, Material_Group STRING, `Company_Code-Text` STRING, Year STRING, Month STRING)
RETURNS TABLE (Channel STRING,`LI bucket 1` STRING,`LI bucket 2` STRING,`Company_Code-Text` STRING, `Material Group` STRING, `FCB Material` STRING, `FCB Plant` STRING, `Year` STRING, `Month` STRING, `Sum of Quantity_KG` double, `Sum of Amount FiFo_LC` double) 

RETURN  

    with temp as (
    select Channel, LI_bucket_1 AS `LI bucket 1`, LI_bucket_2 AS `LI bucket 2`, `Company_Code-Text`, Material_Group AS `Material Group`, FCB_Material AS `FCB Material`, FCB_Plant AS `FCB Plant`, `Year` AS Year, `Month` AS Month, Quantity_KG AS `Quantity KG`, Amount_FiFo_LC AS `Amount FiFo LC`
    from `{uc_catalog_name}`.`{uc_curated_schema}`.{mars_ddo_view_name} ddo
    WHERE
    (array_contains(SPLIT({mars_plant_and_material_security_function_name}.Channel,',') ,ddo.Channel) OR ({mars_plant_and_material_security_function_name}.Channel='ALL')) AND
    (array_contains(SPLIT({mars_plant_and_material_security_function_name}.LI_bucket_1,',') ,ddo.LI_bucket_1) OR ({mars_plant_and_material_security_function_name}.LI_bucket_1='ALL')) AND
    (array_contains(SPLIT({mars_plant_and_material_security_function_name}.LI_bucket_2,',') ,ddo.LI_bucket_2) OR ({mars_plant_and_material_security_function_name}.LI_bucket_2='ALL')) AND
    (array_contains(SPLIT({mars_plant_and_material_security_function_name}.Material_Group,',') ,ddo.Material_Group) OR ({mars_plant_and_material_security_function_name}.Material_Group='ALL')) AND
    (array_contains(SPLIT({mars_plant_and_material_security_function_name}.`Company_Code-Text`,',') ,`Company_Code-Text`) OR ({mars_plant_and_material_security_function_name}.`Company_Code-Text`='ALL')) AND
    (array_contains(SPLIT({mars_plant_and_material_security_function_name}.Year,',') ,ddo.`Year`) OR ({mars_plant_and_material_security_function_name}.Year='ALL')) AND
    (array_contains(SPLIT({mars_plant_and_material_security_function_name}.Month,',') ,ddo.`Month`) OR ({mars_plant_and_material_security_function_name}.Month='ALL')) AND
    ddo.OPFLAG != 'D'
    )

    SELECT Channel, `LI bucket 1`, `LI bucket 2`, `Company_Code-Text`, `Material Group`, `FCB Material`, `FCB Plant`,`Year`, `Month`, sum(`Quantity KG`) AS `Sum of Quantity_KG`, sum(`Amount FiFo LC`) AS `Sum of Amount FiFo_LC` FROM temp 
    GROUP BY Channel, `LI bucket 1`, `LI bucket 2`, `Company_Code-Text`, `Material Group`, `FCB Material`, `FCB Plant`, `Year`, `Month`""")

assignFunctionPermission(uc_catalog_name, uc_curated_schema, mars_plant_and_material_security_function_name, tbl_owner_grp,security_end_user_aad_group_name, security_object_aad_group_name, security_functional_dev_aad_group, security_functional_readers_aad_group)

print("mars plant and material security function dropped and recreated in the schema")

# COMMAND ----------

# DBTITLE 1,MARS Mass balance check source - SQL Function
spark.sql(f"""DROP FUNCTION IF EXISTS `{uc_catalog_name}`.`{uc_curated_schema}`.{mars_mass_balance_check_source_security_function_name}""")
spark.sql(f"""
CREATE OR REPLACE FUNCTION `{uc_catalog_name}`.`{uc_curated_schema}`.{mars_mass_balance_check_source_security_function_name}(Channel STRING, Source STRING, LI_bucket_1 STRING, LI_bucket_2 STRING, `Company_Code-Text` STRING, Year STRING, Month STRING)
RETURNS TABLE (Channel STRING,Source STRING,`LI bucket 1` STRING,`LI bucket 2` STRING,`Company_Code-Text` STRING, `FCB Plant` STRING, `FCB Plant Name` STRING, `Sum of Quantity_KG` double, `Sum of Amount FiFo_LC` double, `Unit price check` double) 

RETURN  

    with temp as (
    select Channel, Source, LI_bucket_1 AS `LI bucket 1`, LI_bucket_2 AS `LI bucket 2`, `Company_Code-Text`, FCB_Plant AS `FCB Plant`, FCB_Plant_Name AS `FCB Plant Name`, Quantity_KG AS `Quantity KG`, Amount_FiFo_LC AS `Amount FiFo LC`
    from `{uc_catalog_name}`.`{uc_curated_schema}`.{mars_ddo_view_name} ddo
    WHERE
    (array_contains(SPLIT({mars_mass_balance_check_source_security_function_name}.Channel,',') ,ddo.Channel) OR ({mars_mass_balance_check_source_security_function_name}.Channel='ALL')) AND
    (array_contains(SPLIT({mars_mass_balance_check_source_security_function_name}.Source,',') ,ddo.Source) OR ({mars_mass_balance_check_source_security_function_name}.Source='ALL')) AND
    (array_contains(SPLIT({mars_mass_balance_check_source_security_function_name}.LI_bucket_1,',') ,ddo.LI_bucket_1) OR ({mars_mass_balance_check_source_security_function_name}.LI_bucket_1='ALL')) AND
    (array_contains(SPLIT({mars_mass_balance_check_source_security_function_name}.LI_bucket_2,',') ,ddo.LI_bucket_2) OR ({mars_mass_balance_check_source_security_function_name}.LI_bucket_2='ALL')) AND
    (array_contains(SPLIT({mars_mass_balance_check_source_security_function_name}.`Company_Code-Text`,',') ,`Company_Code-Text`) OR ({mars_mass_balance_check_source_security_function_name}.`Company_Code-Text`='ALL')) AND
    (array_contains(SPLIT({mars_mass_balance_check_source_security_function_name}.Year,',') ,ddo.`Year`) OR ({mars_mass_balance_check_source_security_function_name}.Year='ALL')) AND
    (array_contains(SPLIT({mars_mass_balance_check_source_security_function_name}.Month,',') ,ddo.`Month`) OR ({mars_mass_balance_check_source_security_function_name}.Month='ALL')) AND
    ddo.OPFLAG != 'D'
    )

    SELECT Channel, Source, `LI bucket 1`, `LI bucket 2`, `Company_Code-Text`, `FCB Plant`, `FCB Plant Name`, sum(`Quantity KG`) AS `Sum of Quantity_KG`, sum(`Amount FiFo LC`) AS `Sum of Amount FiFo_LC`, IF(((sum(`Quantity KG`) IS NOT NULL AND sum(`Amount FiFo LC`) IS NOT NULL) AND (sum(`Quantity KG`) != 0 AND sum(`Amount FiFo LC`) != 0)),cast(round(sum(`Amount FiFo LC`)/sum(`Quantity KG`)) as bigint),0) AS `Unit price check` FROM temp 
    GROUP BY Channel, Source, `LI bucket 1`, `LI bucket 2`, `Company_Code-Text`, `FCB Plant`, `FCB Plant Name`""")

assignFunctionPermission(uc_catalog_name, uc_curated_schema, mars_mass_balance_check_source_security_function_name, tbl_owner_grp,security_end_user_aad_group_name, security_object_aad_group_name, security_functional_dev_aad_group, security_functional_readers_aad_group)

print("mars mass balance check source security function dropped and recreated in the schema")

# COMMAND ----------

# DBTITLE 1,MARS Mass balance check account - SQL Function
spark.sql(f"""DROP FUNCTION IF EXISTS `{uc_catalog_name}`.`{uc_curated_schema}`.{mars_mass_balance_check_account_security_function_name}""")
spark.sql(f"""
CREATE OR REPLACE FUNCTION `{uc_catalog_name}`.`{uc_curated_schema}`.{mars_mass_balance_check_account_security_function_name}(Channel STRING, LI_bucket_2 STRING, LI_bucket_1 STRING, Material_Group STRING, Year STRING, Month STRING)
RETURNS TABLE (Channel STRING,`LI bucket 2` STRING,`LI bucket 1` STRING,`Material Group` STRING, Account STRING, `Account Name` STRING, `Sum of Quantity_KG` double, `Sum of Amount FiFo_LC` double, `Average of Unit Rate LC/Kg` double) 

RETURN  

    with temp as (
    select Channel, LI_bucket_2 AS `LI bucket 2`, LI_bucket_1 AS `LI bucket 1`, Material_Group AS `Material Group`, Account, Account_Name AS `Account Name`, Quantity_KG AS `Quantity KG`, Amount_FiFo_LC AS `Amount FiFo LC`
    from `{uc_catalog_name}`.`{uc_curated_schema}`.{mars_ddo_view_name} ddo
    WHERE
    (array_contains(SPLIT({mars_mass_balance_check_account_security_function_name}.Channel,',') ,ddo.Channel) OR ({mars_mass_balance_check_account_security_function_name}.Channel='ALL')) AND
    (array_contains(SPLIT({mars_mass_balance_check_account_security_function_name}.LI_bucket_2,',') ,ddo.LI_bucket_2) OR ({mars_mass_balance_check_account_security_function_name}.LI_bucket_2='ALL')) AND
    (array_contains(SPLIT({mars_mass_balance_check_account_security_function_name}.LI_bucket_1,',') ,ddo.LI_bucket_1) OR ({mars_mass_balance_check_account_security_function_name}.LI_bucket_1='ALL')) AND
    (array_contains(SPLIT({mars_mass_balance_check_account_security_function_name}.Material_Group,',') ,ddo.Material_Group) OR ({mars_mass_balance_check_account_security_function_name}.Material_Group='ALL')) AND
    (array_contains(SPLIT({mars_mass_balance_check_account_security_function_name}.Year,',') ,ddo.`Year`) OR ({mars_mass_balance_check_account_security_function_name}.Year='ALL')) AND
    (array_contains(SPLIT({mars_mass_balance_check_account_security_function_name}.Month,',') ,ddo.`Month`) OR ({mars_mass_balance_check_account_security_function_name}.Month='ALL')) AND
    ddo.OPFLAG != 'D'
    )
    SELECT Channel, `LI bucket 2`, `LI bucket 1`, `Material Group`, Account, `Account Name`, `Sum of Quantity_KG`, `Sum of Amount FiFo_LC`, IF(((`Sum of Quantity_KG` IS NOT NULL AND `Sum of Amount FiFo_LC` IS NOT NULL) AND (`Sum of Quantity_KG` != 0 AND `Sum of Amount FiFo_LC` != 0)),cast(round(avg(`Sum of Amount FiFo_LC`/`Sum of Quantity_KG`)) as bigint),0) AS `Average of Unit Rate LC/Kg` FROM (
    SELECT Channel, `LI bucket 2`, `LI bucket 1`, `Material Group`, Account, `Account Name`, sum(`Quantity KG`) AS `Sum of Quantity_KG`, sum(`Amount FiFo LC`) AS `Sum of Amount FiFo_LC` FROM temp 
    GROUP BY Channel, `LI bucket 2`, `LI bucket 1`, `Material Group`, Account, `Account Name`) mass_balance_check_account 
    GROUP BY Channel, `LI bucket 2`, `LI bucket 1`, `Material Group`, Account, `Account Name`, `Sum of Quantity_KG`, `Sum of Amount FiFo_LC`""")

assignFunctionPermission(uc_catalog_name, uc_curated_schema, mars_mass_balance_check_account_security_function_name, tbl_owner_grp,security_end_user_aad_group_name, security_object_aad_group_name, security_functional_dev_aad_group, security_functional_readers_aad_group)

print("mars mass balance check account security function dropped and recreated in the schema")

# COMMAND ----------

# DBTITLE 1,MARS detailed overview - SQL Function
spark.sql(f"""DROP FUNCTION IF EXISTS `{uc_catalog_name}`.`{uc_curated_schema}`.{mars_detail_overview_security_function_name}""")
spark.sql(f"""
CREATE OR REPLACE FUNCTION `{uc_catalog_name}`.`{uc_curated_schema}`.{mars_detail_overview_security_function_name}(Channel STRING, LI_bucket_2 STRING, Material_Group STRING, `Company_Code-Text` STRING, Year STRING, Month STRING)
RETURNS TABLE (Channel STRING, `LI bucket 2` STRING, `LI bucket 1` STRING, `Material Group` STRING, `Company_Code-Text` STRING, `FCB Plant` STRING, `FCB Plant Name` STRING, `FCB Material` STRING, `Material Name` STRING, `Customer Vendor Name` STRING, `Functional Area` STRING, `Partner Plant` STRING, `Partner Material` STRING, `Sum of Quantity_KG` double, `Sum of Volume_L15` double, `Sum of Amount FiFo_LC` double, `Sum of Unit Rate LC To` double) 

RETURN  

    with temp as (
    select Channel, LI_bucket_2 AS `LI bucket 2`, LI_bucket_1 AS `LI bucket 1`, Material_Group AS `Material Group`, `Company_Code-Text`, FCB_Plant AS `FCB Plant`, FCB_Plant_Name AS `FCB Plant Name`, FCB_Material AS `FCB Material`, Material_Name AS `Material Name`,  Customer_Vendor_Name AS `Customer Vendor Name`, Functional_Area AS `Functional Area`,  Partner_Plant AS `Partner Plant`, Partner_Material AS `Partner Material`, Quantity_KG AS `Quantity KG`, Volume_L15 AS `Volume L15`, Amount_FiFo_LC AS `Amount FiFo LC`
    from `{uc_catalog_name}`.`{uc_curated_schema}`.{mars_ddo_view_name} ddo
    WHERE
    (array_contains(SPLIT({mars_detail_overview_security_function_name}.Channel,',') ,ddo.Channel) OR ({mars_detail_overview_security_function_name}.Channel='ALL')) AND
    (array_contains(SPLIT({mars_detail_overview_security_function_name}.LI_bucket_2,',') ,ddo.LI_bucket_2) OR ({mars_detail_overview_security_function_name}.LI_bucket_2='ALL')) AND
    (array_contains(SPLIT({mars_detail_overview_security_function_name}.Material_Group,',') ,ddo.Material_Group) OR ({mars_detail_overview_security_function_name}.Material_Group='ALL')) AND
    (array_contains(SPLIT({mars_detail_overview_security_function_name}.`Company_Code-Text`,',') ,`Company_Code-Text`) OR ({mars_detail_overview_security_function_name}.`Company_Code-Text`='ALL')) AND
    (array_contains(SPLIT({mars_detail_overview_security_function_name}.Year,',') ,ddo.`Year`) OR ({mars_detail_overview_security_function_name}.Year='ALL')) AND
    (array_contains(SPLIT({mars_detail_overview_security_function_name}.Month,',') ,ddo.`Month`) OR ({mars_detail_overview_security_function_name}.Month='ALL')) AND
    ddo.OPFLAG != 'D'
    )
    SELECT Channel, `LI bucket 2`, `LI bucket 1`, `Material Group`, `Company_Code-Text`, `FCB Plant`, `FCB Plant Name`, `FCB Material`, `Material Name`,  `Customer Vendor Name`, `Functional Area`,  `Partner Plant`, `Partner Material`, sum(`Quantity KG`) AS `Sum of Quantity_KG`, sum(`Volume L15`) AS `Sum of Volume_L15`, sum(`Amount FiFo LC`) AS `Sum of Amount FiFo_LC`, IF(((sum(`Volume L15`) IS NOT NULL AND sum(`Amount FiFo LC`) IS NOT NULL) AND (sum(`Volume L15`) != 0 AND sum(`Amount FiFo LC`) != 0)),cast(round(sum(`Amount FiFo LC`)/sum(`Volume L15`)) as bigint),0) AS `Sum of Unit Rate LC To` FROM temp GROUP BY Channel, `LI bucket 2`, `LI bucket 1`, `Material Group`, `Company_Code-Text`, `FCB Plant`, `FCB Plant Name`, `FCB Material`, `Material Name`,  `Customer Vendor Name`, `Functional Area`, `Partner Plant`, `Partner Material`
""")

assignFunctionPermission(uc_catalog_name, uc_curated_schema, mars_detail_overview_security_function_name, tbl_owner_grp, security_end_user_aad_group_name, security_object_aad_group_name, security_functional_dev_aad_group, security_functional_readers_aad_group)

print("mars detail view security function dropped and recreated in the schema")

# COMMAND ----------

# DBTITLE 1,MARS stocks - SQL Function
spark.sql(f"""DROP FUNCTION IF EXISTS `{uc_catalog_name}`.`{uc_curated_schema}`.{mars_stocks_security_function_name}""")
spark.sql(f"""
CREATE OR REPLACE FUNCTION `{uc_catalog_name}`.`{uc_curated_schema}`.{mars_stocks_security_function_name}(Channel STRING, Source STRING, `Company_Code-Text` STRING, Functional_Area STRING, `Account-Text` STRING, LI_bucket_1 STRING, Year STRING, Month STRING) 
RETURNS TABLE (Channel STRING, Source STRING, `Company_Code-Text` STRING, `Functional Area` STRING, `Account-Text` STRING, `LI bucket 1` STRING, `Material Group` STRING, `FCB Plant` STRING, `FCB Plant Name` STRING, `FCB Material` STRING, `Material Name` STRING, `LI bucket 2` STRING, `Sum of Quantity KG` double, `Sum of Amount FiFo LC` double, `Sum of Unit Rate LC To` double) 

RETURN  

    with temp as (
    select Channel, Source, `Company_Code-Text`, Functional_Area AS `Functional Area`,`Account-Text`, LI_bucket_1 AS `LI bucket 1`, Material_Group AS `Material Group`, FCB_Plant AS `FCB Plant`, FCB_Plant_Name AS `FCB Plant Name`, FCB_Material AS `FCB Material`, Material_Name AS `Material Name`, LI_bucket_2 AS `LI bucket 2`, Quantity_KG AS `Quantity KG`, Amount_FiFo_LC AS `Amount FiFo LC` from `{uc_catalog_name}`.`{uc_curated_schema}`.{mars_ddo_view_name} ddo
    WHERE
    (array_contains(SPLIT({mars_stocks_security_function_name}.Channel,',') ,ddo.Channel) OR ({mars_stocks_security_function_name}.Channel='ALL')) AND
    (array_contains(SPLIT({mars_stocks_security_function_name}.Source,',') ,ddo.Source) OR ({mars_stocks_security_function_name}.Source='ALL')) AND
    (array_contains(SPLIT({mars_stocks_security_function_name}.`Company_Code-Text`,',') ,ddo.`Company_Code-Text`) OR ({mars_stocks_security_function_name}.`Company_Code-Text`='ALL')) AND
    (array_contains(SPLIT({mars_stocks_security_function_name}.Functional_Area,',') ,ddo.Functional_Area) OR ({mars_stocks_security_function_name}.Functional_Area='ALL')) AND
    (array_contains(SPLIT({mars_stocks_security_function_name}.`Account-Text`,',') ,ddo.`Account-Text`) OR ({mars_stocks_security_function_name}.`Account-Text`='ALL')) AND
    (array_contains(SPLIT({mars_stocks_security_function_name}.LI_bucket_1,',') ,ddo.LI_bucket_1) OR ({mars_stocks_security_function_name}.LI_bucket_1='ALL')) AND
    (array_contains(SPLIT({mars_stocks_security_function_name}.Year,',') ,ddo.`Year`) OR ({mars_stocks_security_function_name}.Year='ALL')) AND
    (array_contains(SPLIT({mars_stocks_security_function_name}.Month,',') ,ddo.`Month`) OR ({mars_stocks_security_function_name}.Month='ALL')) AND
    ddo.OPFLAG != 'D'
    )

    SELECT Channel, Source, `Company_Code-Text`, `Functional Area`,`Account-Text`, `LI bucket 1`, `Material Group`, `FCB Plant`, `FCB Plant Name`, `FCB Material`, `Material Name`, `LI bucket 2`, sum(`Quantity KG`) AS `Sum of Quantity_KG`, sum(`Amount FiFo LC`) AS `Sum of Amount FiFo_LC`, IF(((sum(`Quantity KG`) IS NOT NULL AND sum(`Amount FiFo LC`) IS NOT NULL) AND (sum(`Quantity KG`) != 0 AND sum(`Amount FiFo LC`) != 0)),cast(round((sum(`Amount FiFo LC`)/sum(`Quantity KG`))*1000) as bigint),0) AS `Sum of Unit Rate LC To` FROM temp
    GROUP BY Channel, Source, `Company_Code-Text`, `Functional Area`,`Account-Text`, `LI bucket 1`, `Material Group`, `FCB Plant`, `FCB Plant Name`, `FCB Material`, `Material Name`, `LI bucket 2`""")

assignFunctionPermission(uc_catalog_name, uc_curated_schema, mars_stocks_security_function_name, tbl_owner_grp,security_end_user_aad_group_name, security_object_aad_group_name, security_functional_dev_aad_group, security_functional_readers_aad_group)

print("mars stocks security function dropped and recreated in the schema")

# COMMAND ----------

# DBTITLE 1,MARS margin explanation - SQL Function
spark.sql(f"""DROP FUNCTION IF EXISTS `{uc_catalog_name}`.`{uc_curated_schema}`.{mars_margin_explanation_security_function_name}""")
spark.sql(f"""
CREATE OR REPLACE FUNCTION `{uc_catalog_name}`.`{uc_curated_schema}`.{mars_margin_explanation_security_function_name}(Plant_Name STRING, Plant STRING, Material_Name STRING, FCB_Material STRING, Bio_Group STRING, Year STRING, Month STRING) 
RETURNS TABLE (Plant_Name STRING, Plant STRING, Material_Name STRING, FCB_Material STRING, Bio_Group STRING,`Company_Code-Text` STRING, `Material Group Finance` STRING, `Margin Bucket` STRING, `Sum of SumOfC1 FifO` double) 

RETURN  

    with temp as (
    select Plant_Name, Plant, Material_Name, FCB_Material, Bio_Group, `Company_Code-Text`, Material_Group_Finance AS `Material Group Finance`, Margin_Bucket AS `Margin Bucket`, SumOfC1_FifO AS `SumOfC1 FifO` from `{uc_catalog_name}`.`{uc_curated_schema}`.{mars_mbc_view_name} mbc
    WHERE
    (array_contains(SPLIT({mars_margin_explanation_security_function_name}.Plant_Name,',') ,mbc.Plant_Name) OR ({mars_margin_explanation_security_function_name}.Plant_Name='ALL')) AND
    (array_contains(SPLIT({mars_margin_explanation_security_function_name}.Plant,',') ,mbc.Plant) OR ({mars_margin_explanation_security_function_name}.Plant='ALL')) AND
    (array_contains(SPLIT({mars_margin_explanation_security_function_name}.Material_Name,',') ,mbc.Material_Name) OR ({mars_margin_explanation_security_function_name}.Material_Name='ALL')) AND
    (array_contains(SPLIT({mars_margin_explanation_security_function_name}.FCB_Material,',') ,mbc.FCB_Material) OR ({mars_margin_explanation_security_function_name}.FCB_Material='ALL')) AND
    (array_contains(SPLIT({mars_margin_explanation_security_function_name}.Bio_Group,',') ,mbc.Bio_Group) OR ({mars_margin_explanation_security_function_name}.Bio_Group='ALL')) AND
    (array_contains(SPLIT({mars_margin_explanation_security_function_name}.Year,',') ,mbc.`Year`) OR ({mars_margin_explanation_security_function_name}.Year='ALL')) AND
    (array_contains(SPLIT({mars_margin_explanation_security_function_name}.Month,',') ,mbc.`Month`) OR ({mars_margin_explanation_security_function_name}.Month='ALL')) AND
    mbc.OPFLAG != 'D'
    )

    SELECT Plant_Name, Plant, Material_Name, FCB_Material, Bio_Group, `Company_Code-Text`, `Material Group Finance`, `Margin Bucket`, sum(`SumOfC1 FifO`) AS `Sum of SumOfC1 FifO` FROM temp
    GROUP BY Plant_Name, Plant, Material_Name, FCB_Material, Bio_Group, `Company_Code-Text`, `Material Group Finance`, `Margin Bucket`""")

assignFunctionPermission(uc_catalog_name, uc_curated_schema, mars_margin_explanation_security_function_name, tbl_owner_grp,security_end_user_aad_group_name, security_object_aad_group_name, security_functional_dev_aad_group, security_functional_readers_aad_group)

print("mars margin explanation security function dropped and recreated in the schema")

# COMMAND ----------

# DBTITLE 1,MARS Composite NIBIAT - SQL Function
spark.sql(f"""DROP FUNCTION IF EXISTS `{uc_catalog_name}`.`{uc_curated_schema}`.{mars_composite_nibiat_security_function_name}""")
spark.sql(f"""
CREATE OR REPLACE FUNCTION `{uc_catalog_name}`.`{uc_curated_schema}`.{mars_composite_nibiat_security_function_name}(Channel STRING, Year STRING, Month STRING) 
RETURNS TABLE (Channel STRING, `Company_Code-Text` STRING, `Material Group` STRING, `Sum of Amount FiFo_LC` double) 

RETURN  

    with temp as (
    select Channel, `Company_Code-Text`, Material_Group AS `Material Group`, Amount_FiFo_LC AS `Amount FiFo LC` 
    from `{uc_catalog_name}`.`{uc_curated_schema}`.{mars_ddo_view_name} ddo
    WHERE
    (array_contains(SPLIT({mars_composite_nibiat_security_function_name}.Channel,',') ,ddo.Channel) OR ({mars_composite_nibiat_security_function_name}.Channel='ALL')) AND
    (array_contains(SPLIT({mars_composite_nibiat_security_function_name}.Year,',') ,ddo.`Year`) OR ({mars_composite_nibiat_security_function_name}.Year='ALL')) AND
    (array_contains(SPLIT({mars_composite_nibiat_security_function_name}.Month,',') ,ddo.`Month`) OR ({mars_composite_nibiat_security_function_name}.Month='ALL')) AND
    ddo.OPFLAG != 'D'
    )

    SELECT Channel, `Company_Code-Text`, `Material Group`, sum(`Amount FiFo LC`) AS `Sum of Amount FiFo LC` FROM temp
    GROUP BY Channel, `Company_Code-Text`, `Material Group`""")

assignFunctionPermission(uc_catalog_name, uc_curated_schema, mars_composite_nibiat_security_function_name, tbl_owner_grp,security_end_user_aad_group_name, security_object_aad_group_name, security_functional_dev_aad_group, security_functional_readers_aad_group)

print("mars composite nibiat security function dropped and recreated in the schema")
