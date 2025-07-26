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
fcb_dynamic_view_security_function_name = dbutils.widgets.get("fcb_dynamic_view_security_function_name")
fcb_price_effect_security_function_name = dbutils.widgets.get("fcb_price_effect_security_function_name")
fcb_stock_effect_security_function_name = dbutils.widgets.get("fcb_stock_effect_security_function_name")
massbalance_per_commodity_grade_security_function_name = dbutils.widgets.get("massbalance_per_commodity_grade_security_function_name")
internal_transfers_security_function_name = dbutils.widgets.get("internal_transfers_security_function_name")
fcb_per_commodity_grade_security_function_name = dbutils.widgets.get("fcb_per_commodity_grade_security_function_name")
tfp_exception_security_function_name = dbutils.widgets.get("tfp_exception_security_function_name")
detailed_report_security_function_name = dbutils.widgets.get("detailed_report_security_function_name")
gains_and_losses_security_function_name = dbutils.widgets.get("gains_and_losses_security_function_name")
fcb_dynamic_view_name = dbutils.widgets.get(name="fcb_dynamic_view_name")

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

security_end_user_aad_group_name = env_conf.security_end_user_aad_group
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
df_schema_function = spark.sql(f"SHOW FUNCTIONS IN `{uc_catalog_name}`.`{uc_curated_schema}`").select("function")
function_list = [row[0] for row in df_schema_function.select('function').collect()]

# COMMAND ----------

fcb_dynamic_view_security_func_name = uc_catalog_name+"."+uc_curated_schema+"."+fcb_dynamic_view_security_function_name

if fcb_dynamic_view_security_func_name in function_list:
    spark.sql(f"""DROP FUNCTION `{uc_catalog_name}`.`{uc_curated_schema}`.{fcb_dynamic_view_security_function_name}""")
    spark.sql(f"""CREATE OR REPLACE FUNCTION `{uc_catalog_name}`.`{uc_curated_schema}`.{fcb_dynamic_view_security_function_name} (`Company_Code-Text` STRING,`Profit_Center-Text` STRING, Fiscal_Year STRING, Posting_Period STRING, Division STRING, Account_No STRING, Document_Type STRING, `Plant-Text` STRING, CURRENCY STRING, UOM STRING, Display_Unit STRING) 
    RETURNS TABLE (line_item STRING, line_item_header STRING, R3_Amount decimal(25,2), R3_Quantity decimal(25,2)) 
    RETURN 
    with temp as ( 
    select Amt_Comp_Code_Curr, Amt_PC_Local_Curr, Weight_Qty, Volume_Qty,Volume_Qty_GAL, Volume_Qty_BB6,Account_No,Functional_Area,Posting_Period,Plant 
    from `{uc_catalog_name}`.`{uc_curated_schema}`.{fcb_dynamic_view_name} fcb 
    WHERE 
    (array_contains(SPLIT({fcb_dynamic_view_security_function_name}.`Company_Code-Text`,',') ,fcb.`Company_Code-Text`) OR ({fcb_dynamic_view_security_function_name}.`Company_Code-Text`='ALL')) AND 
    (array_contains(SPLIT({fcb_dynamic_view_security_function_name}.`Profit_Center-Text`,',') ,fcb.`Profit_Center-Text`) OR ({fcb_dynamic_view_security_function_name}.`Profit_Center-Text`='ALL')) AND 
    (array_contains(SPLIT({fcb_dynamic_view_security_function_name}.Fiscal_Year,',') ,fcb.Fiscal_Year) OR 
        ({fcb_dynamic_view_security_function_name}.Fiscal_Year='ALL')) AND 
    (array_contains(SPLIT({fcb_dynamic_view_security_function_name}.Posting_Period,',') ,fcb.Posting_Period) OR 
        ({fcb_dynamic_view_security_function_name}.Posting_Period='ALL')) AND 
    (array_contains(SPLIT({fcb_dynamic_view_security_function_name}.Division,',') ,fcb.Division) OR 
        ({fcb_dynamic_view_security_function_name}.Division='ALL')) AND 
    (array_contains(SPLIT({fcb_dynamic_view_security_function_name}.Account_No,',') ,fcb.Account_No) OR 
        ({fcb_dynamic_view_security_function_name}.Account_No='ALL')) AND 
    (array_contains(SPLIT({fcb_dynamic_view_security_function_name}.Document_Type,',') ,fcb.Document_Type) OR 
        ({fcb_dynamic_view_security_function_name}.Document_Type='ALL')) AND 
    (array_contains(SPLIT({fcb_dynamic_view_security_function_name}.`Plant-Text`,',') ,fcb.`Plant-Text`) OR 
    ({fcb_dynamic_view_security_function_name}.`Plant-Text`='ALL'))
    AND fcb.OPFLAG != 'D') 
    
    SELECT `line_item`, `line_item_header`, 
    CASE WHEN {fcb_dynamic_view_security_function_name}.Display_Unit = 'Actual Value' THEN `R3_Amount`
    WHEN {fcb_dynamic_view_security_function_name}.Display_Unit = 'Thousands' THEN `R3_Amount`*0.001
    WHEN {fcb_dynamic_view_security_function_name}.Display_Unit = 'Millions' THEN `R3_Amount`*0.000001
    WHEN {fcb_dynamic_view_security_function_name}.Display_Unit = 'Billions' THEN `R3_Amount`*0.000000001
    END
    AS `R3_Amount`,

    CASE WHEN {fcb_dynamic_view_security_function_name}.Display_Unit = 'Actual Value' THEN `R3_Quantity`
    WHEN {fcb_dynamic_view_security_function_name}.Display_Unit = 'Thousands' THEN `R3_Quantity`*0.001
    WHEN {fcb_dynamic_view_security_function_name}.Display_Unit = 'Millions' THEN `R3_Quantity`*0.000001
    WHEN {fcb_dynamic_view_security_function_name}.Display_Unit = 'Billions' THEN `R3_Quantity`*0.000000001
    END
    AS `R3_Quantity`

    FROM  
    
    (SELECT "Inner LOB Sale" as line_item, "Sales Proceeds" as line_item_header, 
        COALESCE(sum(INTER_LOB_SALES1),0) + COALESCE(sum(INTER_LOB_SALES2),0) as R3_Amount, 
        sum(INTER_LOB_SALES_QTY) as R3_Quantity FROM 
    (SELECT Account_No,Functional_Area, \
        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006380302","0006380303","0006355565") AND Functional_Area IN ("SV46") THEN SUM(Amt_Comp_Code_Curr)*(-1) 
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006380302","0006380303","0006355565")AND Functional_Area IN ("SV46") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS INTER_LOB_SALES1, 
        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006380305") THEN SUM(Amt_Comp_Code_Curr)*(-1) 
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006380305") THEN SUM(Amt_PC_Local_Curr)*(-1) 
                    ELSE "" END AS INTER_LOB_SALES2, 
            CASE WHEN {fcb_dynamic_view_security_function_name}.UOM="KG" AND Account_No IN ("0006380302","0006380303") AND Functional_Area ="SV46" THEN SUM(Weight_Qty) 
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="L15" AND Account_No IN ("0006380302","0006380303") AND Functional_Area ="SV46" THEN SUM(Volume_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="GAL" AND Account_No IN ("0006380302","0006380303") AND Functional_Area ="SV46" THEN SUM(Volume_Qty_GAL)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="BBL" AND Account_No IN ("0006380302","0006380303") AND Functional_Area ="SV46" THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS INTER_LOB_SALES_QTY
        from temp 
        group by Account_No,Functional_Area
    )
    UNION
    SELECT  "IG Sales" as line_item, "Sales Proceeds" as line_item_header, sum(IG_SALES) as R3_Amount, sum(IG_SALES_QTY) as R3_Quantity 
    FROM
    (SELECT Account_No,Functional_Area,

        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006001500","0006001700","0006001900","0006002100","0006002200","0006009002","0006110035","0006110135","0006110235","0006520350","0006520360") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006001500","0006001700","0006001900","0006002100","0006002200","0006009002","0006110035","0006110135","0006110235","0006520350","0006520360") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IG_SALES,

        CASE WHEN {fcb_dynamic_view_security_function_name}.UOM="KG" AND Account_No IN ("0006001500","0006001700","0006001900","0006002100") THEN SUM(Weight_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="L15" AND Account_No IN ("0006001500","0006001700","0006001900","0006002100") THEN SUM(Volume_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="GAL" AND Account_No IN ("0006001500","0006001700","0006001900","0006002100") THEN SUM(Volume_Qty_GAL)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="BBL" AND Account_No IN ("0006001500","0006001700","0006001900","0006002100") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IG_SALES_QTY

        from temp 
        group by Account_No,Functional_Area
    )

    UNION
    SELECT  "3rd Party Sales" as line_item, "Sales Proceeds" as line_item_header, sum(THIRD_PARTY_SALES) as R3_Amount, sum(THIRD_PARTY_SALES_QTY) as R3_Quantity

    FROM

    (SELECT Account_No,Functional_Area,
                
            CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006000100","0006000150","0006000151","0006000152","0006000153", "0006000200","0006000250","0006000300","0006000400","0006000500","0006000600","0006000650","0006000700","0006000800","0006002105","0006002205","0006002230","0006009000","0006009005","0006009010","0006009015","0006009050","0006009100","0006009800","0006009920","0006110010","0006110015","0006110020","0006110021","0006110030","0006110031","0006110110","0006110115","0006110120","0006110121","0006110130","0006110131","0006110210","0006110215","0006110220","0006110221","0006110230","0006110231","0006110300","0006110305","0006110310","0006110320","0006110330","0006110340","0006140100","0006140105","0006140200","0006140300","0006140500","0006140700","0006140800","0006340610","0006340620","0006340710","0006342300","0006342500","0006342550","0006920460","0006920461") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006000100","0006000150","0006000151","0006000152","0006000153", "0006000200","0006000250","0006000300","0006000400","0006000500","0006000600","0006000650","0006000700","0006000800","0006002105","0006002205","0006002230","0006009000","0006009005","0006009010","0006009015","0006009050","0006009100","0006009800","0006009920","0006110010","0006110015","0006110020","0006110021","0006110030","0006110031","0006110110","0006110115","0006110120","0006110121","0006110130","0006110131","0006110210","0006110215","0006110220","0006110221","0006110230","0006110231","0006110300","0006110305","0006110310","0006110320","0006110330","0006110340","0006140100","0006140105","0006140200","0006140300","0006140500","0006140700","0006140800","0006340610","0006340620","0006340710","0006342300","0006342500","0006342550","0006920460","0006920461") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS THIRD_PARTY_SALES,

            CASE WHEN {fcb_dynamic_view_security_function_name}.UOM="KG" AND Account_No IN ("0006000100","0006000150","0006000200","0006000250","0006000300","0006000400","0006000500","0006000600","0006002105","0006009000","0006009050","0006009100","0006140100","0006140105","0006140200","0006140300","0006140500","0006140700","0006342550") THEN SUM(Weight_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="L15" AND Account_No IN ("0006000100","0006000150","0006000200","0006000250","0006000300","0006000400","0006000500","0006000600","0006002105","0006009000","0006009050","0006009100","0006140100","0006140105","0006140200","0006140300","0006140500","0006140700","0006342550") THEN SUM(Volume_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="GAL" AND Account_No IN ("0006000100","0006000150","0006000200","0006000250","0006000300","0006000400","0006000500","0006000600","0006002105","0006009000","0006009050","0006009100","0006140100","0006140105","0006140200","0006140300","0006140500","0006140700","0006342550") THEN SUM(Volume_Qty_GAL)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="BBL" AND Account_No IN ("0006000100","0006000150","0006000200","0006000250","0006000300","0006000400","0006000500","0006000600","0006002105","0006009000","0006009050","0006009100","0006140100","0006140105","0006140200","0006140300","0006140500","0006140700","0006342550") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS THIRD_PARTY_SALES_QTY

        from temp 
        group by Account_No,Functional_Area
    )
    UNION
    SELECT  "Proceeds from Services" as line_item, "Sales Proceeds" as line_item_header, sum(PROCEEDS_FROM_SERVICE) as R3_Amount, 0 as R3_Quantity

    FROM

    (SELECT Account_No,Functional_Area,
                
        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006140025","0006520050","0006520100","0006520300","0006520310","0006520400","0006590100","0006610100","0006610200","0006610300","0006610400","0006900200","0006900205","0006900250","0006900400","0007220560") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006140025","0006520050","0006520100","0006520300","0006520310","0006520400","0006590100","0006610100","0006610200","0006610300","0006610400","0006900200","0006900205","0006900250","0006900400","0007220560") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS PROCEEDS_FROM_SERVICE

        from temp 
        group by Account_No,Functional_Area
    )

    UNION
    SELECT  "EXG Sales" as line_item, "Exchanges" as line_item_header, sum(EXG_SALES) as R3_Amount, sum(EXG_SALES_QTY) as R3_Quantity

    FROM

    (SELECT Account_No,Functional_Area,
                
        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006002220","0006350510","0006190200") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006002220","0006350510","0006190200") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS EXG_SALES,

        CASE WHEN {fcb_dynamic_view_security_function_name}.UOM="KG" AND Account_No ="0006002220" THEN SUM(Weight_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="L15" AND Account_No ="0006002220" THEN SUM(Volume_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="GAL" AND Account_No ="0006002220" THEN SUM(Volume_Qty_GAL)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="BBL" AND Account_No ="0006002220" THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS EXG_SALES_QTY

        from temp 
        group by Account_No,Functional_Area
    )
    UNION

    SELECT  "EXG Purchases" as line_item, "Exchanges" as line_item_header,  COALESCE(sum(EXG_PURCHASES1),0)+ COALESCE(sum(EXG_PURCHASES2),0) as R3_Amount, sum(EXG_PURCHASE_QTY) as R3_Quantity

    FROM

    (SELECT Account_No,Functional_Area,
                
        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006342000","0006340010") AND Functional_Area IN ("SV10","SV11","SV12","SV50","SV51") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006342000","0006340010")AND Functional_Area IN ("SV10","SV11","SV12","SV50","SV51") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS EXG_PURCHASES1,

        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0008399100","0006350520") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0008399100","0006350520") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS EXG_PURCHASES2,

        CASE WHEN {fcb_dynamic_view_security_function_name}.UOM="KG" AND Account_No IN ("0006342000","0006340010") AND Functional_Area IN ("SV10","SV11","SV12","SV50","SV51") THEN SUM(Weight_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="L15" AND Account_No IN ("0006342000","0006340010") AND Functional_Area IN ("SV10","SV11","SV12","SV50","SV51")THEN SUM(Volume_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="GAL" AND Account_No IN ("0006342000","0006340010") AND Functional_Area IN ("SV10","SV11","SV12","SV50","SV51") THEN SUM(Volume_Qty_GAL)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="BBL" AND Account_No IN ("0006342000","0006340010") AND Functional_Area IN ("SV10","SV11","SV12","SV50","SV51") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS EXG_PURCHASE_QTY

        from temp 
        group by Account_No,Functional_Area
    )

    UNION 

    SELECT  "3rd Party Purchases" as line_item, "Acquisition Costs" as line_item_header, COALESCE(sum(THIRD_PARTY_PURCHASES_CURRENCY1),0)+ COALESCE(sum(THIRD_PARTY_PURCHASES_CURRENCY2),0) as R3_Amount, COALESCE(sum(THIRD_PARTY_PURCHASES_UOM1),0)+ COALESCE(sum(THIRD_PARTY_PURCHASES_UOM2),0) as R3_Quantity

    FROM 
    (SELECT Account_No,Functional_Area,
                
        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV05","SV06") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV05","SV06") THEN SUM(Amt_PC_Local_Curr)*(-1)
                ELSE "" END AS THIRD_PARTY_PURCHASES_CURRENCY1,

        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006330140","0006340020","0006342110","0006342130","0006342150","0006342200","0006342510","0006342520","0006342530","0006351070","0006351800","0006354200","0006900470","0007250210","0006341140","0006341141","0006341142","0006341143","0006365000") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006330140","0006340020","0006342110","0006342130","0006342150","0006342200","0006342510","0006342520","0006342530","0006351070","0006351800","0006354200","0006900470","0007250210","0006341140","0006341141","0006341142","0006341143","0006365000") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS THIRD_PARTY_PURCHASES_CURRENCY2,

        CASE WHEN {fcb_dynamic_view_security_function_name}.UOM="KG" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV05","SV06") THEN SUM(Weight_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="L15" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV05","SV06")THEN SUM(Volume_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="GAL" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV05","SV06") THEN SUM(Volume_Qty_GAL)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="BBL" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV05","SV06") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS THIRD_PARTY_PURCHASES_UOM1,

        CASE WHEN {fcb_dynamic_view_security_function_name}.UOM="KG" AND Account_No IN ("0006330140","0006340020","0006342110","0006342130","0006342150","0006342200","0006342510","0006342520","0006342530","0006351070","0006351800","0006354200","0006900470","0007250210","0006341140","0006341141","0006341142","0006341143","0006365000") THEN SUM(Weight_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="L15" AND Account_No IN ("0006330140","0006340020","0006342110","0006342130","0006342150","0006342200","0006342510","0006342520","0006342530","0006351070","0006351800","0006354200","0006900470","0007250210","0006341140","0006341141","0006341142","0006341143","0006365000") THEN SUM(Volume_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="GAL" AND Account_No IN ("0006330140","0006340020","0006342110","0006342130","0006342150","0006342200","0006342510","0006342520","0006342530","0006351070","0006351800","0006354200","0006900470","0007250210","0006341140","0006341141","0006341142","0006341143","0006365000") THEN SUM(Volume_Qty_GAL)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="BBL" AND Account_No IN ("0006330140","0006340020","0006342110","0006342130","0006342150","0006342200","0006342510","0006342520","0006342530","0006351070","0006351800","0006354200","0006900470","0007250210","0006341140","0006341141","0006341142","0006341143","0006365000") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS THIRD_PARTY_PURCHASES_UOM2

        from temp 
        group by Account_No,Functional_Area
    )

    UNION 

    SELECT  "IG Purchases" as line_item, "Acquisition Costs" as line_item_header, COALESCE(sum(IG_PURCHASE_AMOUNT1),0)+ COALESCE(sum(IG_PURCHASE_AMOUNT2),0) as R3_Amount, COALESCE(sum(IG_PURCHASE_QTY1),0)+ COALESCE(sum(IG_PURCHASE_QTY2),0) as R3_Quantity

    FROM 
    (SELECT Account_No,Functional_Area,
                
        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV01","SV02","SV60","SV61") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV01","SV02","SV60","SV61")THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IG_PURCHASE_AMOUNT1,

        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006341200","0006342100","0006342050","0006342120") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006341200","0006342100","0006342050","0006342120") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IG_PURCHASE_AMOUNT2,

        CASE WHEN {fcb_dynamic_view_security_function_name}.UOM="KG" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV01","SV02","SV60","SV61") THEN SUM(Weight_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="L15" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV01","SV02","SV60","SV61")THEN SUM(Volume_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="GAL" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV01","SV02","SV60","SV61") THEN SUM(Volume_Qty_GAL)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="BBL" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV01","SV02","SV60","SV61") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IG_PURCHASE_QTY1,

        CASE WHEN {fcb_dynamic_view_security_function_name}.UOM="KG" AND Account_No IN ("0006342100","0006342050","0006342120") THEN SUM(Weight_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="L15" AND Account_No IN ("0006342100","0006342050","0006342120") THEN SUM(Volume_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="GAL" AND Account_No IN ("0006342100","0006342050","0006342120") THEN SUM(Volume_Qty_GAL)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="BBL" AND Account_No IN ("0006342100","0006342050","0006342120") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IG_PURCHASE_QTY2

        from temp 
        group by Account_No,Functional_Area
    )

    UNION 

    SELECT  "Hedge Paper" as line_item, "Acquisition Costs" as line_item_header, COALESCE(sum(HEDGE_PAPER),0) as R3_Amount, 0 as R3_Quantity

    FROM 
    (SELECT Account_No,Functional_Area,
                
        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006920600","0006920650","0006920675","0006920680") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006920600","0006920650","0006920675","0006920680") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS HEDGE_PAPER

        from temp 
        group by Account_No,Functional_Area
    )

    UNION 

    SELECT  "Demurrage" as line_item, "Acquisition Costs" as line_item_header, COALESCE(sum(DEMURRAGE),0) as R3_Amount, 0 as R3_Quantity

    FROM 
    (SELECT Account_No,Functional_Area,
                
        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006301300","0006301310","0006301320") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006301300","0006301310","0006301320") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS DEMURRAGE

        from temp 
        group by Account_No,Functional_Area
    )

    UNION 

    SELECT  "Excise Duty" as line_item, "Acquisition Costs" as line_item_header, COALESCE(sum(EXCISE_DUTY),0) as R3_Amount, 0 as R3_Quantity

    FROM 
    (SELECT Account_No,Functional_Area,
                
        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006210010","0006210015","0006210020","0006210060","0006211200","0006211201","0006211202","0006211210","0006211300","0006210310","0006211400","0006211500","0006342410") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006210010","0006210015","0006210020","0006210060","0006211200","0006211201","0006211202","0006211210","0006211300","0006210310","0006211400","0006211500","0006342410") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS EXCISE_DUTY

        from temp 
        group by Account_No,Functional_Area
    )

    UNION 

    SELECT  "CSO" as line_item, "Acquisition Costs" as line_item_header, COALESCE(sum(CSO),0) as R3_Amount, 0 as R3_Quantity

    FROM 
    (SELECT Account_No,Functional_Area,
                
        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006355100","0006355600","0007240550") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006355100","0006355600","0007240550") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS CSO

        from temp 
        group by Account_No,Functional_Area
    )

    UNION 

    SELECT  "Stock Adjustments" as line_item, "Acquisition Costs" as line_item_header, COALESCE(sum(STOCK_ADJUSTMENT),0) as R3_Amount,COALESCE(sum(STOCK_ADJUSTMENT_QTY),0)*(-1) as R3_Quantity

    FROM 
    (SELECT Account_No,Functional_Area,
                
        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006351400","0006351405","0006351410","0006351415","0006351600","0006351605","0006351610","0006351615") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006351400","0006351405","0006351410","0006351415","0006351600","0006351605","0006351610","0006351615") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS STOCK_ADJUSTMENT,

        CASE WHEN {fcb_dynamic_view_security_function_name}.UOM="KG" AND Account_No IN ("0006351400","0006351405","0006351410","0006351415","0006351600","0006351605","0006351610","0006351615") THEN SUM(Weight_Qty)
            WHEN {fcb_dynamic_view_security_function_name}.UOM="L15" AND Account_No IN ("0006351400","0006351405","0006351410","0006351415","0006351600","0006351605","0006351610","0006351615") THEN SUM(Volume_Qty)
            WHEN {fcb_dynamic_view_security_function_name}.UOM="GAL" AND Account_No IN ("0006351400","0006351405","0006351410","0006351415","0006351600","0006351605","0006351610","0006351615") THEN SUM(Volume_Qty_GAL)
            WHEN {fcb_dynamic_view_security_function_name}.UOM="BBL" AND Account_No IN ("0006351400","0006351405","0006351410","0006351415","0006351600","0006351605","0006351610","0006351615") THEN SUM(Volume_Qty_BB6) 
            ELSE "" END AS STOCK_ADJUSTMENT_QTY

        from temp 
        group by Account_No,Functional_Area
    )

    UNION 

    SELECT  "Other Costs" as line_item, "Acquisition Costs" as line_item_header, COALESCE(sum(OTHER_COSTS1),0)+COALESCE(sum(OTHER_COSTS2),0) as R3_Amount,0 as R3_Quantity

    FROM 
    (SELECT Account_No,Functional_Area,
                
        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006211530","0006340410","0006340510","0006340520","0006341150","0006342250","0006342400","0006342405","0006342420","0006344000","0006350210","0006352030","0006354100","0006355630","0006390200","0006390210","0006390220","0006390230","0006390240","0006390250","0006390260","0006390300", "0006820200","0006820250","0007240700","0007492250","0008381500","0008381505","0008381510","0008381515","0008383500","0008383510","0008750200","0009800206","0009800207","0009800208")  THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006211530","0006340410","0006340510","0006340520","0006341150","0006342250","0006342400","0006342405","0006342420","0006344000","0006350210","0006352030","0006354100","0006355630","0006390200","0006390210","0006390220","0006390230","0006390240","0006390250","0006390260","0006390300", "0006820200","0006820250","0007240700","0007492250","0008381500","0008381505","0008381510","0008381515","0008383500","0008383510","0008750200","0009800206","0009800207","0009800208") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS OTHER_COSTS1,

        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Functional_Area IN ("OP57") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Functional_Area IN ("OP57") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS OTHER_COSTS2

        from temp 
        group by Account_No,Functional_Area
    )
    UNION 

    SELECT  "IS Purchases" as line_item, "Transfers" as line_item_header, COALESCE(sum(IS_PURCHASES1),0)+COALESCE(sum(IS_PURCHASES2),0)+COALESCE(sum(IS_PURCHASES3),0)+COALESCE(sum(IS_PURCHASES4),0)+COALESCE(sum(IS_PURCHASES5),0)+COALESCE(sum(IS_PURCHASES6),0) as R3_Amount, COALESCE(sum(IS_PURCHASES_QTY1),0)+COALESCE(sum(IS_PURCHASES_QTY2),0)+COALESCE(sum(IS_PURCHASES_QTY3),0)+COALESCE(sum(IS_PURCHASES_QTY4),0)+COALESCE(sum(IS_PURCHASES_QTY5),0) as R3_Quantity

    FROM 
    (SELECT Account_No,Functional_Area,

        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006380200","0006380201","0006380306","0006390100","0006390400") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006380200","0006380201","0006380306","0006390100","0006390400") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IS_PURCHASES1,

        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No = "0006355595" AND Functional_Area IN (" ","SV44","SV45","SV48","SV49") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No = "0006355595" AND Functional_Area IN (" ","SV44","SV45","SV48","SV49") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IS_PURCHASES2,

        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No = "0006380202" AND Functional_Area IN ("SV44","SV45","SV48","SV56","SV58"," ") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No = "0006380202" AND Functional_Area IN ("SV44","SV45","SV48","SV56","SV58"," ") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IS_PURCHASES3,

        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No = "0006380203" AND Functional_Area IN ("SV03","SV48","SV49","SV53","SV55"," ") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No = "0006380203" AND Functional_Area IN ("SV03","SV48","SV49","SV53","SV55"," ") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IS_PURCHASES4,

        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No = "0006380302" AND Functional_Area IN ("SV33","SV57") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No = "0006380302" AND Functional_Area IN ("SV33","SV57") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IS_PURCHASES5,

        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No = "0006380303" AND Functional_Area IN ("SV54") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No = "0006380303" AND Functional_Area IN ("SV54") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IS_PURCHASES6,

                CASE WHEN {fcb_dynamic_view_security_function_name}.UOM="KG" AND Account_No ="0006380201" THEN SUM(Weight_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="L15" AND Account_No ="0006380201" THEN SUM(Volume_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="GAL" AND Account_No ="0006380201" THEN SUM(Volume_Qty_GAL)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="BBL" AND Account_No ="0006380201" THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IS_PURCHASES_QTY1,

        CASE WHEN {fcb_dynamic_view_security_function_name}.UOM="KG" AND Account_No = "0006380202" AND Functional_Area IN ("SV44","SV45","SV48","SV56","SV58","") THEN SUM(Weight_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="L15" AND Account_No = "0006380202" AND Functional_Area IN ("SV44","SV45","SV48","SV56","SV58","")THEN SUM(Volume_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="GAL" AND Account_No = "0006380202" AND Functional_Area IN ("SV44","SV45","SV48","SV56","SV58","") THEN SUM(Volume_Qty_GAL)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="BBL" AND Account_No = "0006380202" AND Functional_Area IN ("SV44","SV45","SV48","SV56","SV58","") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IS_PURCHASES_QTY2,

        CASE WHEN {fcb_dynamic_view_security_function_name}.UOM="KG" AND Account_No = "0006380203" AND Functional_Area IN ("SV03","SV48","SV49","SV53","SV55","") THEN SUM(Weight_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="L15" AND Account_No = "0006380203" AND Functional_Area IN ("SV03","SV48","SV49","SV53","SV55","")THEN SUM(Volume_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="GAL" AND Account_No = "0006380203" AND Functional_Area IN ("SV03","SV48","SV49","SV53","SV55","") THEN SUM(Volume_Qty_GAL)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="BBL" AND Account_No = "0006380203" AND Functional_Area IN ("SV03","SV48","SV49","SV53","SV55","") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IS_PURCHASES_QTY3,

        CASE WHEN {fcb_dynamic_view_security_function_name}.UOM="KG" AND Account_No = "0006380302" AND Functional_Area IN ("SV33","SV57") THEN SUM(Weight_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="L15" AND Account_No = "0006380302" AND Functional_Area IN ("SV33","SV57")THEN SUM(Volume_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="GAL" AND Account_No = "0006380302" AND Functional_Area IN ("SV33","SV57") THEN SUM(Volume_Qty_GAL)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="BBL" AND Account_No = "0006380302" AND Functional_Area IN ("SV33","SV57") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IS_PURCHASES_QTY4,

        CASE WHEN {fcb_dynamic_view_security_function_name}.UOM="KG" AND Account_No = "0006380303" AND Functional_Area="SV54" THEN SUM(Weight_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="L15" AND Account_No = "0006380303" AND Functional_Area ="SV54" THEN SUM(Volume_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="GAL" AND Account_No = "0006380303" AND Functional_Area ="SV54" THEN SUM(Volume_Qty_GAL)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="BBL" AND Account_No = "0006380303" AND Functional_Area ="SV54" THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IS_PURCHASES_QTY5

        from temp 
        group by Account_No,Functional_Area
    )
    UNION 

    SELECT  "Internal SC-(From)" as line_item, "Transfers" as line_item_header, COALESCE(sum(INTERNAL_SC_FROM)*(-1),0) as R3_Amount,COALESCE(sum(INTERNAL_SC_FROM_QTY)*(-1),0) as R3_Quantity

    FROM 
    (SELECT Account_No,Functional_Area,
                
        CASE WHEN {fcb_dynamic_view_security_function_name}.UOM="KG" AND Account_No = "0006380203" AND Functional_Area ="SV30" THEN SUM(Weight_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="L15" AND Account_No = "0006380203" AND Functional_Area ="SV30"THEN SUM(Volume_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="GAL" AND Account_No = "0006380203" AND Functional_Area ="SV30" THEN SUM(Volume_Qty_GAL)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="BBL" AND Account_No = "0006380203" AND Functional_Area ="SV30" THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS INTERNAL_SC_FROM_QTY,

        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No = "0006380203" AND Functional_Area = "SV30" THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No = "0006380203" AND Functional_Area = "SV30" THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS INTERNAL_SC_FROM

        from temp 
        group by Account_No,Functional_Area
    )
    UNION 

    SELECT  "Internal SC-(To)" as line_item, "Transfers" as line_item_header, COALESCE(sum(INTERNAL_SC_TO),0) as R3_Amount,COALESCE(sum(INTERNAL_SC_TO_QTY)*(-1),0) as R3_Quantity

    FROM 
    (SELECT Account_No,Functional_Area,
                
        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No = "0006380303" AND Functional_Area = "SV31" THEN SUM(Amt_Comp_Code_Curr)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No = "0006380303" AND Functional_Area = "SV31" THEN SUM(Amt_PC_Local_Curr)
                    ELSE "" END AS INTERNAL_SC_TO,

        CASE WHEN {fcb_dynamic_view_security_function_name}.UOM="KG" AND Account_No = "0006380303" AND Functional_Area ="SV31" THEN SUM(Weight_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="L15" AND Account_No = "0006380303" AND Functional_Area ="SV31"THEN SUM(Volume_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="GAL" AND Account_No = "0006380303" AND Functional_Area ="SV31" THEN SUM(Volume_Qty_GAL)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="BBL" AND Account_No = "0006380303" AND Functional_Area ="SV31" THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS INTERNAL_SC_TO_QTY

        from temp 
        group by Account_No,Functional_Area
    )
    UNION 

    SELECT  "IS Sales" as line_item, "Transfers" as line_item_header, COALESCE(sum(IS_SALES1),0)+COALESCE(sum(IS_SALES2),0)+COALESCE(sum(IS_SALES3),0)+COALESCE(sum(IS_SALES4),0)+COALESCE(sum(IS_SALES5),0) as R3_Amount, COALESCE(sum(IS_SALES_QTY1),0)+COALESCE(sum(IS_SALES_QTY2),0)+COALESCE(sum(IS_SALES_QTY3),0)+COALESCE(sum(IS_SALES_QTY4),0) as R3_Quantity

    FROM 
    (SELECT Account_No,Functional_Area,
                
        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN("0006380100","0006380300","0006380301")  THEN SUM(Amt_Comp_Code_Curr)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN("0006380100","0006380300","0006380301") THEN SUM(Amt_PC_Local_Curr)
                    ELSE "" END AS IS_SALES1,

        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No = "0006355095" AND Functional_Area IN ("SV48") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No = "0006355095" AND Functional_Area IN ("SV48") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IS_SALES2,

        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN("0006355565","0006380302") AND Functional_Area IN ("SV34","SV47","") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN("0006355565","0006380302")AND Functional_Area IN ("SV34","SV47","") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IS_SALES3,

        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN("0006380202","0006380203") AND Functional_Area IN ("SV49") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN("0006380202","0006380203") AND Functional_Area IN ("SV49") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IS_SALES4,

        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No = "0006380303" AND Functional_Area IN ("SV03","SV32","SV47","") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No = "0006380303" AND Functional_Area IN ("SV03","SV32","SV47","") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IS_SALES5,

        CASE WHEN {fcb_dynamic_view_security_function_name}.UOM="KG" AND Account_No IN ("0006380100","0006380300") THEN SUM(Weight_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="L15" AND Account_No IN ("0006380100","0006380300") THEN SUM(Volume_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="GAL" AND Account_No IN ("0006380100","0006380300") THEN SUM(Volume_Qty_GAL)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="BBL" AND Account_No IN ("0006380100","0006380300") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IS_SALES_QTY1,

        CASE WHEN {fcb_dynamic_view_security_function_name}.UOM="KG" AND Account_No IN("0006380202","0006380203") AND Functional_Area="SV49" THEN SUM(Weight_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="L15" AND Account_No IN("0006380202","0006380203") AND Functional_Area ="SV49" THEN SUM(Volume_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="GAL" AND Account_No IN("0006380202","0006380203") AND Functional_Area ="SV49" THEN SUM(Volume_Qty_GAL)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="BBL" AND Account_No IN("0006380202","0006380203") AND Functional_Area ="SV49" THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IS_SALES_QTY2,

        CASE WHEN {fcb_dynamic_view_security_function_name}.UOM="KG" AND Account_No = "0006380303" AND Functional_Area IN ("SV03","SV32","SV47","") THEN SUM(Weight_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="L15" AND Account_No = "0006380303" AND Functional_Area IN ("SV03","SV32","SV47","")THEN SUM(Volume_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="GAL" AND Account_No = "0006380303" AND Functional_Area IN ("SV03","SV32","SV47","") THEN SUM(Volume_Qty_GAL)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="BBL" AND Account_No = "0006380303" AND Functional_Area IN ("SV03","SV32","SV47","") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IS_SALES_QTY3,

        CASE WHEN {fcb_dynamic_view_security_function_name}.UOM="KG" AND Account_No = "0006380302" AND Functional_Area IN ("SV34","SV47","") THEN SUM(Weight_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="L15" AND Account_No = "0006380302" AND Functional_Area IN ("SV34","SV47","")THEN SUM(Volume_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="GAL" AND Account_No = "0006380302" AND Functional_Area IN ("SV34","SV47","") THEN SUM(Volume_Qty_GAL)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="BBL" AND Account_No = "0006380302" AND Functional_Area IN ("SV34","SV47","") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IS_SALES_QTY4

        from temp 
        group by Account_No,Functional_Area
    )
    UNION
    SELECT  " " as line_item, "Acquisition Costs" as line_item_header, 0 as R3_Amount,COALESCE(sum(STOCK_ADJUSTMENT_QTY),0) as R3_Quantity

    FROM 
    (SELECT Account_No,Functional_Area,
                
        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006351400","0006351405","0006351410","0006351415","0006351600","0006351605","0006351610","0006351615") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006351400","0006351405","0006351410","0006351415","0006351600","0006351605","0006351610","0006351615") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS STOCK_ADJUSTMENT,

        CASE WHEN {fcb_dynamic_view_security_function_name}.UOM="KG" AND Account_No IN ("0006351400","0006351405","0006351410","0006351415","0006351600","0006351605","0006351610","0006351615") THEN SUM(Weight_Qty)
            WHEN {fcb_dynamic_view_security_function_name}.UOM="L15" AND Account_No IN ("0006351400","0006351405","0006351410","0006351415","0006351600","0006351605","0006351610","0006351615") THEN SUM(Volume_Qty)
            WHEN {fcb_dynamic_view_security_function_name}.UOM="GAL" AND Account_No IN ("0006351400","0006351405","0006351410","0006351415","0006351600","0006351605","0006351610","0006351615") THEN SUM(Volume_Qty_GAL)
            WHEN {fcb_dynamic_view_security_function_name}.UOM="BBL" AND Account_No IN ("0006351400","0006351405","0006351410","0006351415","0006351600","0006351605","0006351610","0006351615") THEN SUM(Volume_Qty_BB6) 
            ELSE "" END AS STOCK_ADJUSTMENT_QTY

        from temp 
        group by Account_No,Functional_Area
    ))""")
    
    assignFunctionPermission(uc_catalog_name, uc_curated_schema, fcb_dynamic_view_security_function_name, tbl_owner_grp,security_end_user_aad_group_name, security_object_aad_group_name, security_functional_dev_aad_group, security_functional_readers_aad_group)

    print("fcb_dynamic_view_security function dropped and recreated in the schema")
else:
    spark.sql(f"""CREATE OR REPLACE FUNCTION `{uc_catalog_name}`.`{uc_curated_schema}`.{fcb_dynamic_view_security_function_name} (`Company_Code-Text` STRING,`Profit_Center-Text` STRING, Fiscal_Year STRING, Posting_Period STRING, Division STRING, Account_No STRING, Document_Type STRING, `Plant-Text` STRING, CURRENCY STRING, UOM STRING , Display_Unit STRING) 
    RETURNS TABLE (line_item STRING, line_item_header STRING, R3_Amount decimal(25,2), R3_Quantity decimal(25,2)) 
    RETURN 
    with temp as ( 
    select Amt_Comp_Code_Curr, Amt_PC_Local_Curr, Weight_Qty, Volume_Qty,Volume_Qty_GAL, Volume_Qty_BB6,Account_No,Functional_Area,Posting_Period,Plant 
    from `{uc_catalog_name}`.`{uc_curated_schema}`.{fcb_dynamic_view_name} fcb 
    WHERE 
    (array_contains(SPLIT({fcb_dynamic_view_security_function_name}.`Company_Code-Text`,',') ,fcb.`Company_Code-Text`) OR ({fcb_dynamic_view_security_function_name}.`Company_Code-Text`='ALL')) AND 
    (array_contains(SPLIT({fcb_dynamic_view_security_function_name}.`Profit_Center-Text`,',') ,fcb.`Profit_Center-Text`) OR ({fcb_dynamic_view_security_function_name}.`Profit_Center-Text`='ALL')) AND 
    (array_contains(SPLIT({fcb_dynamic_view_security_function_name}.Fiscal_Year,',') ,fcb.Fiscal_Year) OR 
        ({fcb_dynamic_view_security_function_name}.Fiscal_Year='ALL')) AND 
    (array_contains(SPLIT({fcb_dynamic_view_security_function_name}.Posting_Period,',') ,fcb.Posting_Period) OR 
        ({fcb_dynamic_view_security_function_name}.Posting_Period='ALL')) AND 
    (array_contains(SPLIT({fcb_dynamic_view_security_function_name}.Division,',') ,fcb.Division) OR 
        ({fcb_dynamic_view_security_function_name}.Division='ALL')) AND 
    (array_contains(SPLIT({fcb_dynamic_view_security_function_name}.Account_No,',') ,fcb.Account_No) OR 
        ({fcb_dynamic_view_security_function_name}.Account_No='ALL')) AND 
    (array_contains(SPLIT({fcb_dynamic_view_security_function_name}.Document_Type,',') ,fcb.Document_Type) OR 
        ({fcb_dynamic_view_security_function_name}.Document_Type='ALL')) AND 
    (array_contains(SPLIT({fcb_dynamic_view_security_function_name}.`Plant-Text`,',') ,fcb.`Plant-Text`) OR 
    ({fcb_dynamic_view_security_function_name}.`Plant-Text`='ALL'))
    AND fcb.OPFLAG != 'D') 

    SELECT `line_item`, `line_item_header`, 
    CASE WHEN {fcb_dynamic_view_security_function_name}.Display_Unit = 'Actual Value' THEN `R3_Amount`
    WHEN {fcb_dynamic_view_security_function_name}.Display_Unit = 'Thousands' THEN `R3_Amount`*0.001
    WHEN {fcb_dynamic_view_security_function_name}.Display_Unit = 'Millions' THEN `R3_Amount`*0.000001
    WHEN {fcb_dynamic_view_security_function_name}.Display_Unit = 'Billions' THEN `R3_Amount`*0.000000001
    END
    AS `R3_Amount`,

    CASE WHEN {fcb_dynamic_view_security_function_name}.Display_Unit = 'Actual Value' THEN `R3_Quantity`
    WHEN {fcb_dynamic_view_security_function_name}.Display_Unit = 'Thousands' THEN `R3_Quantity`*0.001
    WHEN {fcb_dynamic_view_security_function_name}.Display_Unit = 'Millions' THEN `R3_Quantity`*0.000001
    WHEN {fcb_dynamic_view_security_function_name}.Display_Unit = 'Billions' THEN `R3_Quantity`*0.000000001
    END
    AS `R3_Quantity`

    FROM  
    
    (   SELECT "Inner LOB Sale" as line_item, "Sales Proceeds" as line_item_header, 
        COALESCE(sum(INTER_LOB_SALES1),0) + COALESCE(sum(INTER_LOB_SALES2),0) as R3_Amount, 
        sum(INTER_LOB_SALES_QTY) as R3_Quantity FROM 
    (SELECT Account_No,Functional_Area, \
        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006380302","0006380303","0006355565") AND Functional_Area IN ("SV46") THEN SUM(Amt_Comp_Code_Curr)*(-1) 
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006380302","0006380303","0006355565")AND Functional_Area IN ("SV46") THEN SUM(Amt_PC_Local_Curr)*(-1) 
                    ELSE "" END AS INTER_LOB_SALES1, 
        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006380305") THEN SUM(Amt_Comp_Code_Curr)*(-1) 
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006380305") THEN SUM(Amt_PC_Local_Curr)*(-1) 
                    ELSE "" END AS INTER_LOB_SALES2, 
            CASE WHEN {fcb_dynamic_view_security_function_name}.UOM="KG" AND Account_No IN ("0006380302","0006380303") AND Functional_Area ="SV46" THEN SUM(Weight_Qty) 
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="L15" AND Account_No IN ("0006380302","0006380303") AND Functional_Area ="SV46" THEN SUM(Volume_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="GAL" AND Account_No IN ("0006380302","0006380303") AND Functional_Area ="SV46" THEN SUM(Volume_Qty_GAL)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="BBL" AND Account_No IN ("0006380302","0006380303") AND Functional_Area ="SV46" THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS INTER_LOB_SALES_QTY
        from temp 
        group by Account_No,Functional_Area
    )
    UNION
    SELECT  "IG Sales" as line_item, "Sales Proceeds" as line_item_header, sum(IG_SALES) as R3_Amount, sum(IG_SALES_QTY) as R3_Quantity 
    FROM
    (SELECT Account_No,Functional_Area,

        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006001500","0006001700","0006001900","0006002100","0006002200","0006009002","0006110035","0006110135","0006110235","0006520350","0006520360") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006001500","0006001700","0006001900","0006002100","0006002200","0006009002","0006110035","0006110135","0006110235","0006520350","0006520360") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IG_SALES,

        CASE WHEN {fcb_dynamic_view_security_function_name}.UOM="KG" AND Account_No IN ("0006001500","0006001700","0006001900","0006002100") THEN SUM(Weight_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="L15" AND Account_No IN ("0006001500","0006001700","0006001900","0006002100") THEN SUM(Volume_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="GAL" AND Account_No IN ("0006001500","0006001700","0006001900","0006002100") THEN SUM(Volume_Qty_GAL)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="BBL" AND Account_No IN ("0006001500","0006001700","0006001900","0006002100") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IG_SALES_QTY

        from temp 
        group by Account_No,Functional_Area
    )

    UNION
    SELECT  "3rd Party Sales" as line_item, "Sales Proceeds" as line_item_header, sum(THIRD_PARTY_SALES) as R3_Amount, sum(THIRD_PARTY_SALES_QTY) as R3_Quantity

    FROM

    (SELECT Account_No,Functional_Area,
                
            CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006000100","0006000150","0006000151","0006000152","0006000153", "0006000200","0006000250","0006000300","0006000400","0006000500","0006000600","0006000650","0006000700","0006000800","0006002105","0006002205","0006002230","0006009000","0006009005","0006009010","0006009015","0006009050","0006009100","0006009800","0006009920","0006110010","0006110015","0006110020","0006110021","0006110030","0006110031","0006110110","0006110115","0006110120","0006110121","0006110130","0006110131","0006110210","0006110215","0006110220","0006110221","0006110230","0006110231","0006110300","0006110305","0006110310","0006110320","0006110330","0006110340","0006140100","0006140105","0006140200","0006140300","0006140500","0006140700","0006140800","0006340610","0006340620","0006340710","0006342300","0006342500","0006342550","0006920460","0006920461") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006000100","0006000150","0006000151","0006000152","0006000153", "0006000200","0006000250","0006000300","0006000400","0006000500","0006000600","0006000650","0006000700","0006000800","0006002105","0006002205","0006002230","0006009000","0006009005","0006009010","0006009015","0006009050","0006009100","0006009800","0006009920","0006110010","0006110015","0006110020","0006110021","0006110030","0006110031","0006110110","0006110115","0006110120","0006110121","0006110130","0006110131","0006110210","0006110215","0006110220","0006110221","0006110230","0006110231","0006110300","0006110305","0006110310","0006110320","0006110330","0006110340","0006140100","0006140105","0006140200","0006140300","0006140500","0006140700","0006140800","0006340610","0006340620","0006340710","0006342300","0006342500","0006342550","0006920460","0006920461") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS THIRD_PARTY_SALES,

            CASE WHEN {fcb_dynamic_view_security_function_name}.UOM="KG" AND Account_No IN ("0006000100","0006000150","0006000200","0006000250","0006000300","0006000400","0006000500","0006000600","0006002105","0006009000","0006009050","0006009100","0006140100","0006140105","0006140200","0006140300","0006140500","0006140700","0006342550") THEN SUM(Weight_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="L15" AND Account_No IN ("0006000100","0006000150","0006000200","0006000250","0006000300","0006000400","0006000500","0006000600","0006002105","0006009000","0006009050","0006009100","0006140100","0006140105","0006140200","0006140300","0006140500","0006140700","0006342550") THEN SUM(Volume_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="GAL" AND Account_No IN ("0006000100","0006000150","0006000200","0006000250","0006000300","0006000400","0006000500","0006000600","0006002105","0006009000","0006009050","0006009100","0006140100","0006140105","0006140200","0006140300","0006140500","0006140700","0006342550") THEN SUM(Volume_Qty_GAL)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="BBL" AND Account_No IN ("0006000100","0006000150","0006000200","0006000250","0006000300","0006000400","0006000500","0006000600","0006002105","0006009000","0006009050","0006009100","0006140100","0006140105","0006140200","0006140300","0006140500","0006140700","0006342550") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS THIRD_PARTY_SALES_QTY

        from temp 
        group by Account_No,Functional_Area
    )
    UNION
    SELECT  "Proceeds from Services" as line_item, "Sales Proceeds" as line_item_header, sum(PROCEEDS_FROM_SERVICE) as R3_Amount, 0 as R3_Quantity

    FROM

    (SELECT Account_No,Functional_Area,
                
        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006140025","0006520050","0006520100","0006520300","0006520310","0006520400","0006590100","0006610100","0006610200","0006610300","0006610400","0006900200","0006900205","0006900250","0006900400","0007220560") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006140025","0006520050","0006520100","0006520300","0006520310","0006520400","0006590100","0006610100","0006610200","0006610300","0006610400","0006900200","0006900205","0006900250","0006900400","0007220560") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS PROCEEDS_FROM_SERVICE

        from temp 
        group by Account_No,Functional_Area
    )

    UNION
    SELECT  "EXG Sales" as line_item, "Exchanges" as line_item_header, sum(EXG_SALES) as R3_Amount, sum(EXG_SALES_QTY) as R3_Quantity

    FROM

    (SELECT Account_No,Functional_Area,
                
        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006002220","0006350510","0006190200") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006002220","0006350510","0006190200") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS EXG_SALES,

        CASE WHEN {fcb_dynamic_view_security_function_name}.UOM="KG" AND Account_No ="0006002220" THEN SUM(Weight_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="L15" AND Account_No ="0006002220" THEN SUM(Volume_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="GAL" AND Account_No ="0006002220" THEN SUM(Volume_Qty_GAL)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="BBL" AND Account_No ="0006002220" THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS EXG_SALES_QTY

        from temp 
        group by Account_No,Functional_Area
    )
    UNION

    SELECT  "EXG Purchases" as line_item, "Exchanges" as line_item_header,  COALESCE(sum(EXG_PURCHASES1),0)+ COALESCE(sum(EXG_PURCHASES2),0) as R3_Amount, sum(EXG_PURCHASE_QTY) as R3_Quantity

    FROM

    (SELECT Account_No,Functional_Area,
                
        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006342000","0006340010") AND Functional_Area IN ("SV10","SV11","SV12","SV50","SV51") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006342000","0006340010")AND Functional_Area IN ("SV10","SV11","SV12","SV50","SV51") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS EXG_PURCHASES1,

        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0008399100","0006350520") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0008399100","0006350520") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS EXG_PURCHASES2,

        CASE WHEN {fcb_dynamic_view_security_function_name}.UOM="KG" AND Account_No IN ("0006342000","0006340010") AND Functional_Area IN ("SV10","SV11","SV12","SV50","SV51") THEN SUM(Weight_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="L15" AND Account_No IN ("0006342000","0006340010") AND Functional_Area IN ("SV10","SV11","SV12","SV50","SV51")THEN SUM(Volume_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="GAL" AND Account_No IN ("0006342000","0006340010") AND Functional_Area IN ("SV10","SV11","SV12","SV50","SV51") THEN SUM(Volume_Qty_GAL)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="BBL" AND Account_No IN ("0006342000","0006340010") AND Functional_Area IN ("SV10","SV11","SV12","SV50","SV51") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS EXG_PURCHASE_QTY

        from temp 
        group by Account_No,Functional_Area
    )

    UNION 

    SELECT  "3rd Party Purchases" as line_item, "Acquisition Costs" as line_item_header, COALESCE(sum(THIRD_PARTY_PURCHASES_CURRENCY1),0)+ COALESCE(sum(THIRD_PARTY_PURCHASES_CURRENCY2),0) as R3_Amount, COALESCE(sum(THIRD_PARTY_PURCHASES_UOM1),0)+ COALESCE(sum(THIRD_PARTY_PURCHASES_UOM2),0) as R3_Quantity

    FROM 
    (SELECT Account_No,Functional_Area,
                
        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV05","SV06") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV05","SV06") THEN SUM(Amt_PC_Local_Curr)*(-1)
                ELSE "" END AS THIRD_PARTY_PURCHASES_CURRENCY1,

        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006330140","0006340020","0006342110","0006342130","0006342150","0006342200","0006342510","0006342520","0006342530","0006351070","0006351800","0006354200","0006900470","0007250210","0006341140","0006341141","0006341142","0006341143","0006365000") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006330140","0006340020","0006342110","0006342130","0006342150","0006342200","0006342510","0006342520","0006342530","0006351070","0006351800","0006354200","0006900470","0007250210","0006341140","0006341141","0006341142","0006341143","0006365000") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS THIRD_PARTY_PURCHASES_CURRENCY2,

        CASE WHEN {fcb_dynamic_view_security_function_name}.UOM="KG" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV05","SV06") THEN SUM(Weight_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="L15" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV05","SV06")THEN SUM(Volume_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="GAL" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV05","SV06") THEN SUM(Volume_Qty_GAL)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="BBL" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV05","SV06") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS THIRD_PARTY_PURCHASES_UOM1,

        CASE WHEN {fcb_dynamic_view_security_function_name}.UOM="KG" AND Account_No IN ("0006330140","0006340020","0006342110","0006342130","0006342150","0006342200","0006342510","0006342520","0006342530","0006351070","0006351800","0006354200","0006900470","0007250210","0006341140","0006341141","0006341142","0006341143","0006365000") THEN SUM(Weight_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="L15" AND Account_No IN ("0006330140","0006340020","0006342110","0006342130","0006342150","0006342200","0006342510","0006342520","0006342530","0006351070","0006351800","0006354200","0006900470","0007250210","0006341140","0006341141","0006341142","0006341143","0006365000") THEN SUM(Volume_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="GAL" AND Account_No IN ("0006330140","0006340020","0006342110","0006342130","0006342150","0006342200","0006342510","0006342520","0006342530","0006351070","0006351800","0006354200","0006900470","0007250210","0006341140","0006341141","0006341142","0006341143","0006365000") THEN SUM(Volume_Qty_GAL)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="BBL" AND Account_No IN ("0006330140","0006340020","0006342110","0006342130","0006342150","0006342200","0006342510","0006342520","0006342530","0006351070","0006351800","0006354200","0006900470","0007250210","0006341140","0006341141","0006341142","0006341143","0006365000") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS THIRD_PARTY_PURCHASES_UOM2

        from temp 
        group by Account_No,Functional_Area
    )

    UNION 

    SELECT  "IG Purchases" as line_item, "Acquisition Costs" as line_item_header, COALESCE(sum(IG_PURCHASE_AMOUNT1),0)+ COALESCE(sum(IG_PURCHASE_AMOUNT2),0) as R3_Amount, COALESCE(sum(IG_PURCHASE_QTY1),0)+ COALESCE(sum(IG_PURCHASE_QTY2),0) as R3_Quantity

    FROM 
    (SELECT Account_No,Functional_Area,
                
        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV01","SV02","SV60","SV61") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV01","SV02","SV60","SV61")THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IG_PURCHASE_AMOUNT1,

        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006341200","0006342100","0006342050","0006342120") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006341200","0006342100","0006342050","0006342120") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IG_PURCHASE_AMOUNT2,

        CASE WHEN {fcb_dynamic_view_security_function_name}.UOM="KG" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV01","SV02","SV60","SV61") THEN SUM(Weight_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="L15" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV01","SV02","SV60","SV61")THEN SUM(Volume_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="GAL" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV01","SV02","SV60","SV61") THEN SUM(Volume_Qty_GAL)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="BBL" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV01","SV02","SV60","SV61") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IG_PURCHASE_QTY1,

        CASE WHEN {fcb_dynamic_view_security_function_name}.UOM="KG" AND Account_No IN ("0006342100","0006342050","0006342120") THEN SUM(Weight_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="L15" AND Account_No IN ("0006342100","0006342050","0006342120") THEN SUM(Volume_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="GAL" AND Account_No IN ("0006342100","0006342050","0006342120") THEN SUM(Volume_Qty_GAL)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="BBL" AND Account_No IN ("0006342100","0006342050","0006342120") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IG_PURCHASE_QTY2

        from temp 
        group by Account_No,Functional_Area
    )

    UNION 

    SELECT  "Hedge Paper" as line_item, "Acquisition Costs" as line_item_header, COALESCE(sum(HEDGE_PAPER),0) as R3_Amount, 0 as R3_Quantity

    FROM 
    (SELECT Account_No,Functional_Area,
                
        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006920600","0006920650","0006920675","0006920680") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006920600","0006920650","0006920675","0006920680") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS HEDGE_PAPER

        from temp 
        group by Account_No,Functional_Area
    )

    UNION 

    SELECT  "Demurrage" as line_item, "Acquisition Costs" as line_item_header, COALESCE(sum(DEMURRAGE),0) as R3_Amount, 0 as R3_Quantity

    FROM 
    (SELECT Account_No,Functional_Area,
                
        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006301300","0006301310","0006301320") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006301300","0006301310","0006301320") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS DEMURRAGE

        from temp 
        group by Account_No,Functional_Area
    )

    UNION 

    SELECT  "Excise Duty" as line_item, "Acquisition Costs" as line_item_header, COALESCE(sum(EXCISE_DUTY),0) as R3_Amount, 0 as R3_Quantity

    FROM 
    (SELECT Account_No,Functional_Area,
                
        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006210010","0006210015","0006210020","0006210060","0006211200","0006211201","0006211202","0006211210","0006211300","0006210310","0006211400","0006211500","0006342410") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006210010","0006210015","0006210020","0006210060","0006211200","0006211201","0006211202","0006211210","0006211300","0006210310","0006211400","0006211500","0006342410") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS EXCISE_DUTY

        from temp 
        group by Account_No,Functional_Area
    )

    UNION 

    SELECT  "CSO" as line_item, "Acquisition Costs" as line_item_header, COALESCE(sum(CSO),0) as R3_Amount, 0 as R3_Quantity

    FROM 
    (SELECT Account_No,Functional_Area,
                
        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006355100","0006355600","0007240550") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006355100","0006355600","0007240550") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS CSO

        from temp 
        group by Account_No,Functional_Area
    )

    UNION 

    SELECT  "Stock Adjustments" as line_item, "Acquisition Costs" as line_item_header, COALESCE(sum(STOCK_ADJUSTMENT),0) as R3_Amount,COALESCE(sum(STOCK_ADJUSTMENT_QTY),0)*(-1) as R3_Quantity

    FROM 
    (SELECT Account_No,Functional_Area,
                
        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006351400","0006351405","0006351410","0006351415","0006351600","0006351605","0006351610","0006351615") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006351400","0006351405","0006351410","0006351415","0006351600","0006351605","0006351610","0006351615") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS STOCK_ADJUSTMENT,

        CASE WHEN {fcb_dynamic_view_security_function_name}.UOM="KG" AND Account_No IN ("0006351400","0006351405","0006351410","0006351415","0006351600","0006351605","0006351610","0006351615") THEN SUM(Weight_Qty)
            WHEN {fcb_dynamic_view_security_function_name}.UOM="L15" AND Account_No IN ("0006351400","0006351405","0006351410","0006351415","0006351600","0006351605","0006351610","0006351615") THEN SUM(Volume_Qty)
            WHEN {fcb_dynamic_view_security_function_name}.UOM="GAL" AND Account_No IN ("0006351400","0006351405","0006351410","0006351415","0006351600","0006351605","0006351610","0006351615") THEN SUM(Volume_Qty_GAL)
            WHEN {fcb_dynamic_view_security_function_name}.UOM="BBL" AND Account_No IN ("0006351400","0006351405","0006351410","0006351415","0006351600","0006351605","0006351610","0006351615") THEN SUM(Volume_Qty_BB6) 
            ELSE "" END AS STOCK_ADJUSTMENT_QTY

        from temp 
        group by Account_No,Functional_Area
    )

    UNION 

    SELECT  "Other Costs" as line_item, "Acquisition Costs" as line_item_header, COALESCE(sum(OTHER_COSTS1),0)+COALESCE(sum(OTHER_COSTS2),0) as R3_Amount,0 as R3_Quantity

    FROM 
    (SELECT Account_No,Functional_Area,
                
        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006211530","0006340410","0006340510","0006340520","0006341150","0006342250","0006342400","0006342405","0006342420","0006344000","0006350210","0006352030","0006354100","0006355630","0006390200","0006390210","0006390220","0006390230","0006390240","0006390250","0006390260","0006390300", "0006820200","0006820250","0007240700","0007492250","0008381500","0008381505","0008381510","0008381515","0008383500","0008383510","0008750200","0009800206","0009800207","0009800208")  THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006211530","0006340410","0006340510","0006340520","0006341150","0006342250","0006342400","0006342405","0006342420","0006344000","0006350210","0006352030","0006354100","0006355630","0006390200","0006390210","0006390220","0006390230","0006390240","0006390250","0006390260","0006390300", "0006820200","0006820250","0007240700","0007492250","0008381500","0008381505","0008381510","0008381515","0008383500","0008383510","0008750200","0009800206","0009800207","0009800208") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS OTHER_COSTS1,

        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Functional_Area IN ("OP57") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Functional_Area IN ("OP57") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS OTHER_COSTS2

        from temp 
        group by Account_No,Functional_Area
    )
    UNION 

    SELECT  "IS Purchases" as line_item, "Transfers" as line_item_header, COALESCE(sum(IS_PURCHASES1),0)+COALESCE(sum(IS_PURCHASES2),0)+COALESCE(sum(IS_PURCHASES3),0)+COALESCE(sum(IS_PURCHASES4),0)+COALESCE(sum(IS_PURCHASES5),0)+COALESCE(sum(IS_PURCHASES6),0) as R3_Amount, COALESCE(sum(IS_PURCHASES_QTY1),0)+COALESCE(sum(IS_PURCHASES_QTY2),0)+COALESCE(sum(IS_PURCHASES_QTY3),0)+COALESCE(sum(IS_PURCHASES_QTY4),0)+COALESCE(sum(IS_PURCHASES_QTY5),0) as R3_Quantity

    FROM 
    (SELECT Account_No,Functional_Area,

        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006380200","0006380201","0006380306","0006390100","0006390400") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006380200","0006380201","0006380306","0006390100","0006390400") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IS_PURCHASES1,

        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No = "0006355595" AND Functional_Area IN (" ","SV44","SV45","SV48","SV49") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No = "0006355595" AND Functional_Area IN (" ","SV44","SV45","SV48","SV49") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IS_PURCHASES2,

        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No = "0006380202" AND Functional_Area IN ("SV44","SV45","SV48","SV56","SV58"," ") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No = "0006380202" AND Functional_Area IN ("SV44","SV45","SV48","SV56","SV58"," ") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IS_PURCHASES3,

        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No = "0006380203" AND Functional_Area IN ("SV03","SV48","SV49","SV53","SV55"," ") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No = "0006380203" AND Functional_Area IN ("SV03","SV48","SV49","SV53","SV55"," ") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IS_PURCHASES4,

        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No = "0006380302" AND Functional_Area IN ("SV33","SV57") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No = "0006380302" AND Functional_Area IN ("SV33","SV57") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IS_PURCHASES5,

        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No = "0006380303" AND Functional_Area IN ("SV54") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No = "0006380303" AND Functional_Area IN ("SV54") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IS_PURCHASES6,

                CASE WHEN {fcb_dynamic_view_security_function_name}.UOM="KG" AND Account_No ="0006380201" THEN SUM(Weight_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="L15" AND Account_No ="0006380201" THEN SUM(Volume_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="GAL" AND Account_No ="0006380201" THEN SUM(Volume_Qty_GAL)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="BBL" AND Account_No ="0006380201" THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IS_PURCHASES_QTY1,

        CASE WHEN {fcb_dynamic_view_security_function_name}.UOM="KG" AND Account_No = "0006380202" AND Functional_Area IN ("SV44","SV45","SV48","SV56","SV58","") THEN SUM(Weight_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="L15" AND Account_No = "0006380202" AND Functional_Area IN ("SV44","SV45","SV48","SV56","SV58","")THEN SUM(Volume_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="GAL" AND Account_No = "0006380202" AND Functional_Area IN ("SV44","SV45","SV48","SV56","SV58","") THEN SUM(Volume_Qty_GAL)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="BBL" AND Account_No = "0006380202" AND Functional_Area IN ("SV44","SV45","SV48","SV56","SV58","") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IS_PURCHASES_QTY2,

        CASE WHEN {fcb_dynamic_view_security_function_name}.UOM="KG" AND Account_No = "0006380203" AND Functional_Area IN ("SV03","SV48","SV49","SV53","SV55","") THEN SUM(Weight_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="L15" AND Account_No = "0006380203" AND Functional_Area IN ("SV03","SV48","SV49","SV53","SV55","")THEN SUM(Volume_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="GAL" AND Account_No = "0006380203" AND Functional_Area IN ("SV03","SV48","SV49","SV53","SV55","") THEN SUM(Volume_Qty_GAL)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="BBL" AND Account_No = "0006380203" AND Functional_Area IN ("SV03","SV48","SV49","SV53","SV55","") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IS_PURCHASES_QTY3,

        CASE WHEN {fcb_dynamic_view_security_function_name}.UOM="KG" AND Account_No = "0006380302" AND Functional_Area IN ("SV33","SV57") THEN SUM(Weight_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="L15" AND Account_No = "0006380302" AND Functional_Area IN ("SV33","SV57")THEN SUM(Volume_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="GAL" AND Account_No = "0006380302" AND Functional_Area IN ("SV33","SV57") THEN SUM(Volume_Qty_GAL)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="BBL" AND Account_No = "0006380302" AND Functional_Area IN ("SV33","SV57") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IS_PURCHASES_QTY4,

        CASE WHEN {fcb_dynamic_view_security_function_name}.UOM="KG" AND Account_No = "0006380303" AND Functional_Area="SV54" THEN SUM(Weight_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="L15" AND Account_No = "0006380303" AND Functional_Area ="SV54" THEN SUM(Volume_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="GAL" AND Account_No = "0006380303" AND Functional_Area ="SV54" THEN SUM(Volume_Qty_GAL)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="BBL" AND Account_No = "0006380303" AND Functional_Area ="SV54" THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IS_PURCHASES_QTY5

        from temp 
        group by Account_No,Functional_Area
    )
    UNION 

    SELECT  "Internal SC-(From)" as line_item, "Transfers" as line_item_header, COALESCE(sum(INTERNAL_SC_FROM)*(-1),0) as R3_Amount,COALESCE(sum(INTERNAL_SC_FROM_QTY)*(-1),0) as R3_Quantity

    FROM 
    (SELECT Account_No,Functional_Area,
                
        CASE WHEN {fcb_dynamic_view_security_function_name}.UOM="KG" AND Account_No = "0006380203" AND Functional_Area ="SV30" THEN SUM(Weight_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="L15" AND Account_No = "0006380203" AND Functional_Area ="SV30"THEN SUM(Volume_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="GAL" AND Account_No = "0006380203" AND Functional_Area ="SV30" THEN SUM(Volume_Qty_GAL)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="BBL" AND Account_No = "0006380203" AND Functional_Area ="SV30" THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS INTERNAL_SC_FROM_QTY,

        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No = "0006380203" AND Functional_Area = "SV30" THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No = "0006380203" AND Functional_Area = "SV30" THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS INTERNAL_SC_FROM

        from temp 
        group by Account_No,Functional_Area
    )
    UNION 

    SELECT  "Internal SC-(To)" as line_item, "Transfers" as line_item_header, COALESCE(sum(INTERNAL_SC_TO),0) as R3_Amount,COALESCE(sum(INTERNAL_SC_TO_QTY)*(-1),0) as R3_Quantity

    FROM 
    (SELECT Account_No,Functional_Area,
                
        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No = "0006380303" AND Functional_Area = "SV31" THEN SUM(Amt_Comp_Code_Curr)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No = "0006380303" AND Functional_Area = "SV31" THEN SUM(Amt_PC_Local_Curr)
                    ELSE "" END AS INTERNAL_SC_TO,

        CASE WHEN {fcb_dynamic_view_security_function_name}.UOM="KG" AND Account_No = "0006380303" AND Functional_Area ="SV31" THEN SUM(Weight_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="L15" AND Account_No = "0006380303" AND Functional_Area ="SV31"THEN SUM(Volume_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="GAL" AND Account_No = "0006380303" AND Functional_Area ="SV31" THEN SUM(Volume_Qty_GAL)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="BBL" AND Account_No = "0006380303" AND Functional_Area ="SV31" THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS INTERNAL_SC_TO_QTY

        from temp 
        group by Account_No,Functional_Area
    )
    UNION 

    SELECT  "IS Sales" as line_item, "Transfers" as line_item_header, COALESCE(sum(IS_SALES1),0)+COALESCE(sum(IS_SALES2),0)+COALESCE(sum(IS_SALES3),0)+COALESCE(sum(IS_SALES4),0)+COALESCE(sum(IS_SALES5),0) as R3_Amount, COALESCE(sum(IS_SALES_QTY1),0)+COALESCE(sum(IS_SALES_QTY2),0)+COALESCE(sum(IS_SALES_QTY3),0)+COALESCE(sum(IS_SALES_QTY4),0) as R3_Quantity

    FROM 
    (SELECT Account_No,Functional_Area,
                
        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN("0006380100","0006380300","0006380301")  THEN SUM(Amt_Comp_Code_Curr)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN("0006380100","0006380300","0006380301") THEN SUM(Amt_PC_Local_Curr)
                    ELSE "" END AS IS_SALES1,

        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No = "0006355095" AND Functional_Area IN ("SV48") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No = "0006355095" AND Functional_Area IN ("SV48") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IS_SALES2,

        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN("0006355565","0006380302") AND Functional_Area IN ("SV34","SV47","") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN("0006355565","0006380302")AND Functional_Area IN ("SV34","SV47","") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IS_SALES3,

        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN("0006380202","0006380203") AND Functional_Area IN ("SV49") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN("0006380202","0006380203") AND Functional_Area IN ("SV49") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IS_SALES4,

        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No = "0006380303" AND Functional_Area IN ("SV03","SV32","SV47","") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No = "0006380303" AND Functional_Area IN ("SV03","SV32","SV47","") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IS_SALES5,

        CASE WHEN {fcb_dynamic_view_security_function_name}.UOM="KG" AND Account_No IN ("0006380100","0006380300") THEN SUM(Weight_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="L15" AND Account_No IN ("0006380100","0006380300") THEN SUM(Volume_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="GAL" AND Account_No IN ("0006380100","0006380300") THEN SUM(Volume_Qty_GAL)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="BBL" AND Account_No IN ("0006380100","0006380300") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IS_SALES_QTY1,

        CASE WHEN {fcb_dynamic_view_security_function_name}.UOM="KG" AND Account_No IN("0006380202","0006380203") AND Functional_Area="SV49" THEN SUM(Weight_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="L15" AND Account_No IN("0006380202","0006380203") AND Functional_Area ="SV49" THEN SUM(Volume_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="GAL" AND Account_No IN("0006380202","0006380203") AND Functional_Area ="SV49" THEN SUM(Volume_Qty_GAL)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="BBL" AND Account_No IN("0006380202","0006380203") AND Functional_Area ="SV49" THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IS_SALES_QTY2,

        CASE WHEN {fcb_dynamic_view_security_function_name}.UOM="KG" AND Account_No = "0006380303" AND Functional_Area IN ("SV03","SV32","SV47","") THEN SUM(Weight_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="L15" AND Account_No = "0006380303" AND Functional_Area IN ("SV03","SV32","SV47","")THEN SUM(Volume_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="GAL" AND Account_No = "0006380303" AND Functional_Area IN ("SV03","SV32","SV47","") THEN SUM(Volume_Qty_GAL)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="BBL" AND Account_No = "0006380303" AND Functional_Area IN ("SV03","SV32","SV47","") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IS_SALES_QTY3,

        CASE WHEN {fcb_dynamic_view_security_function_name}.UOM="KG" AND Account_No = "0006380302" AND Functional_Area IN ("SV34","SV47","") THEN SUM(Weight_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="L15" AND Account_No = "0006380302" AND Functional_Area IN ("SV34","SV47","")THEN SUM(Volume_Qty)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="GAL" AND Account_No = "0006380302" AND Functional_Area IN ("SV34","SV47","") THEN SUM(Volume_Qty_GAL)
                    WHEN {fcb_dynamic_view_security_function_name}.UOM="BBL" AND Account_No = "0006380302" AND Functional_Area IN ("SV34","SV47","") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IS_SALES_QTY4

        from temp 
        group by Account_No,Functional_Area
    )
    UNION
    SELECT  " " as line_item, "Acquisition Costs" as line_item_header, 0 as R3_Amount,COALESCE(sum(STOCK_ADJUSTMENT_QTY),0) as R3_Quantity

    FROM 
    (SELECT Account_No,Functional_Area,
                
        CASE WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006351400","0006351405","0006351410","0006351415","0006351600","0006351605","0006351610","0006351615") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_dynamic_view_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006351400","0006351405","0006351410","0006351415","0006351600","0006351605","0006351610","0006351615") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS STOCK_ADJUSTMENT,

        CASE WHEN {fcb_dynamic_view_security_function_name}.UOM="KG" AND Account_No IN ("0006351400","0006351405","0006351410","0006351415","0006351600","0006351605","0006351610","0006351615") THEN SUM(Weight_Qty)
            WHEN {fcb_dynamic_view_security_function_name}.UOM="L15" AND Account_No IN ("0006351400","0006351405","0006351410","0006351415","0006351600","0006351605","0006351610","0006351615") THEN SUM(Volume_Qty)
            WHEN {fcb_dynamic_view_security_function_name}.UOM="GAL" AND Account_No IN ("0006351400","0006351405","0006351410","0006351415","0006351600","0006351605","0006351610","0006351615") THEN SUM(Volume_Qty_GAL)
            WHEN {fcb_dynamic_view_security_function_name}.UOM="BBL" AND Account_No IN ("0006351400","0006351405","0006351410","0006351415","0006351600","0006351605","0006351610","0006351615") THEN SUM(Volume_Qty_BB6) 
            ELSE "" END AS STOCK_ADJUSTMENT_QTY

        from temp 
        group by Account_No,Functional_Area
    ))""")
    
    assignFunctionPermission(uc_catalog_name, uc_curated_schema, fcb_dynamic_view_security_function_name, tbl_owner_grp,security_end_user_aad_group_name, security_object_aad_group_name, security_functional_dev_aad_group, security_functional_readers_aad_group)

# COMMAND ----------

fcb_price_effect_security_func_name = uc_catalog_name+"."+uc_curated_schema+"."+fcb_price_effect_security_function_name
if fcb_price_effect_security_func_name in function_list:
    spark.sql(f"""DROP FUNCTION `{uc_catalog_name}`.`{uc_curated_schema}`.{fcb_price_effect_security_function_name}""")
    spark.sql(f"""
    CREATE OR REPLACE FUNCTION `{uc_catalog_name}`.`{uc_curated_schema}`.{fcb_price_effect_security_function_name}(`Company_Code-Text` STRING,`Profit_Center-Text` STRING, Fiscal_Year STRING, Posting_Period STRING, Division STRING, Account_No STRING, Document_Type STRING, `Plant-Text` STRING, CURRENCY STRING, UOM STRING , Display_Unit STRING) 
    RETURNS TABLE (price_effect decimal(25,2)) 
    RETURN 
        with temp as ( 
        select Posting_Period,Material_Number,Plant, Account_No, Amt_Comp_Code_Curr,Amt_PC_Local_Curr, Volume_Qty_GAL, Weight_Qty, Volume_Qty, Volume_Qty_BB6 
        from `{uc_catalog_name}`.`{uc_curated_schema}`.{fcb_dynamic_view_name} fcb
        WHERE 
        (array_contains(SPLIT({fcb_price_effect_security_function_name}.`Company_Code-Text`,',') ,fcb.`Company_Code-Text`) OR ({fcb_price_effect_security_function_name}.`Company_Code-Text`='ALL')) AND 
        (array_contains(SPLIT({fcb_price_effect_security_function_name}.`Profit_Center-Text`,',') ,fcb.`Profit_Center-Text`) OR ({fcb_price_effect_security_function_name}.`Profit_Center-Text`='ALL')) AND 
        (array_contains(SPLIT({fcb_price_effect_security_function_name}.Fiscal_Year,',') ,fcb.Fiscal_Year) OR ({fcb_price_effect_security_function_name}.Fiscal_Year='ALL')) AND 
        (array_contains(SPLIT({fcb_price_effect_security_function_name}.Posting_Period,',') ,fcb.Posting_Period) OR ({fcb_price_effect_security_function_name}.Posting_Period='ALL')) AND 
        (array_contains(SPLIT({fcb_price_effect_security_function_name}.Division,',') ,fcb.Division) OR ({fcb_price_effect_security_function_name}.Division='ALL')) AND 
        (array_contains(SPLIT({fcb_price_effect_security_function_name}.Account_No,',') ,fcb.Account_No) OR ({fcb_price_effect_security_function_name}.Account_No='ALL')) AND 
        (array_contains(SPLIT({fcb_price_effect_security_function_name}.Document_Type,',') ,fcb.Document_Type) OR ({fcb_price_effect_security_function_name}.Document_Type='ALL')) AND 
        (array_contains(SPLIT({fcb_price_effect_security_function_name}.`Plant-Text`,',') ,fcb.`Plant-Text`) OR ({fcb_price_effect_security_function_name}.`Plant-Text`='ALL'))
        AND fcb.OPFLAG != 'D') 

    SELECT 
    CASE WHEN {fcb_price_effect_security_function_name}.Display_Unit = 'Actual Value' THEN `price_effect`
    WHEN {fcb_price_effect_security_function_name}.Display_Unit = 'Thousands' THEN `price_effect`*0.001
    WHEN {fcb_price_effect_security_function_name}.Display_Unit = 'Millions' THEN `price_effect`*0.000001
    WHEN {fcb_price_effect_security_function_name}.Display_Unit = 'Billions' THEN `price_effect`*0.000000001
    END
    AS `price_effect`
    
    FROM
    (
    SELECT SUM(PRICE_EFFECT_INTERMEDIATE) AS price_effect
    FROM 
        (SELECT Posting_Period, Material_Number, Plant,  
        CASE WHEN (ISNULL(CC1) OR ISNULL(UOM1) OR (UOM1=0)) AND  (ISNULL(CC2) OR ISNULL(UOM2) OR (UOM2=0)) THEN (-1*CC1)-CC2 
            WHEN ((ISNULL(UOM1) OR (UOM1=0)) AND (ISNOTNULL(CC1) AND CC1<>0)) THEN (-1*CC1)
            WHEN ((ISNULL(CC1) OR CC1=0)AND (ISNOTNULL(CC2) AND CC2<>0)) THEN 0
            WHEN (ISNULL(UOM2) OR UOM2=0 ) THEN (-1*CC2)
            ELSE (((CC2/UOM2)-(CC1/UOM1))*(-1*UOM2)) END 
        AS PRICE_EFFECT_INTERMEDIATE
        FROM
        (
            SELECT Posting_Period,Material_Number,Plant, SUM(CC1) AS CC1, SUM(CC2) AS CC2, SUM(UOM1) AS UOM1, SUM(UOM2) AS UOM2 
            FROM 
            (
                SELECT Posting_Period,Material_Number,Plant,Account_No, 
                CASE WHEN Account_No IN ("0006341010","0006341020","0006341030","0006341040","0006341050","0006341060") AND {fcb_price_effect_security_function_name}.CURRENCY="Local Currency" THEN SUM(Amt_Comp_Code_Curr)  
                WHEN Account_No IN ("0006341010","0006341020","0006341030","0006341040","0006341050","0006341060") AND {fcb_price_effect_security_function_name}.CURRENCY="Group Currency" THEN SUM(Amt_PC_Local_Curr)             
                ELSE 0 END AS CC1,

                CASE WHEN Account_No IN ("0006341010","0006341020","0006341030","0006341040","0006341050","0006341060") AND {fcb_price_effect_security_function_name}.UOM="KG" THEN SUM(Weight_Qty) 
                WHEN Account_No IN ("0006341010","0006341020","0006341030","0006341040","0006341050","0006341060") AND {fcb_price_effect_security_function_name}.UOM="L15" THEN SUM(Volume_Qty)  
                WHEN Account_No IN ("0006341010","0006341020","0006341030","0006341040","0006341050","0006341060") AND {fcb_price_effect_security_function_name}.UOM="GAL" THEN SUM(Volume_Qty_GAL)  
                WHEN Account_No IN ("0006341010","0006341020","0006341030","0006341040","0006341050","0006341060") AND {fcb_price_effect_security_function_name}.UOM="BBL" THEN SUM(Volume_Qty_BB6) 
                ELSE 0 END AS UOM1,

                CASE WHEN Account_No IN ("0006341810","0006341820","0006341830","0006341840","0006341850","0006341860") AND {fcb_price_effect_security_function_name}.CURRENCY="Local Currency" THEN SUM(Amt_Comp_Code_Curr)  
                WHEN Account_No IN ("0006341810","0006341820","0006341830","0006341840","0006341850","0006341860") AND {fcb_price_effect_security_function_name}.CURRENCY="Group Currency" THEN SUM(Amt_PC_Local_Curr)             
                ELSE 0 END AS CC2,

                CASE WHEN Account_No IN ("0006341810","0006341820","0006341830","0006341840","0006341850","0006341860") AND {fcb_price_effect_security_function_name}.UOM="KG" THEN SUM(Weight_Qty)
                WHEN Account_No IN ("0006341810","0006341820","0006341830","0006341840","0006341850","0006341860") AND {fcb_price_effect_security_function_name}.UOM="L15" THEN SUM(Volume_Qty) 
                WHEN Account_No IN ("0006341810","0006341820","0006341830","0006341840","0006341850","0006341860") AND {fcb_price_effect_security_function_name}.UOM="GAL" THEN SUM(Volume_Qty_GAL)  
                WHEN Account_No IN ("0006341810","0006341820","0006341830","0006341840","0006341850","0006341860") AND {fcb_price_effect_security_function_name}.UOM="BBL" THEN SUM(Volume_Qty_BB6)
                ELSE 0 END AS UOM2
                FROM temp
                GROUP BY Posting_Period,Material_Number,Plant,Account_No
            )
            
            GROUP BY Posting_Period,Material_Number,Plant)
        ))""")
    assignFunctionPermission(uc_catalog_name, uc_curated_schema, fcb_price_effect_security_function_name, tbl_owner_grp,security_end_user_aad_group_name, security_object_aad_group_name, security_functional_dev_aad_group, security_functional_readers_aad_group)

    print("fcb_price_effect_security function dropped and recreated in the schema")
else:
    spark.sql(f"""
    CREATE OR REPLACE FUNCTION `{uc_catalog_name}`.`{uc_curated_schema}`.{fcb_price_effect_security_function_name}(`Company_Code-Text` STRING,`Profit_Center-Text` STRING, Fiscal_Year STRING, Posting_Period STRING, Division STRING, Account_No STRING, Document_Type STRING, `Plant-Text` STRING, CURRENCY STRING, UOM STRING, Display_Unit STRING) 
    RETURNS TABLE (price_effect decimal(25,2)) 
    RETURN 
        with temp as ( 
        select Posting_Period,Material_Number,Plant, Account_No, Amt_Comp_Code_Curr,Amt_PC_Local_Curr, Volume_Qty_GAL, Weight_Qty, Volume_Qty, Volume_Qty_BB6 
        from `{uc_catalog_name}`.`{uc_curated_schema}`.{fcb_dynamic_view_name} fcb
        WHERE 
        (array_contains(SPLIT({fcb_price_effect_security_function_name}.`Company_Code-Text`,',') ,fcb.`Company_Code-Text`) OR ({fcb_price_effect_security_function_name}.`Company_Code-Text`='ALL')) AND 
        (array_contains(SPLIT({fcb_price_effect_security_function_name}.`Profit_Center-Text`,',') ,fcb.`Profit_Center-Text`) OR ({fcb_price_effect_security_function_name}.`Profit_Center-Text`='ALL')) AND 
        (array_contains(SPLIT({fcb_price_effect_security_function_name}.Fiscal_Year,',') ,fcb.Fiscal_Year) OR ({fcb_price_effect_security_function_name}.Fiscal_Year='ALL')) AND 
        (array_contains(SPLIT({fcb_price_effect_security_function_name}.Posting_Period,',') ,fcb.Posting_Period) OR ({fcb_price_effect_security_function_name}.Posting_Period='ALL')) AND 
        (array_contains(SPLIT({fcb_price_effect_security_function_name}.Division,',') ,fcb.Division) OR ({fcb_price_effect_security_function_name}.Division='ALL')) AND 
        (array_contains(SPLIT({fcb_price_effect_security_function_name}.Account_No,',') ,fcb.Account_No) OR ({fcb_price_effect_security_function_name}.Account_No='ALL')) AND 
        (array_contains(SPLIT({fcb_price_effect_security_function_name}.Document_Type,',') ,fcb.Document_Type) OR ({fcb_price_effect_security_function_name}.Document_Type='ALL')) AND 
        (array_contains(SPLIT({fcb_price_effect_security_function_name}.`Plant-Text`,',') ,fcb.`Plant-Text`) OR ({fcb_price_effect_security_function_name}.`Plant-Text`='ALL'))
        AND fcb.OPFLAG != 'D') 

    SELECT 
    CASE WHEN {fcb_price_effect_security_function_name}.Display_Unit = 'Actual Value' THEN `price_effect`
    WHEN {fcb_price_effect_security_function_name}.Display_Unit = 'Thousands' THEN `price_effect`*0.001
    WHEN {fcb_price_effect_security_function_name}.Display_Unit = 'Millions' THEN `price_effect`*0.000001
    WHEN {fcb_price_effect_security_function_name}.Display_Unit = 'Billions' THEN `price_effect`*0.000000001
    END
    AS `price_effect`
    FROM
    (
    SELECT SUM(PRICE_EFFECT_INTERMEDIATE) AS price_effect
    FROM 
        (SELECT Posting_Period, Material_Number, Plant,  
        CASE WHEN (ISNULL(CC1) OR ISNULL(UOM1) OR (UOM1=0)) AND  (ISNULL(CC2) OR ISNULL(UOM2) OR (UOM2=0)) THEN (-1*CC1)-CC2 
            WHEN ((ISNULL(UOM1) OR (UOM1=0)) AND (ISNOTNULL(CC1) AND CC1<>0)) THEN (-1*CC1)
            WHEN ((ISNULL(CC1) OR CC1=0)AND (ISNOTNULL(CC2) AND CC2<>0)) THEN 0
            WHEN (ISNULL(UOM2) OR UOM2=0 ) THEN (-1*CC2)
            ELSE (((CC2/UOM2)-(CC1/UOM1))*(-1*UOM2)) END 
        AS PRICE_EFFECT_INTERMEDIATE
        FROM
        (
            SELECT Posting_Period,Material_Number,Plant, SUM(CC1) AS CC1, SUM(CC2) AS CC2, SUM(UOM1) AS UOM1, SUM(UOM2) AS UOM2 
            FROM 
            (
                SELECT Posting_Period,Material_Number,Plant,Account_No, 
                CASE WHEN Account_No IN ("0006341010","0006341020","0006341030","0006341040","0006341050","0006341060") AND {fcb_price_effect_security_function_name}.CURRENCY="Local Currency" THEN SUM(Amt_Comp_Code_Curr)  
                WHEN Account_No IN ("0006341010","0006341020","0006341030","0006341040","0006341050","0006341060") AND {fcb_price_effect_security_function_name}.CURRENCY="Group Currency" THEN SUM(Amt_PC_Local_Curr)             
                ELSE 0 END AS CC1,

                CASE WHEN Account_No IN ("0006341010","0006341020","0006341030","0006341040","0006341050","0006341060") AND {fcb_price_effect_security_function_name}.UOM="KG" THEN SUM(Weight_Qty) 
                WHEN Account_No IN ("0006341010","0006341020","0006341030","0006341040","0006341050","0006341060") AND {fcb_price_effect_security_function_name}.UOM="L15" THEN SUM(Volume_Qty)  
                WHEN Account_No IN ("0006341010","0006341020","0006341030","0006341040","0006341050","0006341060") AND {fcb_price_effect_security_function_name}.UOM="GAL" THEN SUM(Volume_Qty_GAL)  
                WHEN Account_No IN ("0006341010","0006341020","0006341030","0006341040","0006341050","0006341060") AND {fcb_price_effect_security_function_name}.UOM="BBL" THEN SUM(Volume_Qty_BB6) 
                ELSE 0 END AS UOM1,

                CASE WHEN Account_No IN ("0006341810","0006341820","0006341830","0006341840","0006341850","0006341860") AND {fcb_price_effect_security_function_name}.CURRENCY="Local Currency" THEN SUM(Amt_Comp_Code_Curr)  
                WHEN Account_No IN ("0006341810","0006341820","0006341830","0006341840","0006341850","0006341860") AND {fcb_price_effect_security_function_name}.CURRENCY="Group Currency" THEN SUM(Amt_PC_Local_Curr)             
                ELSE 0 END AS CC2,

                CASE WHEN Account_No IN ("0006341810","0006341820","0006341830","0006341840","0006341850","0006341860") AND {fcb_price_effect_security_function_name}.UOM="KG" THEN SUM(Weight_Qty)
                WHEN Account_No IN ("0006341810","0006341820","0006341830","0006341840","0006341850","0006341860") AND {fcb_price_effect_security_function_name}.UOM="L15" THEN SUM(Volume_Qty) 
                WHEN Account_No IN ("0006341810","0006341820","0006341830","0006341840","0006341850","0006341860") AND {fcb_price_effect_security_function_name}.UOM="GAL" THEN SUM(Volume_Qty_GAL)  
                WHEN Account_No IN ("0006341810","0006341820","0006341830","0006341840","0006341850","0006341860") AND {fcb_price_effect_security_function_name}.UOM="BBL" THEN SUM(Volume_Qty_BB6)
                ELSE 0 END AS UOM2
                FROM temp
                GROUP BY Posting_Period,Material_Number,Plant,Account_No
            )
            
            GROUP BY Posting_Period,Material_Number,Plant)
        ))""")
    
    assignFunctionPermission(uc_catalog_name, uc_curated_schema, fcb_price_effect_security_function_name, tbl_owner_grp,security_end_user_aad_group_name, security_object_aad_group_name, security_functional_dev_aad_group, security_functional_readers_aad_group)

# COMMAND ----------

fcb_stock_effect_security_func_name = uc_catalog_name+"."+uc_curated_schema+"."+fcb_stock_effect_security_function_name
if fcb_stock_effect_security_func_name in function_list:
    spark.sql(f"""DROP FUNCTION `{uc_catalog_name}`.`{uc_curated_schema}`.{fcb_stock_effect_security_function_name}""")
    spark.sql(f"""
    CREATE OR REPLACE FUNCTION `{uc_catalog_name}`.`{uc_curated_schema}`.{fcb_stock_effect_security_function_name}(`Company_Code-Text` STRING,`Profit_Center-Text` STRING, Fiscal_Year STRING, Posting_Period STRING, Division STRING, Account_No STRING, Document_Type STRING,`Plant-Text` STRING, CURRENCY STRING, UOM STRING, Display_Unit STRING) 
    RETURNS TABLE (stock_effect decimal(25,2)) 
    RETURN 

        with temp as (
        select Posting_Period,Material_Number,Plant, Account_No, Amt_Comp_Code_Curr,Amt_PC_Local_Curr, Volume_Qty_GAL, Weight_Qty, Volume_Qty, Volume_Qty_BB6
        from `{uc_catalog_name}`.`{uc_curated_schema}`.{fcb_dynamic_view_name} fcb
        WHERE
        (array_contains(SPLIT({fcb_stock_effect_security_function_name}.`Company_Code-Text`,',') ,fcb.`Company_Code-Text`) OR ({fcb_stock_effect_security_function_name}.`Company_Code-Text`='ALL')) AND
        (array_contains(SPLIT({fcb_stock_effect_security_function_name}.`Profit_Center-Text`,',') ,fcb.`Profit_Center-Text`) OR ({fcb_stock_effect_security_function_name}.`Profit_Center-Text`='ALL')) AND
        (array_contains(SPLIT({fcb_stock_effect_security_function_name}.Fiscal_Year,',') ,fcb.Fiscal_Year) OR ({fcb_stock_effect_security_function_name}.Fiscal_Year='ALL')) AND
        (array_contains(SPLIT({fcb_stock_effect_security_function_name}.Posting_Period,',') ,fcb.Posting_Period) OR ({fcb_stock_effect_security_function_name}.Posting_Period='ALL')) AND
        (array_contains(SPLIT({fcb_stock_effect_security_function_name}.Division,',') ,fcb.Division) OR ({fcb_stock_effect_security_function_name}.Division='ALL')) AND
        (array_contains(SPLIT({fcb_stock_effect_security_function_name}.Account_No,',') ,fcb.Account_No) OR ({fcb_stock_effect_security_function_name}.Account_No='ALL'))AND
        (array_contains(SPLIT({fcb_stock_effect_security_function_name}.Document_Type,',') ,fcb.Document_Type) OR ({fcb_stock_effect_security_function_name}.Document_Type='ALL')) AND
        (array_contains(SPLIT({fcb_stock_effect_security_function_name}.`Plant-Text`,',') ,fcb.`Plant-Text`) OR ({fcb_stock_effect_security_function_name}.`Plant-Text`='ALL'))
        AND fcb.OPFLAG != 'D')

    SELECT 
    CASE WHEN {fcb_stock_effect_security_function_name}.Display_Unit = 'Actual Value' THEN `stock_effect`
    WHEN {fcb_stock_effect_security_function_name}.Display_Unit = 'Thousands' THEN `stock_effect`*0.001
    WHEN {fcb_stock_effect_security_function_name}.Display_Unit = 'Millions' THEN `stock_effect`*0.000001
    WHEN {fcb_stock_effect_security_function_name}.Display_Unit = 'Billions' THEN `stock_effect`*0.000000001
    END
    AS `stock_effect`
    FROM
    (
    SELECT SUM(STOCK_EFFECT_INTERMEDIATE)+SUM(STOCK_EFFECT_EXTRA) AS stock_effect
    FROM 
        (SELECT Posting_Period, Material_Number, Plant,
            CASE WHEN ((-1*UOM2)-UOM1)=0 OR ISNULL((-1*UOM2)-UOM1 ) THEN 0
            WHEN (ISNULL(CC1/UOM1) OR (CC1/UOM1=0)) AND (((CC1 <> 0) AND ISNOTNULL(CC1)) AND ((CC2 <> 0) AND ISNOTNULL(CC2)))
            THEN (-1*CC2) 
            WHEN (ISNULL(CC1/UOM1) OR (CC1/UOM1=0))  AND (CC1 <> 0 AND ISNOTNULL(CC1)) THEN 0
            WHEN (ISNULL(CC1/UOM1) OR (CC1/UOM1=0))  AND (CC1 = 0 OR ISNULL(CC1)) THEN (-1*CC2)
            WHEN (ISNULL(CC2/UOM2) OR (CC2/UOM2=0)) THEN -(CC1)
            WHEN (CC1 = 0 OR ISNULL(CC1)) AND (CC2 <> 0 AND ISNULL(CC2)) THEN (CC1 -CC2)
            WHEN ISNULL(((-1*UOM2)-UOM1)*(-CC1/-UOM1)) THEN 0
            ELSE (((-1*UOM2)-UOM1)*(CC1/UOM1))
            END AS STOCK_EFFECT_INTERMEDIATE,

        CASE WHEN Plant="D314" AND  Material_Number="000000000400001991" THEN ((-1*CC2)-CC1) 
            ELSE 0 END
        AS STOCK_EFFECT_EXTRA  

        FROM
        (
            SELECT Posting_Period,Material_Number,Plant, SUM(CC1) AS CC1, SUM(CC2) AS CC2, SUM(UOM1) AS UOM1, SUM(UOM2) AS UOM2 
            FROM 
            (
                SELECT Posting_Period,Material_Number,Plant,Account_No, 
                CASE WHEN Account_No IN ("0006341010","0006341020","0006341030","0006341040","0006341050","0006341060") AND {fcb_stock_effect_security_function_name}.CURRENCY="Local Currency" THEN SUM(Amt_Comp_Code_Curr)  
                WHEN Account_No IN ("0006341010","0006341020","0006341030","0006341040","0006341050","0006341060") AND {fcb_stock_effect_security_function_name}.CURRENCY="Group Currency" THEN SUM(Amt_PC_Local_Curr)             
                ELSE 0 END AS CC1,

                CASE WHEN Account_No IN ("0006341010","0006341020","0006341030","0006341040","0006341050","0006341060") AND {fcb_stock_effect_security_function_name}.UOM="KG" THEN SUM(Weight_Qty) 
                WHEN Account_No IN ("0006341010","0006341020","0006341030","0006341040","0006341050","0006341060") AND {fcb_stock_effect_security_function_name}.UOM="L15" THEN SUM(Volume_Qty)  
                WHEN Account_No IN ("0006341010","0006341020","0006341030","0006341040","0006341050","0006341060") AND {fcb_stock_effect_security_function_name}.UOM="GAL" THEN SUM(Volume_Qty_GAL)  
                WHEN Account_No IN ("0006341010","0006341020","0006341030","0006341040","0006341050","0006341060") AND {fcb_stock_effect_security_function_name}.UOM="BBL" THEN SUM(Volume_Qty_BB6) 
                ELSE 0 END AS UOM1,

                CASE WHEN Account_No IN ("0006341810","0006341820","0006341830","0006341840","0006341850","0006341860") AND {fcb_stock_effect_security_function_name}.CURRENCY="Local Currency" THEN SUM(Amt_Comp_Code_Curr)  
                WHEN Account_No IN ("0006341810","0006341820","0006341830","0006341840","0006341850","0006341860") AND {fcb_stock_effect_security_function_name}.CURRENCY="Group Currency" THEN SUM(Amt_PC_Local_Curr)             
                ELSE 0 END AS CC2,

                CASE WHEN Account_No IN ("0006341810","0006341820","0006341830","0006341840","0006341850","0006341860") AND {fcb_stock_effect_security_function_name}.UOM="KG" THEN SUM(Weight_Qty)
                WHEN Account_No IN ("0006341810","0006341820","0006341830","0006341840","0006341850","0006341860") AND {fcb_stock_effect_security_function_name}.UOM="L15" THEN SUM(Volume_Qty) 
                WHEN Account_No IN ("0006341810","0006341820","0006341830","0006341840","0006341850","0006341860") AND {fcb_stock_effect_security_function_name}.UOM="GAL" THEN SUM(Volume_Qty_GAL)  
                WHEN Account_No IN ("0006341810","0006341820","0006341830","0006341840","0006341850","0006341860") AND {fcb_stock_effect_security_function_name}.UOM="BBL" THEN SUM(Volume_Qty_BB6)
                ELSE 0 END AS UOM2

                FROM temp
                GROUP BY Posting_Period,Material_Number,Plant,Account_No
            )
            
            GROUP BY Posting_Period,Material_Number,Plant)
        ))""")
    
    assignFunctionPermission(uc_catalog_name, uc_curated_schema, fcb_stock_effect_security_function_name, tbl_owner_grp,security_end_user_aad_group_name, security_object_aad_group_name, security_functional_dev_aad_group, security_functional_readers_aad_group)

    print("fcb_stock_effect_security function dropped and recreated in the schema")
else:
    spark.sql(f"""
    CREATE OR REPLACE FUNCTION `{uc_catalog_name}`.`{uc_curated_schema}`.{fcb_stock_effect_security_function_name}(`Company_Code-Text` STRING,`Profit_Center-Text` STRING, Fiscal_Year STRING, Posting_Period STRING, Division STRING, Account_No STRING, Document_Type STRING, `Plant-Text` STRING, CURRENCY STRING, UOM STRING, Display_Unit STRING) 
    RETURNS TABLE (stock_effect decimal(25,2)) 
    RETURN 

        with temp as (
        select Posting_Period,Material_Number,Plant, Account_No, Amt_Comp_Code_Curr,Amt_PC_Local_Curr, Volume_Qty_GAL, Weight_Qty, Volume_Qty, Volume_Qty_BB6
        from `{uc_catalog_name}`.`{uc_curated_schema}`.{fcb_dynamic_view_name} fcb
        WHERE
        (array_contains(SPLIT({fcb_stock_effect_security_function_name}.`Company_Code-Text`,',') ,fcb.`Company_Code-Text`) OR ({fcb_stock_effect_security_function_name}.`Company_Code-Text`='ALL')) AND
        (array_contains(SPLIT({fcb_stock_effect_security_function_name}.`Profit_Center-Text`,',') ,fcb.`Profit_Center-Text`) OR ({fcb_stock_effect_security_function_name}.`Profit_Center-Text`='ALL')) AND
        (array_contains(SPLIT({fcb_stock_effect_security_function_name}.Fiscal_Year,',') ,fcb.Fiscal_Year) OR ({fcb_stock_effect_security_function_name}.Fiscal_Year='ALL')) AND
        (array_contains(SPLIT({fcb_stock_effect_security_function_name}.Posting_Period,',') ,fcb.Posting_Period) OR ({fcb_stock_effect_security_function_name}.Posting_Period='ALL')) AND
        (array_contains(SPLIT({fcb_stock_effect_security_function_name}.Division,',') ,fcb.Division) OR ({fcb_stock_effect_security_function_name}.Division='ALL')) AND
        (array_contains(SPLIT({fcb_stock_effect_security_function_name}.Account_No,',') ,fcb.Account_No) OR ({fcb_stock_effect_security_function_name}.Account_No='ALL'))AND
        (array_contains(SPLIT({fcb_stock_effect_security_function_name}.Document_Type,',') ,fcb.Document_Type) OR ({fcb_stock_effect_security_function_name}.Document_Type='ALL')) AND
        (array_contains(SPLIT({fcb_stock_effect_security_function_name}.`Plant-Text`,',') ,fcb.`Plant-Text`) OR ({fcb_stock_effect_security_function_name}.`Plant-Text`='ALL'))
        AND fcb.OPFLAG != 'D')

    SELECT 
    CASE WHEN {fcb_stock_effect_security_function_name}.Display_Unit = 'Actual Value' THEN `stock_effect`
    WHEN {fcb_stock_effect_security_function_name}.Display_Unit = 'Thousands' THEN `stock_effect`*0.001
    WHEN {fcb_stock_effect_security_function_name}.Display_Unit = 'Millions' THEN `stock_effect`*0.000001
    WHEN {fcb_stock_effect_security_function_name}.Display_Unit = 'Billions' THEN `stock_effect`*0.000000001
    END
    AS `stock_effect`
    FROM
    (
    SELECT SUM(STOCK_EFFECT_INTERMEDIATE)+SUM(STOCK_EFFECT_EXTRA) AS stock_effect
    FROM 
        (SELECT Posting_Period, Material_Number, Plant,
            CASE WHEN ((-1*UOM2)-UOM1)=0 OR ISNULL((-1*UOM2)-UOM1 ) THEN 0
            WHEN (ISNULL(CC1/UOM1) OR (CC1/UOM1=0)) AND (((CC1 <> 0) AND ISNOTNULL(CC1)) AND ((CC2 <> 0) AND ISNOTNULL(CC2)))
            THEN (-1*CC2) 
            WHEN (ISNULL(CC1/UOM1) OR (CC1/UOM1=0))  AND (CC1 <> 0 AND ISNOTNULL(CC1)) THEN 0
            WHEN (ISNULL(CC1/UOM1) OR (CC1/UOM1=0))  AND (CC1 = 0 OR ISNULL(CC1)) THEN (-1*CC2)
            WHEN (ISNULL(CC2/UOM2) OR (CC2/UOM2=0)) THEN -(CC1)
            WHEN (CC1 = 0 OR ISNULL(CC1)) AND (CC2 <> 0 AND ISNULL(CC2)) THEN (CC1 -CC2)
            WHEN ISNULL(((-1*UOM2)-UOM1)*(-CC1/-UOM1)) THEN 0
            ELSE (((-1*UOM2)-UOM1)*(CC1/UOM1))
            END AS STOCK_EFFECT_INTERMEDIATE,

        CASE WHEN Plant="D314" AND  Material_Number="000000000400001991" THEN ((-1*CC2)-CC1) 
            ELSE 0 END
        AS STOCK_EFFECT_EXTRA  

        FROM
        (
            SELECT Posting_Period,Material_Number,Plant, SUM(CC1) AS CC1, SUM(CC2) AS CC2, SUM(UOM1) AS UOM1, SUM(UOM2) AS UOM2 
            FROM 
            (
                SELECT Posting_Period,Material_Number,Plant,Account_No, 
                CASE WHEN Account_No IN ("0006341010","0006341020","0006341030","0006341040","0006341050","0006341060") AND {fcb_stock_effect_security_function_name}.CURRENCY="Local Currency" THEN SUM(Amt_Comp_Code_Curr)  
                WHEN Account_No IN ("0006341010","0006341020","0006341030","0006341040","0006341050","0006341060") AND {fcb_stock_effect_security_function_name}.CURRENCY="Group Currency" THEN SUM(Amt_PC_Local_Curr)             
                ELSE 0 END AS CC1,

                CASE WHEN Account_No IN ("0006341010","0006341020","0006341030","0006341040","0006341050","0006341060") AND {fcb_stock_effect_security_function_name}.UOM="KG" THEN SUM(Weight_Qty) 
                WHEN Account_No IN ("0006341010","0006341020","0006341030","0006341040","0006341050","0006341060") AND {fcb_stock_effect_security_function_name}.UOM="L15" THEN SUM(Volume_Qty)  
                WHEN Account_No IN ("0006341010","0006341020","0006341030","0006341040","0006341050","0006341060") AND {fcb_stock_effect_security_function_name}.UOM="GAL" THEN SUM(Volume_Qty_GAL)  
                WHEN Account_No IN ("0006341010","0006341020","0006341030","0006341040","0006341050","0006341060") AND {fcb_stock_effect_security_function_name}.UOM="BBL" THEN SUM(Volume_Qty_BB6) 
                ELSE 0 END AS UOM1,

                CASE WHEN Account_No IN ("0006341810","0006341820","0006341830","0006341840","0006341850","0006341860") AND {fcb_stock_effect_security_function_name}.CURRENCY="Local Currency" THEN SUM(Amt_Comp_Code_Curr)  
                WHEN Account_No IN ("0006341810","0006341820","0006341830","0006341840","0006341850","0006341860") AND {fcb_stock_effect_security_function_name}.CURRENCY="Group Currency" THEN SUM(Amt_PC_Local_Curr)             
                ELSE 0 END AS CC2,

                CASE WHEN Account_No IN ("0006341810","0006341820","0006341830","0006341840","0006341850","0006341860") AND {fcb_stock_effect_security_function_name}.UOM="KG" THEN SUM(Weight_Qty)
                WHEN Account_No IN ("0006341810","0006341820","0006341830","0006341840","0006341850","0006341860") AND {fcb_stock_effect_security_function_name}.UOM="L15" THEN SUM(Volume_Qty) 
                WHEN Account_No IN ("0006341810","0006341820","0006341830","0006341840","0006341850","0006341860") AND {fcb_stock_effect_security_function_name}.UOM="GAL" THEN SUM(Volume_Qty_GAL)  
                WHEN Account_No IN ("0006341810","0006341820","0006341830","0006341840","0006341850","0006341860") AND {fcb_stock_effect_security_function_name}.UOM="BBL" THEN SUM(Volume_Qty_BB6)
                ELSE 0 END AS UOM2

                FROM temp
                GROUP BY Posting_Period,Material_Number,Plant,Account_No
            )
            
            GROUP BY Posting_Period,Material_Number,Plant)
        ))""")
    assignFunctionPermission(uc_catalog_name, uc_curated_schema, fcb_stock_effect_security_function_name, tbl_owner_grp,security_end_user_aad_group_name, security_object_aad_group_name, security_functional_dev_aad_group, security_functional_readers_aad_group)

# COMMAND ----------

massbalance_per_commodity_grade_security_func_name = uc_catalog_name+"."+uc_curated_schema+"."+massbalance_per_commodity_grade_security_function_name
if massbalance_per_commodity_grade_security_func_name in function_list:
    spark.sql(f"""DROP FUNCTION `{uc_catalog_name}`.`{uc_curated_schema}`.{massbalance_per_commodity_grade_security_function_name}""")
    spark.sql(f"""
    CREATE OR REPLACE FUNCTION `{uc_catalog_name}`.`{uc_curated_schema}`.{massbalance_per_commodity_grade_security_function_name}(`Company_Code-Text` STRING,`Profit_Center-Text` STRING, Fiscal_Year STRING, Posting_Period STRING, Division STRING, Account_No STRING, Document_Type STRING, `Plant-Text` STRING, CURRENCY STRING, UOM STRING,Display_Unit STRING) 

    RETURNS TABLE (COMMODITY_CODE STRING, COMMODITY_GRADE STRING,STOCK_OPENING_QTY decimal(25,2), ACQUISITION_QTY decimal(25,2),SALES_QTY decimal(25,2), EXCHANGES_QTY decimal(25,2),TRANSFER_QTY decimal(25,2), STOCK_CLOSE_QTY decimal(25,2), STOCK_CHANGE_QTY decimal(25,2) )

    RETURN 

    with temp as (
    select Commodity_Code AS COMMODITY_CODE, Commodity_Code_Text AS COMMODITY_GRADE, Account_No,Functional_Area, Amt_Comp_Code_Curr,Amt_PC_Local_Curr, Volume_Qty_GAL, Weight_Qty, Volume_Qty, Volume_Qty_BB6
    from `{uc_catalog_name}`.`{uc_curated_schema}`.{fcb_dynamic_view_name} fcb
    WHERE
    (array_contains(SPLIT({massbalance_per_commodity_grade_security_function_name}.`Company_Code-Text`,',') ,fcb.`Company_Code-Text`) OR ({massbalance_per_commodity_grade_security_function_name}.`Company_Code-Text`='ALL')) AND
    (array_contains(SPLIT({massbalance_per_commodity_grade_security_function_name}.`Profit_Center-Text`,',') ,fcb.`Profit_Center-Text`) OR ({massbalance_per_commodity_grade_security_function_name}.`Profit_Center-Text`='ALL')) AND
    (array_contains(SPLIT({massbalance_per_commodity_grade_security_function_name}.Fiscal_Year,',') ,fcb.Fiscal_Year) OR ({massbalance_per_commodity_grade_security_function_name}.Fiscal_Year='ALL')) AND
    (array_contains(SPLIT({massbalance_per_commodity_grade_security_function_name}.Posting_Period,',') ,fcb.Posting_Period) OR ({massbalance_per_commodity_grade_security_function_name}.Posting_Period='ALL')) AND
    (array_contains(SPLIT({massbalance_per_commodity_grade_security_function_name}.Division,',') ,fcb.Division) OR ({massbalance_per_commodity_grade_security_function_name}.Division='ALL')) AND
    (array_contains(SPLIT({massbalance_per_commodity_grade_security_function_name}.Account_No,',') ,fcb.Account_No) OR ({massbalance_per_commodity_grade_security_function_name}.Account_No='ALL'))AND
    (array_contains(SPLIT({massbalance_per_commodity_grade_security_function_name}.Document_Type,',') ,fcb.Document_Type) OR ({massbalance_per_commodity_grade_security_function_name}.Document_Type='ALL')) AND
    (array_contains(SPLIT({massbalance_per_commodity_grade_security_function_name}.`Plant-Text`,',') ,fcb.`Plant-Text`) OR ({massbalance_per_commodity_grade_security_function_name}.`Plant-Text`='ALL'))
    AND fcb.OPFLAG != 'D')

    SELECT
        Commodity_Code,COMMODITY_GRADE,
        CASE 
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Actual Value' THEN STOCK_OPENING_QTY
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Thousands' THEN STOCK_OPENING_QTY*0.001
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Millions' THEN STOCK_OPENING_QTY*0.000001
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Billions' THEN STOCK_OPENING_QTY*0.000000001
        END AS STOCK_OPENING_QTY,
        CASE 
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Actual Value' THEN ACQUISITION_QTY
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Thousands' THEN ACQUISITION_QTY*0.001
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Millions' THEN ACQUISITION_QTY*0.000001
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Billions' THEN ACQUISITION_QTY*0.000000001
        END AS ACQUISITION_QTY,        
        CASE 
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Actual Value' THEN SALES_QTY
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Thousands' THEN SALES_QTY*0.001
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Millions' THEN SALES_QTY*0.000001
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Billions' THEN SALES_QTY*0.000000001
        END AS SALES_QTY,
        CASE 
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Actual Value' THEN EXCHANGES_QTY
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Thousands' THEN EXCHANGES_QTY*0.001
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Millions' THEN EXCHANGES_QTY*0.000001
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Billions' THEN EXCHANGES_QTY*0.000000001
        END  AS EXCHANGES_QTY,
        CASE 
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Actual Value' THEN TRANSFER_QTY
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Thousands' THEN TRANSFER_QTY*0.001
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Millions' THEN TRANSFER_QTY*0.000001
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Billions' THEN TRANSFER_QTY*0.000000001
        END  AS TRANSFER_QTY ,
        CASE 
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Actual Value' THEN STOCK_CLOSE_QTY
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Thousands' THEN STOCK_CLOSE_QTY*0.001
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Millions' THEN STOCK_CLOSE_QTY*0.000001
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Billions' THEN STOCK_CLOSE_QTY*0.000000001
        END AS STOCK_CLOSE_QTY,
        CASE 
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Actual Value' THEN STOCK_CHANGE_QTY
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Thousands' THEN STOCK_CHANGE_QTY*0.001
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Millions' THEN STOCK_CHANGE_QTY*0.000001
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Billions' THEN STOCK_CHANGE_QTY*0.000000001
        END AS STOCK_CHANGE_QTY
    FROM
    (
    SELECT  Commodity_Code,COMMODITY_GRADE, SUM(STOCK_OPENING_QTY) AS STOCK_OPENING_QTY, COALESCE(SUM(THIRD_PARTY_PURCHASES1),0)+ COALESCE(SUM(THIRD_PARTY_PURCHASES2),0)+ COALESCE(SUM(IG_PURCHASE_QTY1),0)+ COALESCE(SUM(IG_PURCHASE_QTY2),0) AS ACQUISITION_QTY, COALESCE(SUM(THIRD_PARTY_SALES_QTY),0)+COALESCE(SUM(INTER_LOB_SALES_QTY),0)+COALESCE(SUM(IG_SALES_QTY),0) AS SALES_QTY ,COALESCE(SUM(EXG_PURCHASE_QTY),0)+COALESCE(SUM(EXG_SALES_QTY),0) AS EXCHANGES_QTY,COALESCE(SUM(IS_PURCHASES_QTY1),0)+COALESCE(SUM(IS_PURCHASES_QTY2),0)+COALESCE(SUM(IS_PURCHASES_QTY3),0)+COALESCE(SUM(IS_PURCHASES_QTY4),0)+COALESCE(SUM(IS_PURCHASES_QTY5),0)+COALESCE(SUM(IS_SALES_QTY1),0)+COALESCE(SUM(IS_SALES_QTY2),0)+COALESCE(SUM(IS_SALES_QTY3),0)+COALESCE(SUM(IS_SALES_QTY4),0)+COALESCE(SUM(INTERNAL_SC_FROM_QTY),0)+COALESCE(SUM(INTERNAL_SC_TO_QTY),0) AS TRANSFER_QTY ,SUM(STOCK_CLOSE_QTY) AS STOCK_CLOSE_QTY, CASE WHEN (ISNOTNULL(SUM(STOCK_CLOSE_QTY)) OR ISNOTNULL(SUM(STOCK_OPENING_QTY))) THEN(-1*(COALESCE(SUM(STOCK_CLOSE_QTY),0)+COALESCE(SUM(STOCK_OPENING_QTY),0))) ELSE '' END AS STOCK_CHANGE_QTY

    FROM 
    (SELECT Commodity_Code,COMMODITY_GRADE, Account_No, Functional_Area,
                
        CASE WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="KG" AND Account_No IN ("0006341010","0006341020","0006341030","0006341040","0006341050","0006341060") THEN SUM(Weight_Qty)
                WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="L15" AND Account_No IN ("0006341010","0006341020","0006341030","0006341040","0006341050","0006341060") THEN SUM(Volume_Qty)
                WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="GAL" AND Account_No IN ("0006341010","0006341020","0006341030","0006341040","0006341050","0006341060") THEN SUM(Volume_Qty_GAL)
                WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="BBL" AND Account_No IN ("0006341010","0006341020","0006341030","0006341040","0006341050","0006341060") THEN SUM(Volume_Qty_BB6) 
                ELSE "" END AS STOCK_OPENING_QTY,


        CASE WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="KG" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV05","SV06") THEN SUM(Weight_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="L15" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV05","SV06")THEN SUM(Volume_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="GAL" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV05","SV06") THEN SUM(Volume_Qty_GAL)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="BBL" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV05","SV06") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS THIRD_PARTY_PURCHASES1,


        CASE WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="KG" AND Account_No IN ("0006330140","0006340020","0006342110","0006342130","0006342150","0006342200","0006342510","0006342520","0006342530","0006351070","0006351800","0006354200","0006900470","0007250210","0006341140","0006341141","0006341142","0006341143","0006365000") THEN SUM(Weight_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="L15" AND Account_No IN ("0006330140","0006340020","0006342110","0006342130","0006342150","0006342200","0006342510","0006342520","0006342530","0006351070","0006351800","0006354200","0006900470","0007250210","0006341140","0006341141","0006341142","0006341143","0006365000") THEN SUM(Volume_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="GAL" AND Account_No IN ("0006330140","0006340020","0006342110","0006342130","0006342150","0006342200","0006342510","0006342520","0006342530","0006351070","0006351800","0006354200","0006900470","0007250210","0006341140","0006341141","0006341142","0006341143","0006365000") THEN SUM(Volume_Qty_GAL)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="BBL" AND Account_No IN ("0006330140","0006340020","0006342110","0006342130","0006342150","0006342200","0006342510","0006342520","0006342530","0006351070","0006351800","0006354200","0006900470","0007250210","0006341140","0006341141","0006341142","0006341143","0006365000") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS THIRD_PARTY_PURCHASES2,

        CASE WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="KG" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV01","SV02","SV60","SV61") THEN SUM(Weight_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="L15" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV01","SV02","SV60","SV61")THEN SUM(Volume_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="GAL" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV01","SV02","SV60","SV61") THEN SUM(Volume_Qty_GAL)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="BBL" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV01","SV02","SV60","SV61") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IG_PURCHASE_QTY1,


        CASE WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="KG" AND Account_No IN ("0006342100","0006342050","0006342120") THEN SUM(Weight_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="L15" AND Account_No IN ("0006342100","0006342050","0006342120") THEN SUM(Volume_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="GAL" AND Account_No IN ("0006342100","0006342050","0006342120") THEN SUM(Volume_Qty_GAL)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="BBL" AND Account_No IN ("0006342100","0006342050","0006342120") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IG_PURCHASE_QTY2,


        CASE WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="KG" AND Account_No IN ("0006000100","0006000150","0006000200","0006000250","0006000300","0006000400","0006000500","0006000600","0006002105","0006009000","0006009050","0006009100","0006140100","0006140105","0006140200","0006140300","0006140500","0006140700","0006342550") THEN SUM(Weight_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="L15" AND Account_No IN ("0006000100","0006000150","0006000200","0006000250","0006000300","0006000400","0006000500","0006000600","0006002105","0006009000","0006009050","0006009100","0006140100","0006140105","0006140200","0006140300","0006140500","0006140700","0006342550") THEN SUM(Volume_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="GAL" AND Account_No IN ("0006000100","0006000150","0006000200","0006000250","0006000300","0006000400","0006000500","0006000600","0006002105","0006009000","0006009050","0006009100","0006140100","0006140105","0006140200","0006140300","0006140500","0006140700","0006342550") THEN SUM(Volume_Qty_GAL)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="BBL" AND Account_No IN ("0006000100","0006000150","0006000200","0006000250","0006000300","0006000400","0006000500","0006000600","0006002105","0006009000","0006009050","0006009100","0006140100","0006140105","0006140200","0006140300","0006140500","0006140700","0006342550") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS THIRD_PARTY_SALES_QTY,

        CASE WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="KG" AND Account_No IN ("0006380302","0006380303") AND Functional_Area ="SV46" THEN SUM(Weight_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="L15" AND Account_No IN ("0006380302","0006380303") AND Functional_Area ="SV46" THEN SUM(Volume_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="GAL" AND Account_No IN ("0006380302","0006380303") AND Functional_Area ="SV46" THEN SUM(Volume_Qty_GAL)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="BBL" AND Account_No IN ("0006380302","0006380303") AND Functional_Area ="SV46" THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS INTER_LOB_SALES_QTY,



        CASE WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="KG" AND Account_No IN ("0006001500","0006001700","0006001900","0006002100") THEN SUM(Weight_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="L15" AND Account_No IN ("0006001500","0006001700","0006001900","0006002100") THEN SUM(Volume_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="GAL" AND Account_No IN ("0006001500","0006001700","0006001900","0006002100") THEN SUM(Volume_Qty_GAL)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="BBL" AND Account_No IN ("0006001500","0006001700","0006001900","0006002100") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IG_SALES_QTY,


        CASE WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="KG" AND Account_No IN ("0006342000","0006340010") AND Functional_Area IN ("SV10","SV11","SV12","SV50","SV51") THEN SUM(Weight_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="L15" AND Account_No IN ("0006342000","0006340010") AND Functional_Area IN ("SV10","SV11","SV12","SV50","SV51")THEN SUM(Volume_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="GAL" AND Account_No IN ("0006342000","0006340010") AND Functional_Area IN ("SV10","SV11","SV12","SV50","SV51") THEN SUM(Volume_Qty_GAL)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="BBL" AND Account_No IN ("0006342000","0006340010") AND Functional_Area IN ("SV10","SV11","SV12","SV50","SV51") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS EXG_PURCHASE_QTY,


        CASE WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="KG" AND Account_No ="0006002220" THEN SUM(Weight_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="L15" AND Account_No ="0006002220" THEN SUM(Volume_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="GAL" AND Account_No ="0006002220" THEN SUM(Volume_Qty_GAL)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="BBL" AND Account_No ="0006002220" THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS EXG_SALES_QTY,



        CASE WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="KG" AND Account_No ="0006380201" THEN SUM(Weight_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="L15" AND Account_No ="0006380201" THEN SUM(Volume_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="GAL" AND Account_No ="0006380201" THEN SUM(Volume_Qty_GAL)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="BBL" AND Account_No ="0006380201" THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IS_PURCHASES_QTY1,




        CASE WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="KG" AND Account_No = "0006380202" AND Functional_Area IN ("SV44","SV45","SV48","SV56","SV58","") THEN SUM(Weight_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="L15" AND Account_No = "0006380202" AND Functional_Area IN ("SV44","SV45","SV48","SV56","SV58","")THEN SUM(Volume_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="GAL" AND Account_No = "0006380202" AND Functional_Area IN ("SV44","SV45","SV48","SV56","SV58","") THEN SUM(Volume_Qty_GAL)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="BBL" AND Account_No = "0006380202" AND Functional_Area IN ("SV44","SV45","SV48","SV56","SV58","") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IS_PURCHASES_QTY2,



        CASE WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="KG" AND Account_No = "0006380203" AND Functional_Area IN ("SV03","SV48","SV49","SV53","SV55","") THEN SUM(Weight_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="L15" AND Account_No = "0006380203" AND Functional_Area IN ("SV03","SV48","SV49","SV53","SV55","")THEN SUM(Volume_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="GAL" AND Account_No = "0006380203" AND Functional_Area IN ("SV03","SV48","SV49","SV53","SV55","") THEN SUM(Volume_Qty_GAL)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="BBL" AND Account_No = "0006380203" AND Functional_Area IN ("SV03","SV48","SV49","SV53","SV55","") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IS_PURCHASES_QTY3,


        CASE WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="KG" AND Account_No = "0006380302" AND Functional_Area IN ("SV33","SV57") THEN SUM(Weight_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="L15" AND Account_No = "0006380302" AND Functional_Area IN ("SV33","SV57")THEN SUM(Volume_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="GAL" AND Account_No = "0006380302" AND Functional_Area IN ("SV33","SV57") THEN SUM(Volume_Qty_GAL)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="BBL" AND Account_No = "0006380302" AND Functional_Area IN ("SV33","SV57") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IS_PURCHASES_QTY4,



        CASE WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="KG" AND Account_No = "0006380303" AND Functional_Area="SV54" THEN SUM(Weight_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="L15" AND Account_No = "0006380303" AND Functional_Area ="SV54" THEN SUM(Volume_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="GAL" AND Account_No = "0006380303" AND Functional_Area ="SV54" THEN SUM(Volume_Qty_GAL)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="BBL" AND Account_No = "0006380303" AND Functional_Area ="SV54" THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IS_PURCHASES_QTY5,


        CASE WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="KG" AND Account_No IN ("0006380100","0006380300") THEN SUM(Weight_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="L15" AND Account_No IN ("0006380100","0006380300") THEN SUM(Volume_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="GAL" AND Account_No IN ("0006380100","0006380300") THEN SUM(Volume_Qty_GAL)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="BBL" AND Account_No IN ("0006380100","0006380300") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IS_SALES_QTY1,


        CASE WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="KG" AND Account_No IN("0006380202","0006380203") AND Functional_Area="SV49" THEN SUM(Weight_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="L15" AND Account_No IN("0006380202","0006380203") AND Functional_Area ="SV49" THEN SUM(Volume_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="GAL" AND Account_No IN("0006380202","0006380203") AND Functional_Area ="SV49" THEN SUM(Volume_Qty_GAL)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="BBL" AND Account_No IN("0006380202","0006380203") AND Functional_Area ="SV49" THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IS_SALES_QTY2,


        CASE WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="KG" AND Account_No = "0006380303" AND Functional_Area IN ("SV03","SV32","SV47","") THEN SUM(Weight_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="L15" AND Account_No = "0006380303" AND Functional_Area IN ("SV03","SV32","SV47","")THEN SUM(Volume_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="GAL" AND Account_No = "0006380303" AND Functional_Area IN ("SV03","SV32","SV47","") THEN SUM(Volume_Qty_GAL)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="BBL" AND Account_No = "0006380303" AND Functional_Area IN ("SV03","SV32","SV47","") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IS_SALES_QTY3,


        CASE WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="KG" AND Account_No = "0006380302" AND Functional_Area IN ("SV34","SV47","") THEN SUM(Weight_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="L15" AND Account_No = "0006380302" AND Functional_Area IN ("SV34","SV47","")THEN SUM(Volume_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="GAL" AND Account_No = "0006380302" AND Functional_Area IN ("SV34","SV47","") THEN SUM(Volume_Qty_GAL)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="BBL" AND Account_No = "0006380302" AND Functional_Area IN ("SV34","SV47","") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IS_SALES_QTY4,


        CASE WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="KG" AND Account_No = "0006380203" AND Functional_Area ="SV30" THEN SUM(Weight_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="L15" AND Account_No = "0006380203" AND Functional_Area ="SV30"THEN SUM(Volume_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="GAL" AND Account_No = "0006380203" AND Functional_Area ="SV30" THEN SUM(Volume_Qty_GAL)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="BBL" AND Account_No = "0006380203" AND Functional_Area ="SV30" THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS INTERNAL_SC_FROM_QTY,



        CASE WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="KG" AND Account_No = "0006380303" AND Functional_Area ="SV31" THEN SUM(Weight_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="L15" AND Account_No = "0006380303" AND Functional_Area ="SV31"THEN SUM(Volume_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="GAL" AND Account_No = "0006380303" AND Functional_Area ="SV31" THEN SUM(Volume_Qty_GAL)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="BBL" AND Account_No = "0006380303" AND Functional_Area ="SV31" THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS INTERNAL_SC_TO_QTY,




        CASE WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="KG" AND Account_No IN ("0006341810","0006341820","0006341830","0006341840","0006341850","0006341860") THEN SUM(Weight_Qty)
                WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="L15" AND Account_No IN ("0006341810","0006341820","0006341830","0006341840","0006341850","0006341860") THEN SUM(Volume_Qty)
                WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="GAL" AND Account_No IN ("0006341810","0006341820","0006341830","0006341840","0006341850","0006341860") THEN SUM(Volume_Qty_GAL)
                WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="BBL" AND Account_No IN ("0006341810","0006341820","0006341830","0006341840","0006341850","0006341860") THEN SUM(Volume_Qty_BB6) 
                ELSE "" END AS STOCK_CLOSE_QTY

        FROM temp 
        GROUP BY Commodity_Code,COMMODITY_GRADE, Account_No, Functional_Area
    )
    GROUP BY Commodity_Code,COMMODITY_GRADE)""")
    
    assignFunctionPermission(uc_catalog_name, uc_curated_schema, massbalance_per_commodity_grade_security_function_name, tbl_owner_grp,security_end_user_aad_group_name, security_object_aad_group_name, security_functional_dev_aad_group, security_functional_readers_aad_group)

    print("massbalance_per_commodity_grade_security function dropped and recreated in the schema")
else:
    spark.sql(f"""
    CREATE OR REPLACE FUNCTION `{uc_catalog_name}`.`{uc_curated_schema}`.{massbalance_per_commodity_grade_security_function_name}(`Company_Code-Text` STRING,`Profit_Center-Text` STRING, Fiscal_Year STRING, Posting_Period STRING, Division STRING, Account_No STRING, Document_Type STRING, `Plant-Text` STRING, CURRENCY STRING, UOM STRING,Display_Unit STRING) 

    RETURNS TABLE ( COMMODITY_CODE STRING, COMMODITY_GRADE STRING,STOCK_OPENING_QTY decimal(25,2), ACQUISITION_QTY decimal(25,2),SALES_QTY decimal(25,2), EXCHANGES_QTY decimal(25,2),TRANSFER_QTY decimal(25,2), STOCK_CLOSE_QTY decimal(25,2), STOCK_CHANGE_QTY decimal(25,2) )

    RETURN 

    with temp as (
    select Commodity_Code AS COMMODITY_CODE, Commodity_Code_Text AS COMMODITY_GRADE, Account_No,Functional_Area, Amt_Comp_Code_Curr,Amt_PC_Local_Curr, Volume_Qty_GAL, Weight_Qty, Volume_Qty, Volume_Qty_BB6
    from `{uc_catalog_name}`.`{uc_curated_schema}`.{fcb_dynamic_view_name} fcb
    WHERE
    (array_contains(SPLIT({massbalance_per_commodity_grade_security_function_name}.`Company_Code-Text`,',') ,fcb.`Company_Code-Text`) OR ({massbalance_per_commodity_grade_security_function_name}.`Company_Code-Text`='ALL')) AND
    (array_contains(SPLIT({massbalance_per_commodity_grade_security_function_name}.`Profit_Center-Text`,',') ,fcb.`Profit_Center-Text`) OR ({massbalance_per_commodity_grade_security_function_name}.`Profit_Center-Text`='ALL')) AND
    (array_contains(SPLIT({massbalance_per_commodity_grade_security_function_name}.Fiscal_Year,',') ,fcb.Fiscal_Year) OR ({massbalance_per_commodity_grade_security_function_name}.Fiscal_Year='ALL')) AND
    (array_contains(SPLIT({massbalance_per_commodity_grade_security_function_name}.Posting_Period,',') ,fcb.Posting_Period) OR ({massbalance_per_commodity_grade_security_function_name}.Posting_Period='ALL')) AND
    (array_contains(SPLIT({massbalance_per_commodity_grade_security_function_name}.Division,',') ,fcb.Division) OR ({massbalance_per_commodity_grade_security_function_name}.Division='ALL')) AND
    (array_contains(SPLIT({massbalance_per_commodity_grade_security_function_name}.Account_No,',') ,fcb.Account_No) OR ({massbalance_per_commodity_grade_security_function_name}.Account_No='ALL'))AND
    (array_contains(SPLIT({massbalance_per_commodity_grade_security_function_name}.Document_Type,',') ,fcb.Document_Type) OR ({massbalance_per_commodity_grade_security_function_name}.Document_Type='ALL')) AND
    (array_contains(SPLIT({massbalance_per_commodity_grade_security_function_name}.`Plant-Text`,',') ,fcb.`Plant-Text`) OR ({massbalance_per_commodity_grade_security_function_name}.`Plant-Text`='ALL'))
    AND fcb.OPFLAG != 'D')
    SELECT
        Commodity_Code,COMMODITY_GRADE,
        CASE 
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Actual Value' THEN STOCK_OPENING_QTY
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Thousands' THEN STOCK_OPENING_QTY*0.001
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Millions' THEN STOCK_OPENING_QTY*0.000001
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Billions' THEN STOCK_OPENING_QTY*0.000000001
        END AS STOCK_OPENING_QTY,
        CASE 
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Actual Value' THEN ACQUISITION_QTY
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Thousands' THEN ACQUISITION_QTY*0.001
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Millions' THEN ACQUISITION_QTY*0.000001
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Billions' THEN ACQUISITION_QTY*0.000000001
        END AS ACQUISITION_QTY,        
        CASE 
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Actual Value' THEN SALES_QTY
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Thousands' THEN SALES_QTY*0.001
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Millions' THEN SALES_QTY*0.000001
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Billions' THEN SALES_QTY*0.000000001
        END AS SALES_QTY,
        CASE 
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Actual Value' THEN EXCHANGES_QTY
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Thousands' THEN EXCHANGES_QTY*0.001
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Millions' THEN EXCHANGES_QTY*0.000001
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Billions' THEN EXCHANGES_QTY*0.000000001
        END  AS EXCHANGES_QTY,
        CASE 
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Actual Value' THEN TRANSFER_QTY
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Thousands' THEN TRANSFER_QTY*0.001
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Millions' THEN TRANSFER_QTY*0.000001
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Billions' THEN TRANSFER_QTY*0.000000001
        END  AS TRANSFER_QTY ,
        CASE 
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Actual Value' THEN STOCK_CLOSE_QTY
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Thousands' THEN STOCK_CLOSE_QTY*0.001
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Millions' THEN STOCK_CLOSE_QTY*0.000001
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Billions' THEN STOCK_CLOSE_QTY*0.000000001
        END AS STOCK_CLOSE_QTY,
        CASE 
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Actual Value' THEN STOCK_CHANGE_QTY
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Thousands' THEN STOCK_CHANGE_QTY*0.001
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Millions' THEN STOCK_CHANGE_QTY*0.000001
        WHEN {massbalance_per_commodity_grade_security_function_name}.Display_Unit = 'Billions' THEN STOCK_CHANGE_QTY*0.000000001
        END AS STOCK_CHANGE_QTY
    FROM
    (

    SELECT  Commodity_Code,COMMODITY_GRADE, SUM(STOCK_OPENING_QTY) AS STOCK_OPENING_QTY, COALESCE(SUM(THIRD_PARTY_PURCHASES1),0)+ COALESCE(SUM(THIRD_PARTY_PURCHASES2),0)+ COALESCE(SUM(IG_PURCHASE_QTY1),0)+ COALESCE(SUM(IG_PURCHASE_QTY2),0) AS ACQUISITION_QTY, COALESCE(SUM(THIRD_PARTY_SALES_QTY),0)+COALESCE(SUM(INTER_LOB_SALES_QTY),0)+COALESCE(SUM(IG_SALES_QTY),0) AS SALES_QTY ,COALESCE(SUM(EXG_PURCHASE_QTY),0)+COALESCE(SUM(EXG_SALES_QTY),0) AS EXCHANGES_QTY,COALESCE(SUM(IS_PURCHASES_QTY1),0)+COALESCE(SUM(IS_PURCHASES_QTY2),0)+COALESCE(SUM(IS_PURCHASES_QTY3),0)+COALESCE(SUM(IS_PURCHASES_QTY4),0)+COALESCE(SUM(IS_PURCHASES_QTY5),0)+COALESCE(SUM(IS_SALES_QTY1),0)+COALESCE(SUM(IS_SALES_QTY2),0)+COALESCE(SUM(IS_SALES_QTY3),0)+COALESCE(SUM(IS_SALES_QTY4),0)+COALESCE(SUM(INTERNAL_SC_FROM_QTY),0)+COALESCE(SUM(INTERNAL_SC_TO_QTY),0) AS TRANSFER_QTY ,SUM(STOCK_CLOSE_QTY) AS STOCK_CLOSE_QTY, CASE WHEN (ISNOTNULL(SUM(STOCK_CLOSE_QTY)) OR ISNOTNULL(SUM(STOCK_OPENING_QTY))) THEN(-1*(COALESCE(SUM(STOCK_CLOSE_QTY),0)+COALESCE(SUM(STOCK_OPENING_QTY),0))) ELSE '' END AS STOCK_CHANGE_QTY

    FROM 
    (SELECT Commodity_Code,COMMODITY_GRADE, Account_No, Functional_Area,
                
        CASE WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="KG" AND Account_No IN ("0006341010","0006341020","0006341030","0006341040","0006341050","0006341060") THEN SUM(Weight_Qty)
                WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="L15" AND Account_No IN ("0006341010","0006341020","0006341030","0006341040","0006341050","0006341060") THEN SUM(Volume_Qty)
                WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="GAL" AND Account_No IN ("0006341010","0006341020","0006341030","0006341040","0006341050","0006341060") THEN SUM(Volume_Qty_GAL)
                WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="BBL" AND Account_No IN ("0006341010","0006341020","0006341030","0006341040","0006341050","0006341060") THEN SUM(Volume_Qty_BB6) 
                ELSE "" END AS STOCK_OPENING_QTY,


        CASE WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="KG" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV05","SV06") THEN SUM(Weight_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="L15" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV05","SV06")THEN SUM(Volume_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="GAL" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV05","SV06") THEN SUM(Volume_Qty_GAL)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="BBL" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV05","SV06") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS THIRD_PARTY_PURCHASES1,


        CASE WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="KG" AND Account_No IN ("0006330140","0006340020","0006342110","0006342130","0006342150","0006342200","0006342510","0006342520","0006342530","0006351070","0006351800","0006354200","0006900470","0007250210","0006341140","0006341141","0006341142","0006341143","0006365000") THEN SUM(Weight_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="L15" AND Account_No IN ("0006330140","0006340020","0006342110","0006342130","0006342150","0006342200","0006342510","0006342520","0006342530","0006351070","0006351800","0006354200","0006900470","0007250210","0006341140","0006341141","0006341142","0006341143","0006365000") THEN SUM(Volume_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="GAL" AND Account_No IN ("0006330140","0006340020","0006342110","0006342130","0006342150","0006342200","0006342510","0006342520","0006342530","0006351070","0006351800","0006354200","0006900470","0007250210","0006341140","0006341141","0006341142","0006341143","0006365000") THEN SUM(Volume_Qty_GAL)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="BBL" AND Account_No IN ("0006330140","0006340020","0006342110","0006342130","0006342150","0006342200","0006342510","0006342520","0006342530","0006351070","0006351800","0006354200","0006900470","0007250210","0006341140","0006341141","0006341142","0006341143","0006365000") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS THIRD_PARTY_PURCHASES2,

        CASE WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="KG" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV01","SV02","SV60","SV61") THEN SUM(Weight_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="L15" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV01","SV02","SV60","SV61")THEN SUM(Volume_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="GAL" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV01","SV02","SV60","SV61") THEN SUM(Volume_Qty_GAL)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="BBL" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV01","SV02","SV60","SV61") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IG_PURCHASE_QTY1,


        CASE WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="KG" AND Account_No IN ("0006342100","0006342050","0006342120") THEN SUM(Weight_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="L15" AND Account_No IN ("0006342100","0006342050","0006342120") THEN SUM(Volume_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="GAL" AND Account_No IN ("0006342100","0006342050","0006342120") THEN SUM(Volume_Qty_GAL)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="BBL" AND Account_No IN ("0006342100","0006342050","0006342120") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IG_PURCHASE_QTY2,


        CASE WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="KG" AND Account_No IN ("0006000100","0006000150","0006000200","0006000250","0006000300","0006000400","0006000500","0006000600","0006002105","0006009000","0006009050","0006009100","0006140100","0006140105","0006140200","0006140300","0006140500","0006140700","0006342550") THEN SUM(Weight_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="L15" AND Account_No IN ("0006000100","0006000150","0006000200","0006000250","0006000300","0006000400","0006000500","0006000600","0006002105","0006009000","0006009050","0006009100","0006140100","0006140105","0006140200","0006140300","0006140500","0006140700","0006342550") THEN SUM(Volume_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="GAL" AND Account_No IN ("0006000100","0006000150","0006000200","0006000250","0006000300","0006000400","0006000500","0006000600","0006002105","0006009000","0006009050","0006009100","0006140100","0006140105","0006140200","0006140300","0006140500","0006140700","0006342550") THEN SUM(Volume_Qty_GAL)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="BBL" AND Account_No IN ("0006000100","0006000150","0006000200","0006000250","0006000300","0006000400","0006000500","0006000600","0006002105","0006009000","0006009050","0006009100","0006140100","0006140105","0006140200","0006140300","0006140500","0006140700","0006342550") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS THIRD_PARTY_SALES_QTY,

        CASE WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="KG" AND Account_No IN ("0006380302","0006380303") AND Functional_Area ="SV46" THEN SUM(Weight_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="L15" AND Account_No IN ("0006380302","0006380303") AND Functional_Area ="SV46" THEN SUM(Volume_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="GAL" AND Account_No IN ("0006380302","0006380303") AND Functional_Area ="SV46" THEN SUM(Volume_Qty_GAL)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="BBL" AND Account_No IN ("0006380302","0006380303") AND Functional_Area ="SV46" THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS INTER_LOB_SALES_QTY,



        CASE WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="KG" AND Account_No IN ("0006001500","0006001700","0006001900","0006002100") THEN SUM(Weight_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="L15" AND Account_No IN ("0006001500","0006001700","0006001900","0006002100") THEN SUM(Volume_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="GAL" AND Account_No IN ("0006001500","0006001700","0006001900","0006002100") THEN SUM(Volume_Qty_GAL)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="BBL" AND Account_No IN ("0006001500","0006001700","0006001900","0006002100") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IG_SALES_QTY,


        CASE WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="KG" AND Account_No IN ("0006342000","0006340010") AND Functional_Area IN ("SV10","SV11","SV12","SV50","SV51") THEN SUM(Weight_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="L15" AND Account_No IN ("0006342000","0006340010") AND Functional_Area IN ("SV10","SV11","SV12","SV50","SV51")THEN SUM(Volume_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="GAL" AND Account_No IN ("0006342000","0006340010") AND Functional_Area IN ("SV10","SV11","SV12","SV50","SV51") THEN SUM(Volume_Qty_GAL)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="BBL" AND Account_No IN ("0006342000","0006340010") AND Functional_Area IN ("SV10","SV11","SV12","SV50","SV51") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS EXG_PURCHASE_QTY,


        CASE WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="KG" AND Account_No ="0006002220" THEN SUM(Weight_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="L15" AND Account_No ="0006002220" THEN SUM(Volume_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="GAL" AND Account_No ="0006002220" THEN SUM(Volume_Qty_GAL)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="BBL" AND Account_No ="0006002220" THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS EXG_SALES_QTY,



        CASE WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="KG" AND Account_No ="0006380201" THEN SUM(Weight_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="L15" AND Account_No ="0006380201" THEN SUM(Volume_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="GAL" AND Account_No ="0006380201" THEN SUM(Volume_Qty_GAL)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="BBL" AND Account_No ="0006380201" THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IS_PURCHASES_QTY1,




        CASE WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="KG" AND Account_No = "0006380202" AND Functional_Area IN ("SV44","SV45","SV48","SV56","SV58","") THEN SUM(Weight_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="L15" AND Account_No = "0006380202" AND Functional_Area IN ("SV44","SV45","SV48","SV56","SV58","")THEN SUM(Volume_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="GAL" AND Account_No = "0006380202" AND Functional_Area IN ("SV44","SV45","SV48","SV56","SV58","") THEN SUM(Volume_Qty_GAL)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="BBL" AND Account_No = "0006380202" AND Functional_Area IN ("SV44","SV45","SV48","SV56","SV58","") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IS_PURCHASES_QTY2,



        CASE WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="KG" AND Account_No = "0006380203" AND Functional_Area IN ("SV03","SV48","SV49","SV53","SV55","") THEN SUM(Weight_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="L15" AND Account_No = "0006380203" AND Functional_Area IN ("SV03","SV48","SV49","SV53","SV55","")THEN SUM(Volume_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="GAL" AND Account_No = "0006380203" AND Functional_Area IN ("SV03","SV48","SV49","SV53","SV55","") THEN SUM(Volume_Qty_GAL)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="BBL" AND Account_No = "0006380203" AND Functional_Area IN ("SV03","SV48","SV49","SV53","SV55","") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IS_PURCHASES_QTY3,


        CASE WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="KG" AND Account_No = "0006380302" AND Functional_Area IN ("SV33","SV57") THEN SUM(Weight_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="L15" AND Account_No = "0006380302" AND Functional_Area IN ("SV33","SV57")THEN SUM(Volume_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="GAL" AND Account_No = "0006380302" AND Functional_Area IN ("SV33","SV57") THEN SUM(Volume_Qty_GAL)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="BBL" AND Account_No = "0006380302" AND Functional_Area IN ("SV33","SV57") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IS_PURCHASES_QTY4,



        CASE WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="KG" AND Account_No = "0006380303" AND Functional_Area="SV54" THEN SUM(Weight_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="L15" AND Account_No = "0006380303" AND Functional_Area ="SV54" THEN SUM(Volume_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="GAL" AND Account_No = "0006380303" AND Functional_Area ="SV54" THEN SUM(Volume_Qty_GAL)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="BBL" AND Account_No = "0006380303" AND Functional_Area ="SV54" THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IS_PURCHASES_QTY5,


        CASE WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="KG" AND Account_No IN ("0006380100","0006380300") THEN SUM(Weight_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="L15" AND Account_No IN ("0006380100","0006380300") THEN SUM(Volume_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="GAL" AND Account_No IN ("0006380100","0006380300") THEN SUM(Volume_Qty_GAL)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="BBL" AND Account_No IN ("0006380100","0006380300") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IS_SALES_QTY1,


        CASE WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="KG" AND Account_No IN("0006380202","0006380203") AND Functional_Area="SV49" THEN SUM(Weight_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="L15" AND Account_No IN("0006380202","0006380203") AND Functional_Area ="SV49" THEN SUM(Volume_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="GAL" AND Account_No IN("0006380202","0006380203") AND Functional_Area ="SV49" THEN SUM(Volume_Qty_GAL)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="BBL" AND Account_No IN("0006380202","0006380203") AND Functional_Area ="SV49" THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IS_SALES_QTY2,


        CASE WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="KG" AND Account_No = "0006380303" AND Functional_Area IN ("SV03","SV32","SV47","") THEN SUM(Weight_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="L15" AND Account_No = "0006380303" AND Functional_Area IN ("SV03","SV32","SV47","")THEN SUM(Volume_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="GAL" AND Account_No = "0006380303" AND Functional_Area IN ("SV03","SV32","SV47","") THEN SUM(Volume_Qty_GAL)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="BBL" AND Account_No = "0006380303" AND Functional_Area IN ("SV03","SV32","SV47","") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IS_SALES_QTY3,


        CASE WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="KG" AND Account_No = "0006380302" AND Functional_Area IN ("SV34","SV47","") THEN SUM(Weight_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="L15" AND Account_No = "0006380302" AND Functional_Area IN ("SV34","SV47","")THEN SUM(Volume_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="GAL" AND Account_No = "0006380302" AND Functional_Area IN ("SV34","SV47","") THEN SUM(Volume_Qty_GAL)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="BBL" AND Account_No = "0006380302" AND Functional_Area IN ("SV34","SV47","") THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS IS_SALES_QTY4,


        CASE WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="KG" AND Account_No = "0006380203" AND Functional_Area ="SV30" THEN SUM(Weight_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="L15" AND Account_No = "0006380203" AND Functional_Area ="SV30"THEN SUM(Volume_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="GAL" AND Account_No = "0006380203" AND Functional_Area ="SV30" THEN SUM(Volume_Qty_GAL)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="BBL" AND Account_No = "0006380203" AND Functional_Area ="SV30" THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS INTERNAL_SC_FROM_QTY,



        CASE WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="KG" AND Account_No = "0006380303" AND Functional_Area ="SV31" THEN SUM(Weight_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="L15" AND Account_No = "0006380303" AND Functional_Area ="SV31"THEN SUM(Volume_Qty)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="GAL" AND Account_No = "0006380303" AND Functional_Area ="SV31" THEN SUM(Volume_Qty_GAL)
                    WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="BBL" AND Account_No = "0006380303" AND Functional_Area ="SV31" THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS INTERNAL_SC_TO_QTY,




        CASE WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="KG" AND Account_No IN ("0006341810","0006341820","0006341830","0006341840","0006341850","0006341860") THEN SUM(Weight_Qty)
                WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="L15" AND Account_No IN ("0006341810","0006341820","0006341830","0006341840","0006341850","0006341860") THEN SUM(Volume_Qty)
                WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="GAL" AND Account_No IN ("0006341810","0006341820","0006341830","0006341840","0006341850","0006341860") THEN SUM(Volume_Qty_GAL)
                WHEN {massbalance_per_commodity_grade_security_function_name}.UOM="BBL" AND Account_No IN ("0006341810","0006341820","0006341830","0006341840","0006341850","0006341860") THEN SUM(Volume_Qty_BB6) 
                ELSE "" END AS STOCK_CLOSE_QTY

        FROM temp 
        GROUP BY Commodity_Code,COMMODITY_GRADE, Account_No, Functional_Area
    )
    GROUP BY Commodity_Code,COMMODITY_GRADE)""")

    assignFunctionPermission(uc_catalog_name, uc_curated_schema, massbalance_per_commodity_grade_security_function_name, tbl_owner_grp,security_end_user_aad_group_name, security_object_aad_group_name, security_functional_dev_aad_group, security_functional_readers_aad_group)

# COMMAND ----------

internal_transfers_security_func_name = uc_catalog_name+"."+uc_curated_schema+"."+internal_transfers_security_function_name
if internal_transfers_security_func_name in function_list:
    spark.sql(f"""DROP FUNCTION `{uc_catalog_name}`.`{uc_curated_schema}`.{internal_transfers_security_function_name}""")
    spark.sql(f"""
    CREATE OR REPLACE FUNCTION `{uc_catalog_name}`.`{uc_curated_schema}`.{internal_transfers_security_function_name}(`Company_Code-Text` STRING,`Profit_Center-Text` STRING, Fiscal_Year STRING, Posting_Period STRING, Division STRING, Account_No STRING, Document_Type STRING, `Plant-Text` STRING,Mvmt_Type STRING, CURRENCY STRING, UOM STRING, Display_Unit STRING) 

    RETURNS TABLE (PROFIT_CENTER STRING, PLANT STRING,MM_PLANT STRING, MM_RECEIVING_PLANT STRING, MATERIAL_KEY STRING, MM_MATERIAL_KEY STRING,MM_MOVEMENT_TYPE STRING, INTERNAL_SC_FROM_QTY decimal(25,2),INTERNAL_SC_TO_QUANTITY decimal(25,2), INTERNAL_SC_FROM decimal(25,2), INTERNAL_SC_TO decimal(25,2) )

    RETURN 

    with temp as (
    select CL_Profit_Center AS PROFIT_CENTER, Plant AS PLANT, MM_Plant AS MM_PLANT, MM_Receiving_Plant AS MM_RECEIVING_PLANT, Material_Key AS MATERIAL_KEY, MM_Material_Key as MM_MATERIAL_KEY,MM_Mvmt_Type AS MM_MOVEMENT_TYPE  , Account_No,Functional_Area, Amt_Comp_Code_Curr,Amt_PC_Local_Curr, Volume_Qty_GAL, Weight_Qty, Volume_Qty, Volume_Qty_BB6
    from `{uc_catalog_name}`.`{uc_curated_schema}`.{fcb_dynamic_view_name} fcb
    WHERE
    (array_contains(SPLIT({internal_transfers_security_function_name}.`Company_Code-Text`,',') ,fcb.`Company_Code-Text`) OR ({internal_transfers_security_function_name}.`Company_Code-Text`='ALL')) AND
    (array_contains(SPLIT({internal_transfers_security_function_name}.`Profit_Center-Text`,',') ,fcb.`Profit_Center-Text`) OR ({internal_transfers_security_function_name}.`Profit_Center-Text`='ALL')) AND
    (array_contains(SPLIT({internal_transfers_security_function_name}.Fiscal_Year,',') ,fcb.Fiscal_Year) OR ({internal_transfers_security_function_name}.Fiscal_Year='ALL')) AND
    (array_contains(SPLIT({internal_transfers_security_function_name}.Posting_Period,',') ,fcb.Posting_Period) OR ({internal_transfers_security_function_name}.Posting_Period='ALL')) AND
    (array_contains(SPLIT({internal_transfers_security_function_name}.Division,',') ,fcb.Division) OR ({internal_transfers_security_function_name}.Division='ALL')) AND
    (array_contains(SPLIT({internal_transfers_security_function_name}.Account_No,',') ,fcb.Account_No) OR ({internal_transfers_security_function_name}.Account_No='ALL'))AND
    (array_contains(SPLIT({internal_transfers_security_function_name}.Document_Type,',') ,fcb.Document_Type) OR ({internal_transfers_security_function_name}.Document_Type='ALL')) AND
    (array_contains(SPLIT({internal_transfers_security_function_name}.`Plant-Text`,',') ,fcb.`Plant-Text`) OR ({internal_transfers_security_function_name}.`Plant-Text`='ALL')) AND
    (array_contains(SPLIT({internal_transfers_security_function_name}.Mvmt_Type,',') ,fcb.Mvmt_Type) OR ({internal_transfers_security_function_name}.Mvmt_Type='ALL')) AND
    (Functional_Area="SV30" OR Functional_Area="SV31")
    AND fcb.OPFLAG != 'D'
    )

    SELECT PROFIT_CENTER , PLANT ,MM_PLANT, MM_RECEIVING_PLANT, MATERIAL_KEY, MM_MATERIAL_KEY,MM_MOVEMENT_TYPE,
    CASE WHEN {internal_transfers_security_function_name}.Display_Unit = 'Actual Value' THEN `INTERNAL_SC_FROM_QTY`
    WHEN {internal_transfers_security_function_name}.Display_Unit = 'Thousands' THEN `INTERNAL_SC_FROM_QTY`*0.001
    WHEN {internal_transfers_security_function_name}.Display_Unit = 'Millions' THEN `INTERNAL_SC_FROM_QTY`*0.000001
    WHEN {internal_transfers_security_function_name}.Display_Unit = 'Billions' THEN `INTERNAL_SC_FROM_QTY`*0.000000001
    END    AS `INTERNAL_SC_FROM_QTY`,

    CASE WHEN {internal_transfers_security_function_name}.Display_Unit = 'Actual Value' THEN `INTERNAL_SC_TO_QTY`
    WHEN {internal_transfers_security_function_name}.Display_Unit = 'Thousands' THEN `INTERNAL_SC_TO_QTY`*0.001
    WHEN {internal_transfers_security_function_name}.Display_Unit = 'Millions' THEN `INTERNAL_SC_TO_QTY`*0.000001
    WHEN {internal_transfers_security_function_name}.Display_Unit = 'Billions' THEN `INTERNAL_SC_TO_QTY`*0.000000001
    END    AS `INTERNAL_SC_TO_QTY`,

    CASE WHEN {internal_transfers_security_function_name}.Display_Unit = 'Actual Value' THEN `INTERNAL_SC_FROM`
    WHEN {internal_transfers_security_function_name}.Display_Unit = 'Thousands' THEN `INTERNAL_SC_FROM`*0.001
    WHEN {internal_transfers_security_function_name}.Display_Unit = 'Millions' THEN `INTERNAL_SC_FROM`*0.000001
    WHEN {internal_transfers_security_function_name}.Display_Unit = 'Billions' THEN `INTERNAL_SC_FROM`*0.000000001
    END    AS `INTERNAL_SC_FROM`,

    CASE WHEN {internal_transfers_security_function_name}.Display_Unit = 'Actual Value' THEN `INTERNAL_SC_TO`
    WHEN {internal_transfers_security_function_name}.Display_Unit = 'Thousands' THEN `INTERNAL_SC_TO`*0.001
    WHEN {internal_transfers_security_function_name}.Display_Unit = 'Millions' THEN `INTERNAL_SC_TO`*0.000001
    WHEN {internal_transfers_security_function_name}.Display_Unit = 'Billions' THEN `INTERNAL_SC_TO`*0.000000001
    END    AS `INTERNAL_SC_TO`

    FROM
    (
    SELECT  PROFIT_CENTER , PLANT ,MM_PLANT, MM_RECEIVING_PLANT, MATERIAL_KEY, MM_MATERIAL_KEY,MM_MOVEMENT_TYPE, -1*SUM(INTERNAL_SC_FROM_QTY) AS INTERNAL_SC_FROM_QTY, -1*SUM(INTERNAL_SC_TO_QTY) AS INTERNAL_SC_TO_QTY, -1*SUM(INTERNAL_SC_FROM) AS INTERNAL_SC_FROM, -1*SUM(INTERNAL_SC_TO) AS INTERNAL_SC_TO

    FROM 
    (SELECT PROFIT_CENTER , PLANT ,MM_PLANT, MM_RECEIVING_PLANT, MATERIAL_KEY, MM_MATERIAL_KEY,MM_MOVEMENT_TYPE, Account_No, Functional_Area,
                
        
        CASE WHEN {internal_transfers_security_function_name}.UOM="KG" AND Account_No = "0006380203" AND Functional_Area ="SV30" THEN SUM(Weight_Qty)
                    WHEN {internal_transfers_security_function_name}.UOM="L15" AND Account_No = "0006380203" AND Functional_Area ="SV30"THEN SUM(Volume_Qty)
                    WHEN {internal_transfers_security_function_name}.UOM="GAL" AND Account_No = "0006380203" AND Functional_Area ="SV30" THEN SUM(Volume_Qty_GAL)
                    WHEN {internal_transfers_security_function_name}.UOM="BBL" AND Account_No = "0006380203" AND Functional_Area ="SV30" THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS INTERNAL_SC_FROM_QTY,


        CASE WHEN {internal_transfers_security_function_name}.UOM="KG" AND Account_No = "0006380303" AND Functional_Area ="SV31" THEN SUM(Weight_Qty)
                    WHEN {internal_transfers_security_function_name}.UOM="L15" AND Account_No = "0006380303" AND Functional_Area ="SV31"THEN SUM(Volume_Qty)
                    WHEN {internal_transfers_security_function_name}.UOM="GAL" AND Account_No = "0006380303" AND Functional_Area ="SV31" THEN SUM(Volume_Qty_GAL)
                    WHEN {internal_transfers_security_function_name}.UOM="BBL" AND Account_No = "0006380303" AND Functional_Area ="SV31" THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS INTERNAL_SC_TO_QTY,

        CASE WHEN {internal_transfers_security_function_name}.CURRENCY="Local Currency" AND Account_No = "0006380203" AND Functional_Area ="SV30" THEN SUM(Amt_Comp_Code_Curr)
                    WHEN {internal_transfers_security_function_name}.CURRENCY="Group Currency" AND Account_No = "0006380203" AND Functional_Area ="SV30"THEN SUM(Amt_PC_Local_Curr)
                    ELSE "" END AS INTERNAL_SC_FROM,

        CASE WHEN {internal_transfers_security_function_name}.CURRENCY="Local Currency" AND Account_No = "0006380303" AND Functional_Area ="SV31" THEN SUM(Amt_Comp_Code_Curr)
                    WHEN {internal_transfers_security_function_name}.CURRENCY="Group Currency" AND Account_No = "0006380303" AND Functional_Area ="SV31"THEN SUM(Amt_PC_Local_Curr)
                    ELSE "" END AS INTERNAL_SC_TO

        FROM temp 
        GROUP BY PROFIT_CENTER , PLANT ,MM_PLANT, MM_RECEIVING_PLANT, MATERIAL_KEY, MM_MATERIAL_KEY,MM_MOVEMENT_TYPE, Account_No, Functional_Area
    )
    GROUP BY PROFIT_CENTER , PLANT ,MM_PLANT, MM_RECEIVING_PLANT, MATERIAL_KEY, MM_MATERIAL_KEY,MM_MOVEMENT_TYPE)""")

    assignFunctionPermission(uc_catalog_name, uc_curated_schema, internal_transfers_security_function_name, tbl_owner_grp,security_end_user_aad_group_name, security_object_aad_group_name, security_functional_dev_aad_group, security_functional_readers_aad_group)

    print("internal_transfers_security function dropped and recreated in the schema")
else:
    spark.sql(f"""
    CREATE OR REPLACE FUNCTION `{uc_catalog_name}`.`{uc_curated_schema}`.{internal_transfers_security_function_name}(`Company_Code-Text` STRING,`Profit_Center-Text` STRING, Fiscal_Year STRING, Posting_Period STRING, Division STRING, Account_No STRING, Document_Type STRING, `Plant-Text` STRING,Mvmt_Type STRING, CURRENCY STRING, UOM STRING, Display_Unit STRING) 

    RETURNS TABLE (PROFIT_CENTER STRING, PLANT STRING,MM_PLANT STRING, MM_RECEIVING_PLANT STRING, MATERIAL_KEY STRING, MM_MATERIAL_KEY STRING,MM_MOVEMENT_TYPE STRING, INTERNAL_SC_FROM_QTY decimal(25,2),INTERNAL_SC_TO_QUANTITY decimal(25,2), INTERNAL_SC_FROM decimal(25,2), INTERNAL_SC_TO decimal(25,2) )

    RETURN 

    with temp as (
    select CL_Profit_Center AS PROFIT_CENTER, Plant AS PLANT, MM_Plant AS MM_PLANT, MM_Receiving_Plant AS MM_RECEIVING_PLANT, Material_Key AS MATERIAL_KEY, MM_Material_Key as MM_MATERIAL_KEY,MM_Mvmt_Type AS MM_MOVEMENT_TYPE  , Account_No,Functional_Area, Amt_Comp_Code_Curr,Amt_PC_Local_Curr, Volume_Qty_GAL, Weight_Qty, Volume_Qty, Volume_Qty_BB6
    from `{uc_catalog_name}`.`{uc_curated_schema}`.{fcb_dynamic_view_name} fcb
    WHERE
    (array_contains(SPLIT({internal_transfers_security_function_name}.`Company_Code-Text`,',') ,fcb.`Company_Code-Text`) OR ({internal_transfers_security_function_name}.`Company_Code-Text`='ALL')) AND
    (array_contains(SPLIT({internal_transfers_security_function_name}.`Profit_Center-Text`,',') ,fcb.`Profit_Center-Text`) OR ({internal_transfers_security_function_name}.`Profit_Center-Text`='ALL')) AND
    (array_contains(SPLIT({internal_transfers_security_function_name}.Fiscal_Year,',') ,fcb.Fiscal_Year) OR ({internal_transfers_security_function_name}.Fiscal_Year='ALL')) AND
    (array_contains(SPLIT({internal_transfers_security_function_name}.Posting_Period,',') ,fcb.Posting_Period) OR ({internal_transfers_security_function_name}.Posting_Period='ALL')) AND
    (array_contains(SPLIT({internal_transfers_security_function_name}.Division,',') ,fcb.Division) OR ({internal_transfers_security_function_name}.Division='ALL')) AND
    (array_contains(SPLIT({internal_transfers_security_function_name}.Account_No,',') ,fcb.Account_No) OR ({internal_transfers_security_function_name}.Account_No='ALL'))AND
    (array_contains(SPLIT({internal_transfers_security_function_name}.Document_Type,',') ,fcb.Document_Type) OR ({internal_transfers_security_function_name}.Document_Type='ALL')) AND
    (array_contains(SPLIT({internal_transfers_security_function_name}.`Plant-Text`,',') ,fcb.`Plant-Text`) OR ({internal_transfers_security_function_name}.`Plant-Text`='ALL')) AND
    (array_contains(SPLIT({internal_transfers_security_function_name}.Mvmt_Type,',') ,fcb.Mvmt_Type) OR ({internal_transfers_security_function_name}.Mvmt_Type='ALL')) AND
    (Functional_Area="SV30" OR Functional_Area="SV31")
    AND fcb.OPFLAG != 'D'
    )
    SELECT PROFIT_CENTER , PLANT ,MM_PLANT, MM_RECEIVING_PLANT, MATERIAL_KEY, MM_MATERIAL_KEY,MM_MOVEMENT_TYPE,
    CASE WHEN {internal_transfers_security_function_name}.Display_Unit = 'Actual Value' THEN `INTERNAL_SC_FROM_QTY`
    WHEN {internal_transfers_security_function_name}.Display_Unit = 'Thousands' THEN `INTERNAL_SC_FROM_QTY`*0.001
    WHEN {internal_transfers_security_function_name}.Display_Unit = 'Millions' THEN `INTERNAL_SC_FROM_QTY`*0.000001
    WHEN {internal_transfers_security_function_name}.Display_Unit = 'Billions' THEN `INTERNAL_SC_FROM_QTY`*0.000000001
    END
    AS `INTERNAL_SC_FROM_QTY`,

    CASE WHEN {internal_transfers_security_function_name}.Display_Unit = 'Actual Value' THEN `INTERNAL_SC_TO_QTY`
    WHEN {internal_transfers_security_function_name}.Display_Unit = 'Thousands' THEN `INTERNAL_SC_TO_QTY`*0.001
    WHEN {internal_transfers_security_function_name}.Display_Unit = 'Millions' THEN `INTERNAL_SC_TO_QTY`*0.000001
    WHEN {internal_transfers_security_function_name}.Display_Unit = 'Billions' THEN `INTERNAL_SC_TO_QTY`*0.000000001
    END
    AS `INTERNAL_SC_TO_QTY`,

    CASE WHEN {internal_transfers_security_function_name}.Display_Unit = 'Actual Value' THEN `INTERNAL_SC_FROM`
    WHEN {internal_transfers_security_function_name}.Display_Unit = 'Thousands' THEN `INTERNAL_SC_FROM`*0.001
    WHEN {internal_transfers_security_function_name}.Display_Unit = 'Millions' THEN `INTERNAL_SC_FROM`*0.000001
    WHEN {internal_transfers_security_function_name}.Display_Unit = 'Billions' THEN `INTERNAL_SC_FROM`*0.000000001
    END
    AS `INTERNAL_SC_FROM`,

    CASE WHEN {internal_transfers_security_function_name}.Display_Unit = 'Actual Value' THEN `INTERNAL_SC_TO`
    WHEN {internal_transfers_security_function_name}.Display_Unit = 'Thousands' THEN `INTERNAL_SC_TO`*0.001
    WHEN {internal_transfers_security_function_name}.Display_Unit = 'Millions' THEN `INTERNAL_SC_TO`*0.000001
    WHEN {internal_transfers_security_function_name}.Display_Unit = 'Billions' THEN `INTERNAL_SC_TO`*0.000000001
    END
    AS `INTERNAL_SC_TO`

    FROM
    (
    SELECT  PROFIT_CENTER , PLANT ,MM_PLANT, MM_RECEIVING_PLANT, MATERIAL_KEY, MM_MATERIAL_KEY,MM_MOVEMENT_TYPE, -1*SUM(INTERNAL_SC_FROM_QTY) AS INTERNAL_SC_FROM_QTY, -1*SUM(INTERNAL_SC_TO_QTY) AS INTERNAL_SC_TO_QTY, -1*SUM(INTERNAL_SC_FROM) AS INTERNAL_SC_FROM, -1*SUM(INTERNAL_SC_TO) AS INTERNAL_SC_TO

    FROM 
    (SELECT PROFIT_CENTER , PLANT ,MM_PLANT, MM_RECEIVING_PLANT, MATERIAL_KEY, MM_MATERIAL_KEY,MM_MOVEMENT_TYPE, Account_No, Functional_Area,
                
        
        CASE WHEN {internal_transfers_security_function_name}.UOM="KG" AND Account_No = "0006380203" AND Functional_Area ="SV30" THEN SUM(Weight_Qty)
                    WHEN {internal_transfers_security_function_name}.UOM="L15" AND Account_No = "0006380203" AND Functional_Area ="SV30"THEN SUM(Volume_Qty)
                    WHEN {internal_transfers_security_function_name}.UOM="GAL" AND Account_No = "0006380203" AND Functional_Area ="SV30" THEN SUM(Volume_Qty_GAL)
                    WHEN {internal_transfers_security_function_name}.UOM="BBL" AND Account_No = "0006380203" AND Functional_Area ="SV30" THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS INTERNAL_SC_FROM_QTY,


        CASE WHEN {internal_transfers_security_function_name}.UOM="KG" AND Account_No = "0006380303" AND Functional_Area ="SV31" THEN SUM(Weight_Qty)
                    WHEN {internal_transfers_security_function_name}.UOM="L15" AND Account_No = "0006380303" AND Functional_Area ="SV31"THEN SUM(Volume_Qty)
                    WHEN {internal_transfers_security_function_name}.UOM="GAL" AND Account_No = "0006380303" AND Functional_Area ="SV31" THEN SUM(Volume_Qty_GAL)
                    WHEN {internal_transfers_security_function_name}.UOM="BBL" AND Account_No = "0006380303" AND Functional_Area ="SV31" THEN SUM(Volume_Qty_BB6) 
                    ELSE "" END AS INTERNAL_SC_TO_QTY,

        CASE WHEN {internal_transfers_security_function_name}.CURRENCY="Local Currency" AND Account_No = "0006380203" AND Functional_Area ="SV30" THEN SUM(Amt_Comp_Code_Curr)
                    WHEN {internal_transfers_security_function_name}.CURRENCY="Group Currency" AND Account_No = "0006380203" AND Functional_Area ="SV30"THEN SUM(Amt_PC_Local_Curr)
                    ELSE "" END AS INTERNAL_SC_FROM,

        CASE WHEN {internal_transfers_security_function_name}.CURRENCY="Local Currency" AND Account_No = "0006380303" AND Functional_Area ="SV31" THEN SUM(Amt_Comp_Code_Curr)
                    WHEN {internal_transfers_security_function_name}.CURRENCY="Group Currency" AND Account_No = "0006380303" AND Functional_Area ="SV31"THEN SUM(Amt_PC_Local_Curr)
                    ELSE "" END AS INTERNAL_SC_TO

        FROM temp 
        GROUP BY PROFIT_CENTER , PLANT ,MM_PLANT, MM_RECEIVING_PLANT, MATERIAL_KEY, MM_MATERIAL_KEY,MM_MOVEMENT_TYPE, Account_No, Functional_Area
    )
    GROUP BY PROFIT_CENTER , PLANT ,MM_PLANT, MM_RECEIVING_PLANT, MATERIAL_KEY, MM_MATERIAL_KEY,MM_MOVEMENT_TYPE)""")

    assignFunctionPermission(uc_catalog_name, uc_curated_schema, internal_transfers_security_function_name, tbl_owner_grp,security_end_user_aad_group_name, security_object_aad_group_name, security_functional_dev_aad_group, security_functional_readers_aad_group)

# COMMAND ----------

fcb_per_commodity_grade_security_func_name = uc_catalog_name+"."+uc_curated_schema+"."+fcb_per_commodity_grade_security_function_name
if fcb_per_commodity_grade_security_func_name in function_list:
    spark.sql(f"""DROP FUNCTION `{uc_catalog_name}`.`{uc_curated_schema}`.{fcb_per_commodity_grade_security_function_name}""")
    spark.sql(f"""
    CREATE OR REPLACE FUNCTION `{uc_catalog_name}`.`{uc_curated_schema}`.{fcb_per_commodity_grade_security_function_name}(`Company_Code-Text` STRING,`Profit_Center-Text` STRING, Fiscal_Year STRING, Posting_Period STRING, Division STRING, Account_No STRING, Document_Type STRING, `Plant-Text` STRING, CURRENCY STRING, UOM STRING,Display_Unit STRING) 

    RETURNS TABLE (Commodity_Code STRING, COMMODITY_GRADE STRING, ACQUISITION_COSTS decimal(25,2), SALES_PROCEEDS decimal(25,2),EXCHANGES decimal(25,2), TRANSFERS decimal(25,2),C1_MARGIN_CASH decimal(25,2), PRICE_EFFECT decimal(25,2), STOCK_EFFECT decimal(25,2), C1_MARGIN_FIFO decimal(25,2), COSA_Adj decimal(25,2), COSA_PRICE_EFFECT decimal(25,2), RESIDUAL_CCS_VOL_EFFECT decimal(25,2), C1_MARGIN_CCS decimal(25,2))

    RETURN 

    with temp as (
    select COALESCE(Commodity_Code,'') AS Commodity_Code, COALESCE(Commodity_Code_Text,'') AS COMMODITY_GRADE, Account_No,Functional_Area, Posting_Period, Material_Number,Plant, Amt_Comp_Code_Curr,Amt_PC_Local_Curr, Volume_Qty_GAL, Weight_Qty, Volume_Qty, Volume_Qty_BB6
    from `{uc_catalog_name}`.`{uc_curated_schema}`.{fcb_dynamic_view_name} fcb
    WHERE
    (array_contains(SPLIT({fcb_per_commodity_grade_security_function_name}.`Company_Code-Text`,',') ,fcb.`Company_Code-Text`) OR ({fcb_per_commodity_grade_security_function_name}.`Company_Code-Text`='ALL')) AND
    (array_contains(SPLIT({fcb_per_commodity_grade_security_function_name}.`Profit_Center-Text`,',') ,fcb.`Profit_Center-Text`) OR ({fcb_per_commodity_grade_security_function_name}.`Profit_Center-Text`='ALL')) AND
    (array_contains(SPLIT({fcb_per_commodity_grade_security_function_name}.Fiscal_Year,',') ,fcb.Fiscal_Year) OR ({fcb_per_commodity_grade_security_function_name}.Fiscal_Year='ALL')) AND
    (array_contains(SPLIT({fcb_per_commodity_grade_security_function_name}.Posting_Period,',') ,fcb.Posting_Period) OR ({fcb_per_commodity_grade_security_function_name}.Posting_Period='ALL')) AND
    (array_contains(SPLIT({fcb_per_commodity_grade_security_function_name}.Division,',') ,fcb.Division) OR ({fcb_per_commodity_grade_security_function_name}.Division='ALL')) AND
    (array_contains(SPLIT({fcb_per_commodity_grade_security_function_name}.Account_No,',') ,fcb.Account_No) OR ({fcb_per_commodity_grade_security_function_name}.Account_No='ALL'))AND
    (array_contains(SPLIT({fcb_per_commodity_grade_security_function_name}.Document_Type,',') ,fcb.Document_Type) OR ({fcb_per_commodity_grade_security_function_name}.Document_Type='ALL')) AND
    (array_contains(SPLIT({fcb_per_commodity_grade_security_function_name}.`Plant-Text`,',') ,fcb.`Plant-Text`) OR ({fcb_per_commodity_grade_security_function_name}.`Plant-Text`='ALL'))
    AND fcb.OPFLAG != 'D')





    SELECT Commodity_Code, COMMODITY_GRADE,
        CASE WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Actual Value' THEN ACQUISITION_COSTS
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Thousands' THEN ACQUISITION_COSTS*0.001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Millions' THEN ACQUISITION_COSTS*0.000001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Billions' THEN ACQUISITION_COSTS*0.000000001
        END AS ACQUISITION_COSTS,
        CASE WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Actual Value' THEN SALES_PROCEEDS
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Thousands' THEN SALES_PROCEEDS*0.001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Millions' THEN SALES_PROCEEDS*0.000001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Billions' THEN SALES_PROCEEDS*0.000000001
        END AS SALES_PROCEEDS,
        CASE WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Actual Value' THEN EXCHANGES
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Thousands' THEN EXCHANGES*0.001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Millions' THEN EXCHANGES*0.000001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Billions' THEN EXCHANGES*0.000000001
        END AS EXCHANGES,
        CASE WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Actual Value' THEN TRANSFERS
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Thousands' THEN TRANSFERS*0.001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Millions' THEN TRANSFERS*0.000001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Billions' THEN TRANSFERS*0.000000001
        END AS TRANSFERS,
        CASE WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Actual Value' THEN C1_MARGIN_CASH
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Thousands' THEN C1_MARGIN_CASH*0.001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Millions' THEN C1_MARGIN_CASH*0.000001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Billions' THEN C1_MARGIN_CASH*0.000000001
        END AS C1_MARGIN_CASH,
        CASE WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Actual Value' THEN PRICE_EFFECT
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Thousands' THEN PRICE_EFFECT*0.001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Millions' THEN PRICE_EFFECT*0.000001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Billions' THEN PRICE_EFFECT*0.000000001
        END AS PRICE_EFFECT,
        CASE WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Actual Value' THEN STOCK_EFFECT
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Thousands' THEN STOCK_EFFECT*0.001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Millions' THEN STOCK_EFFECT*0.000001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Billions' THEN STOCK_EFFECT*0.000000001
        END AS STOCK_EFFECT,
        CASE WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Actual Value' THEN C1_MARGIN_FIFO
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Thousands' THEN C1_MARGIN_FIFO*0.001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Millions' THEN C1_MARGIN_FIFO*0.000001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Billions' THEN C1_MARGIN_FIFO*0.000000001
        END AS C1_MARGIN_FIFO,
        CASE WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Actual Value' THEN COSA_Adj
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Thousands' THEN COSA_Adj*0.001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Millions' THEN COSA_Adj*0.000001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Billions' THEN COSA_Adj*0.000000001
        END AS COSA_Adj,
        CASE WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Actual Value' THEN COSA_PRICE_EFFECT
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Thousands' THEN COSA_PRICE_EFFECT*0.001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Millions' THEN COSA_PRICE_EFFECT*0.000001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Billions' THEN COSA_PRICE_EFFECT*0.000000001
        END AS COSA_PRICE_EFFECT,
        CASE WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Actual Value' THEN RESIDUAL_CCS_VOL_EFFECT
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Thousands' THEN RESIDUAL_CCS_VOL_EFFECT*0.001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Millions' THEN RESIDUAL_CCS_VOL_EFFECT*0.000001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Billions' THEN RESIDUAL_CCS_VOL_EFFECT*0.000000001
        END AS RESIDUAL_CCS_VOL_EFFECT,
        CASE WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Actual Value' THEN C1_MARGIN_CCS
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Thousands' THEN C1_MARGIN_CCS*0.001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Millions' THEN C1_MARGIN_CCS*0.000001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Billions' THEN C1_MARGIN_CCS*0.000000001
        END AS C1_MARGIN_CCS

    FROM(
    SELECT a.Commodity_Code, a.COMMODITY_GRADE, ACQUISITION_COSTS, SALES_PROCEEDS, EXCHANGES, TRANSFERS, C1_MARGIN_CASH, PRICE_EFFECT, STOCK_EFFECT, C1_MARGIN_CASH+PRICE_EFFECT+STOCK_EFFECT AS C1_MARGIN_FIFO, COSA_Adj, (-1*PRICE_EFFECT) AS COSA_PRICE_EFFECT,  COSA_Adj+PRICE_EFFECT AS RESIDUAL_CCS_VOL_EFFECT, C1_MARGIN_FIFO+COSA_Adj AS C1_MARGIN_CCS
    FROM
    ((SELECT  Commodity_Code,COMMODITY_GRADE, COALESCE(SUM(THIRD_PARTY_PURCHASES1),0)+ COALESCE(SUM(THIRD_PARTY_PURCHASES2),0)+ COALESCE(SUM(IG_PURCHASE_AMOUNT1),0)+ COALESCE(SUM(IG_PURCHASE_AMOUNT2),0) + COALESCE(SUM(HEDGE_PAPER),0)+ COALESCE(SUM(DEMURRAGE),0)+  COALESCE(SUM(EXCISE_DUTY),0) +  COALESCE(SUM(CSO),0) +  COALESCE(SUM(STOCK_ADJUSTMENT),0)+ COALESCE(SUM(OTHER_COSTS1),0)+ COALESCE(SUM(OTHER_COSTS2),0) AS ACQUISITION_COSTS,
    COALESCE(SUM(INTER_LOB_SALES1),0)+COALESCE(SUM(INTER_LOB_SALES2),0)+COALESCE(SUM(IG_SALES),0) +COALESCE(SUM(THIRD_PARTY_SALES),0) +COALESCE(SUM(PROCEEDS_FROM_SERVICE),0) AS SALES_PROCEEDS,
    COALESCE(SUM(EXG_SALES),0)+COALESCE(SUM(EXG_PURCHASES1),0)+COALESCE(SUM(EXG_PURCHASES2),0) AS EXCHANGES,
    COALESCE(SUM(IS_PURCHASES1),0)+ COALESCE(SUM(IS_PURCHASES2),0)+ COALESCE(SUM(IS_PURCHASES3),0)+ COALESCE(SUM(IS_PURCHASES4),0) + COALESCE(SUM(IS_PURCHASES5),0)+ COALESCE(SUM(IS_PURCHASES6),0)+  COALESCE(SUM(IS_SALES1),0) +  COALESCE(SUM(IS_SALES2),0) +  COALESCE(SUM(IS_SALES3),0)+ COALESCE(SUM(IS_SALES4),0)+ COALESCE(SUM(IS_SALES5),0)+ COALESCE(SUM(INTERNAL_SC_FROM),0)+ COALESCE(SUM(INTERNAL_SC_TO),0) AS TRANSFERS, 
    ACQUISITION_COSTS+SALES_PROCEEDS+EXCHANGES+TRANSFERS AS C1_MARGIN_CASH, COALESCE(SUM(COSA_Adj),0) AS COSA_Adj

    FROM 
    (SELECT Commodity_Code,COMMODITY_GRADE, Account_No, Functional_Area,
                
        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV05","SV06") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV05","SV06") THEN SUM(Amt_PC_Local_Curr)*(-1)
                ELSE "" END AS THIRD_PARTY_PURCHASES1,





        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006330140","0006340020","0006342110","0006342130","0006342150","0006342200","0006342510","0006342520","0006342530","0006351070","0006351800","0006354200","0006900470","0007250210","0006341140","0006341141","0006341142","0006341143","0006365000") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006330140","0006340020","0006342110","0006342130","0006342150","0006342200","0006342510","0006342520","0006342530","0006351070","0006351800","0006354200","0006900470","0007250210","0006341140","0006341141","0006341142","0006341143","0006365000") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS THIRD_PARTY_PURCHASES2,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV01","SV02","SV60","SV61") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV01","SV02","SV60","SV61")THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IG_PURCHASE_AMOUNT1,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006341200","0006342100","0006342050","0006342120") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006341200","0006342100","0006342050","0006342120") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IG_PURCHASE_AMOUNT2,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006920600","0006920650","0006920675","0006920680") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006920600","0006920650","0006920675","0006920680") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS HEDGE_PAPER,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006301300","0006301310","0006301320") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006301300","0006301310","0006301320") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS DEMURRAGE,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006210010","0006210015","0006210020","0006210060","0006211200","0006211201","0006211202","0006211210","0006211300","0006210310","0006211400","0006211500","0006342410") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006210010","0006210015","0006210020","0006210060","0006211200","0006211201","0006211202","0006211210","0006211300","0006210310","0006211400","0006211500","0006342410") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS EXCISE_DUTY ,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006355100","0006355600","0007240550") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006355100","0006355600","0007240550") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS CSO,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006351400","0006351405","0006351410","0006351415","0006351600","0006351605","0006351610","0006351615") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006351400","0006351405","0006351410","0006351415","0006351600","0006351605","0006351610","0006351615") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS STOCK_ADJUSTMENT,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006211530","0006340410","0006340510","0006340520","0006341150","0006342250","0006342400","0006342405","0006342420","0006344000","0006350210","0006352030","0006354100","0006355630","0006390200","0006390210","0006390220","0006390230","0006390240","0006390250","0006390260","0006390300", "0006820200","0006820250","0007240700","0007492250","0008381500","0008381505","0008381510","0008381515","0008383500","0008383510","0008750200","0009800206","0009800207","0009800208")  THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006211530","0006340410","0006340510","0006340520","0006341150","0006342250","0006342400","0006342405","0006342420","0006344000","0006350210","0006352030","0006354100","0006355630","0006390200","0006390210","0006390220","0006390230","0006390240","0006390250","0006390260","0006390300", "0006820200","0006820250","0007240700","0007492250","0008381500","0008381505","0008381510","0008381515","0008383500","0008383510","0008750200","0009800206","0009800207","0009800208") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS OTHER_COSTS1,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Functional_Area IN ("OP57") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Functional_Area IN ("OP57") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS OTHER_COSTS2,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006380302","0006380303","0006355565") AND Functional_Area IN ("SV46") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006380302","0006380303","0006355565")AND Functional_Area IN ("SV46") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS INTER_LOB_SALES1,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006380305") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006380305") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS INTER_LOB_SALES2,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006001500","0006001700","0006001900","0006002100","0006002200","0006009002","0006110035","0006110135","0006110235","0006520350","0006520360") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006001500","0006001700","0006001900","0006002100","0006002200","0006009002","0006110035","0006110135","0006110235","0006520350","0006520360") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IG_SALES,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006000100","0006000150","0006000151","0006000152","0006000153", "0006000200","0006000250","0006000300","0006000400","0006000500","0006000600","0006000650","0006000700","0006000800","0006002105","0006002205","0006002230","0006009000","0006009005","0006009010","0006009015","0006009050","0006009100","0006009800","0006009920","0006110010","0006110015","0006110020","0006110021","0006110030","0006110031","0006110110","0006110115","0006110120","0006110121","0006110130","0006110131","0006110210","0006110215","0006110220","0006110221","0006110230","0006110231","0006110300","0006110305","0006110310","0006110320","0006110330","0006110340","0006140100","0006140105","0006140200","0006140300","0006140500","0006140700","0006140800","0006340610","0006340620","0006340710","0006342300","0006342500","0006342550","0006920460","0006920461") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006000100","0006000150","0006000151","0006000152","0006000153", "0006000200","0006000250","0006000300","0006000400","0006000500","0006000600","0006000650","0006000700","0006000800","0006002105","0006002205","0006002230","0006009000","0006009005","0006009010","0006009015","0006009050","0006009100","0006009800","0006009920","0006110010","0006110015","0006110020","0006110021","0006110030","0006110031","0006110110","0006110115","0006110120","0006110121","0006110130","0006110131","0006110210","0006110215","0006110220","0006110221","0006110230","0006110231","0006110300","0006110305","0006110310","0006110320","0006110330","0006110340","0006140100","0006140105","0006140200","0006140300","0006140500","0006140700","0006140800","0006340610","0006340620","0006340710","0006342300","0006342500","0006342550","0006920460","0006920461") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS THIRD_PARTY_SALES,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006140025","0006520050","0006520100","0006520300","0006520310","0006520400","0006590100","0006610100","0006610200","0006610300","0006610400","0006900200","0006900205","0006900250","0006900400","0007220560") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006140025","0006520050","0006520100","0006520300","0006520310","0006520400","0006590100","0006610100","0006610200","0006610300","0006610400","0006900200","0006900205","0006900250","0006900400","0007220560") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS PROCEEDS_FROM_SERVICE,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006002220","0006350510","0006190200") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006002220","0006350510","0006190200") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS EXG_SALES, 

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006342000","0006340010") AND Functional_Area IN ("SV10","SV11","SV12","SV50","SV51") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006342000","0006340010")AND Functional_Area IN ("SV10","SV11","SV12","SV50","SV51") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS EXG_PURCHASES1,





        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0008399100","0006350520") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0008399100","0006350520") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS EXG_PURCHASES2,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006380200","0006380201","0006380306","0006390100","0006390400") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006380200","0006380201","0006380306","0006390100","0006390400") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IS_PURCHASES1,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No = "0006355595" AND Functional_Area IN (" ","SV44","SV45","SV48","SV49") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No = "0006355595" AND Functional_Area IN (" ","SV44","SV45","SV48","SV49") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IS_PURCHASES2,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No = "0006380202" AND Functional_Area IN ("SV44","SV45","SV48","SV56","SV58"," ") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No = "0006380202" AND Functional_Area IN ("SV44","SV45","SV48","SV56","SV58"," ") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IS_PURCHASES3,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No = "0006380203" AND Functional_Area IN ("SV03","SV48","SV49","SV53","SV55"," ") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No = "0006380203" AND Functional_Area IN ("SV03","SV48","SV49","SV53","SV55"," ") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IS_PURCHASES4,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No = "0006380302" AND Functional_Area IN ("SV33","SV57") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No = "0006380302" AND Functional_Area IN ("SV33","SV57") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IS_PURCHASES5,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No = "0006380303" AND Functional_Area IN ("SV54") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No = "0006380303" AND Functional_Area IN ("SV54") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IS_PURCHASES6,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN("0006380100","0006380300","0006380301")  THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN("0006380100","0006380300","0006380301") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IS_SALES1,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No = "0006355095" AND Functional_Area IN ("SV48") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No = "0006355095" AND Functional_Area IN ("SV48") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IS_SALES2,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN("0006355565","0006380302") AND Functional_Area IN ("SV34","SV47","") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN("0006355565","0006380302")AND Functional_Area IN ("SV34","SV47","") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IS_SALES3,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN("0006380202","0006380203") AND Functional_Area IN ("SV49") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN("0006380202","0006380203") AND Functional_Area IN ("SV49") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IS_SALES4,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No = "0006380303" AND Functional_Area IN ("SV03","SV32","SV47","") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No = "0006380303" AND Functional_Area IN ("SV03","SV32","SV47","") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IS_SALES5,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No = "0006380203" AND Functional_Area = "SV30" THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No = "0006380203" AND Functional_Area = "SV30" THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS INTERNAL_SC_FROM,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No = "0006380303" AND Functional_Area = "SV31" THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No = "0006380303" AND Functional_Area = "SV31" THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS INTERNAL_SC_TO,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006336120","0006336110","0006380305","0006380307") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006336120","0006336110","0006380305","0006380307") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS COSA_Adj                

        FROM temp 
        GROUP BY Commodity_Code,COMMODITY_GRADE, Account_No, Functional_Area
    )
    GROUP BY Commodity_Code,COMMODITY_GRADE  
    ) a LEFT JOIN

    (SELECT Commodity_Code, COMMODITY_GRADE, SUM(PRICE_EFFECT_INTERMEDIATE) AS PRICE_EFFECT,SUM(STOCK_EFFECT_INTERMEDIATE)+SUM(STOCK_EFFECT_EXTRA) AS STOCK_EFFECT
    FROM 
        (SELECT Commodity_Code, COMMODITY_GRADE, Posting_Period, Material_Number, Plant,  
        CASE WHEN (ISNULL(CC1) OR ISNULL(UOM1) OR (UOM1=0)) AND  (ISNULL(CC2) OR ISNULL(UOM2) OR (UOM2=0)) THEN (-1*CC1)-CC2 
            WHEN ((ISNULL(UOM1) OR (UOM1=0)) AND (ISNOTNULL(CC1) AND CC1<>0)) THEN (-1*CC1)
            WHEN ((ISNULL(CC1) OR CC1=0)AND (ISNOTNULL(CC2) AND CC2<>0)) THEN 0
            WHEN (ISNULL(UOM2) OR UOM2=0 ) THEN (-1*CC2)
            ELSE (((CC2/UOM2)-(CC1/UOM1))*(-1*UOM2)) END 
        AS PRICE_EFFECT_INTERMEDIATE,

        CASE WHEN ((-1*UOM2)-UOM1)=0 OR ISNULL((-1*UOM2)-UOM1 ) THEN 0
            WHEN (ISNULL(CC1/UOM1) OR (CC1/UOM1=0)) AND (((CC1 <> 0) AND ISNOTNULL(CC1)) AND ((CC2 <> 0) AND ISNOTNULL(CC2)))
            THEN (-1*CC2) 
            WHEN (ISNULL(CC1/UOM1) OR (CC1/UOM1=0))  AND (CC1 <> 0 AND ISNOTNULL(CC1)) THEN 0
            WHEN (ISNULL(CC1/UOM1) OR (CC1/UOM1=0))  AND (CC1 = 0 OR ISNULL(CC1)) THEN (-1*CC2)
            WHEN (ISNULL(CC2/UOM2) OR (CC2/UOM2=0)) THEN -(CC1)
            WHEN (CC1 = 0 OR ISNULL(CC1)) AND (CC2 <> 0 AND ISNULL(CC2)) THEN (CC1 -CC2)
            WHEN ISNULL(((-1*UOM2)-UOM1)*(-CC1/-UOM1)) THEN 0
            ELSE (((-1*UOM2)-UOM1)*(CC1/UOM1))
            END AS STOCK_EFFECT_INTERMEDIATE,

        CASE WHEN Plant="D314" AND  Material_Number="000000000400001991" THEN ((-1*CC2)-CC1) 
            ELSE 0 END
        AS STOCK_EFFECT_EXTRA

        FROM
        (
            SELECT Commodity_Code, COMMODITY_GRADE, Posting_Period,Material_Number,Plant, SUM(CC1) AS CC1, SUM(CC2) AS CC2, SUM(UOM1) AS UOM1, SUM(UOM2) AS UOM2 
            FROM 
            (
                SELECT Commodity_Code, COMMODITY_GRADE, Posting_Period,Material_Number,Plant,Account_No, 
                CASE WHEN Account_No IN ("0006341010","0006341020","0006341030","0006341040","0006341050","0006341060") AND {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" THEN SUM(Amt_Comp_Code_Curr)  
                WHEN Account_No IN ("0006341010","0006341020","0006341030","0006341040","0006341050","0006341060") AND {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" THEN SUM(Amt_PC_Local_Curr)             
                ELSE 0 END AS CC1,

                CASE WHEN Account_No IN ("0006341010","0006341020","0006341030","0006341040","0006341050","0006341060") AND {fcb_per_commodity_grade_security_function_name}.UOM="KG" THEN SUM(Weight_Qty) 
                WHEN Account_No IN ("0006341010","0006341020","0006341030","0006341040","0006341050","0006341060") AND {fcb_per_commodity_grade_security_function_name}.UOM="L15" THEN SUM(Volume_Qty)  
                WHEN Account_No IN ("0006341010","0006341020","0006341030","0006341040","0006341050","0006341060") AND {fcb_per_commodity_grade_security_function_name}.UOM="GAL" THEN SUM(Volume_Qty_GAL)  
                WHEN Account_No IN ("0006341010","0006341020","0006341030","0006341040","0006341050","0006341060") AND {fcb_per_commodity_grade_security_function_name}.UOM="BBL" THEN SUM(Volume_Qty_BB6) 
                ELSE 0 END AS UOM1,

                CASE WHEN Account_No IN ("0006341810","0006341820","0006341830","0006341840","0006341850","0006341860") AND {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" THEN SUM(Amt_Comp_Code_Curr)  
                WHEN Account_No IN ("0006341810","0006341820","0006341830","0006341840","0006341850","0006341860") AND {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" THEN SUM(Amt_PC_Local_Curr)             
                ELSE 0 END AS CC2,

                CASE WHEN Account_No IN ("0006341810","0006341820","0006341830","0006341840","0006341850","0006341860") AND {fcb_per_commodity_grade_security_function_name}.UOM="KG" THEN SUM(Weight_Qty)
                WHEN Account_No IN ("0006341810","0006341820","0006341830","0006341840","0006341850","0006341860") AND {fcb_per_commodity_grade_security_function_name}.UOM="L15" THEN SUM(Volume_Qty) 
                WHEN Account_No IN ("0006341810","0006341820","0006341830","0006341840","0006341850","0006341860") AND {fcb_per_commodity_grade_security_function_name}.UOM="GAL" THEN SUM(Volume_Qty_GAL)  
                WHEN Account_No IN ("0006341810","0006341820","0006341830","0006341840","0006341850","0006341860") AND {fcb_per_commodity_grade_security_function_name}.UOM="BBL" THEN SUM(Volume_Qty_BB6)
                ELSE 0 END AS UOM2

                FROM temp
                GROUP BY Commodity_Code, COMMODITY_GRADE, Posting_Period,Material_Number,Plant,Account_No
            )
            
            GROUP BY Commodity_Code, COMMODITY_GRADE, Posting_Period,Material_Number,Plant)
        )
    GROUP BY  Commodity_Code, COMMODITY_GRADE
    ) b

    ON ((a.Commodity_Code=b.Commodity_Code) AND (a.COMMODITY_GRADE=b.COMMODITY_GRADE))))""")
    
    assignFunctionPermission(uc_catalog_name, uc_curated_schema, fcb_per_commodity_grade_security_function_name, tbl_owner_grp,security_end_user_aad_group_name, security_object_aad_group_name, security_functional_dev_aad_group, security_functional_readers_aad_group)

    print("fcb_per_commodity_grade_security function dropped and recreated in the schema")
else:
    spark.sql(f"""
    CREATE OR REPLACE FUNCTION `{uc_catalog_name}`.`{uc_curated_schema}`.{fcb_per_commodity_grade_security_function_name}(`Company_Code-Text` STRING,`Profit_Center-Text` STRING, Fiscal_Year STRING, Posting_Period STRING, Division STRING, Account_No STRING, Document_Type STRING, `Plant-Text` STRING, CURRENCY STRING, UOM STRING, Display_Unit STRING) 

    RETURNS TABLE (Commodity_Code STRING, COMMODITY_GRADE STRING, ACQUISITION_COSTS decimal(25,2), SALES_PROCEEDS decimal(25,2),EXCHANGES decimal(25,2), TRANSFERS decimal(25,2),C1_MARGIN_CASH decimal(25,2), PRICE_EFFECT decimal(25,2), STOCK_EFFECT decimal(25,2), C1_MARGIN_FIFO decimal(25,2), COSA_Adj decimal(25,2), COSA_PRICE_EFFECT decimal(25,2), RESIDUAL_CCS_VOL_EFFECT decimal(25,2), C1_MARGIN_CCS decimal(25,2))

    RETURN 

    with temp as (
    select COALESCE(Commodity_Code,'') AS Commodity_Code, COALESCE(Commodity_Code_Text,'') AS COMMODITY_GRADE, Account_No,Functional_Area, Posting_Period, Material_Number,Plant, Amt_Comp_Code_Curr,Amt_PC_Local_Curr, Volume_Qty_GAL, Weight_Qty, Volume_Qty, Volume_Qty_BB6
    from `{uc_catalog_name}`.`{uc_curated_schema}`.{fcb_dynamic_view_name} fcb
    WHERE
    (array_contains(SPLIT({fcb_per_commodity_grade_security_function_name}.`Company_Code-Text`,',') ,fcb.`Company_Code-Text`) OR ({fcb_per_commodity_grade_security_function_name}.`Company_Code-Text`='ALL')) AND
    (array_contains(SPLIT({fcb_per_commodity_grade_security_function_name}.`Profit_Center-Text`,',') ,fcb.`Profit_Center-Text`) OR ({fcb_per_commodity_grade_security_function_name}.`Profit_Center-Text`='ALL')) AND
    (array_contains(SPLIT({fcb_per_commodity_grade_security_function_name}.Fiscal_Year,',') ,fcb.Fiscal_Year) OR ({fcb_per_commodity_grade_security_function_name}.Fiscal_Year='ALL')) AND
    (array_contains(SPLIT({fcb_per_commodity_grade_security_function_name}.Posting_Period,',') ,fcb.Posting_Period) OR ({fcb_per_commodity_grade_security_function_name}.Posting_Period='ALL')) AND
    (array_contains(SPLIT({fcb_per_commodity_grade_security_function_name}.Division,',') ,fcb.Division) OR ({fcb_per_commodity_grade_security_function_name}.Division='ALL')) AND
    (array_contains(SPLIT({fcb_per_commodity_grade_security_function_name}.Account_No,',') ,fcb.Account_No) OR ({fcb_per_commodity_grade_security_function_name}.Account_No='ALL'))AND
    (array_contains(SPLIT({fcb_per_commodity_grade_security_function_name}.Document_Type,',') ,fcb.Document_Type) OR ({fcb_per_commodity_grade_security_function_name}.Document_Type='ALL')) AND
    (array_contains(SPLIT({fcb_per_commodity_grade_security_function_name}.`Plant-Text`,',') ,fcb.`Plant-Text`) OR ({fcb_per_commodity_grade_security_function_name}.`Plant-Text`='ALL'))
    AND fcb.OPFLAG != 'D')

    SELECT Commodity_Code, COMMODITY_GRADE,
        CASE WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Actual Value' THEN ACQUISITION_COSTS
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Thousands' THEN ACQUISITION_COSTS*0.001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Millions' THEN ACQUISITION_COSTS*0.000001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Billions' THEN ACQUISITION_COSTS*0.000000001
        END AS ACQUISITION_COSTS,
        CASE WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Actual Value' THEN SALES_PROCEEDS
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Thousands' THEN SALES_PROCEEDS*0.001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Millions' THEN SALES_PROCEEDS*0.000001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Billions' THEN SALES_PROCEEDS*0.000000001
        END AS SALES_PROCEEDS,
        CASE WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Actual Value' THEN EXCHANGES
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Thousands' THEN EXCHANGES*0.001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Millions' THEN EXCHANGES*0.000001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Billions' THEN EXCHANGES*0.000000001
        END AS EXCHANGES,
        CASE WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Actual Value' THEN TRANSFERS
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Thousands' THEN TRANSFERS*0.001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Millions' THEN TRANSFERS*0.000001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Billions' THEN TRANSFERS*0.000000001
        END AS TRANSFERS,
        CASE WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Actual Value' THEN C1_MARGIN_CASH
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Thousands' THEN C1_MARGIN_CASH*0.001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Millions' THEN C1_MARGIN_CASH*0.000001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Billions' THEN C1_MARGIN_CASH*0.000000001
        END AS C1_MARGIN_CASH,
        CASE WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Actual Value' THEN PRICE_EFFECT
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Thousands' THEN PRICE_EFFECT*0.001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Millions' THEN PRICE_EFFECT*0.000001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Billions' THEN PRICE_EFFECT*0.000000001
        END AS PRICE_EFFECT,
        CASE WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Actual Value' THEN STOCK_EFFECT
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Thousands' THEN STOCK_EFFECT*0.001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Millions' THEN STOCK_EFFECT*0.000001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Billions' THEN STOCK_EFFECT*0.000000001
        END AS STOCK_EFFECT,
        CASE WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Actual Value' THEN C1_MARGIN_FIFO
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Thousands' THEN C1_MARGIN_FIFO*0.001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Millions' THEN C1_MARGIN_FIFO*0.000001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Billions' THEN C1_MARGIN_FIFO*0.000000001
        END AS C1_MARGIN_FIFO,
        CASE WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Actual Value' THEN COSA_Adj
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Thousands' THEN COSA_Adj*0.001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Millions' THEN COSA_Adj*0.000001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Billions' THEN COSA_Adj*0.000000001
        END AS COSA_Adj,
        CASE WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Actual Value' THEN COSA_PRICE_EFFECT
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Thousands' THEN COSA_PRICE_EFFECT*0.001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Millions' THEN COSA_PRICE_EFFECT*0.000001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Billions' THEN COSA_PRICE_EFFECT*0.000000001
        END AS COSA_PRICE_EFFECT,
        CASE WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Actual Value' THEN RESIDUAL_CCS_VOL_EFFECT
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Thousands' THEN RESIDUAL_CCS_VOL_EFFECT*0.001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Millions' THEN RESIDUAL_CCS_VOL_EFFECT*0.000001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Billions' THEN RESIDUAL_CCS_VOL_EFFECT*0.000000001
        END AS RESIDUAL_CCS_VOL_EFFECT,
        CASE WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Actual Value' THEN C1_MARGIN_CCS
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Thousands' THEN C1_MARGIN_CCS*0.001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Millions' THEN C1_MARGIN_CCS*0.000001
            WHEN {fcb_per_commodity_grade_security_function_name}.Display_Unit = 'Billions' THEN C1_MARGIN_CCS*0.000000001
        END AS C1_MARGIN_CCS

    FROM(
    SELECT a.Commodity_Code, a.COMMODITY_GRADE, ACQUISITION_COSTS, SALES_PROCEEDS, EXCHANGES, TRANSFERS, C1_MARGIN_CASH, PRICE_EFFECT, STOCK_EFFECT, C1_MARGIN_CASH+PRICE_EFFECT+STOCK_EFFECT AS C1_MARGIN_FIFO, COSA_Adj, (-1*PRICE_EFFECT) AS COSA_PRICE_EFFECT,  COSA_Adj+PRICE_EFFECT AS RESIDUAL_CCS_VOL_EFFECT, C1_MARGIN_FIFO+COSA_Adj AS C1_MARGIN_CCS
    FROM
    ((SELECT  Commodity_Code,COMMODITY_GRADE, COALESCE(SUM(THIRD_PARTY_PURCHASES1),0)+ COALESCE(SUM(THIRD_PARTY_PURCHASES2),0)+ COALESCE(SUM(IG_PURCHASE_AMOUNT1),0)+ COALESCE(SUM(IG_PURCHASE_AMOUNT2),0) + COALESCE(SUM(HEDGE_PAPER),0)+ COALESCE(SUM(DEMURRAGE),0)+  COALESCE(SUM(EXCISE_DUTY),0) +  COALESCE(SUM(CSO),0) +  COALESCE(SUM(STOCK_ADJUSTMENT),0)+ COALESCE(SUM(OTHER_COSTS1),0)+ COALESCE(SUM(OTHER_COSTS2),0) AS ACQUISITION_COSTS,
    COALESCE(SUM(INTER_LOB_SALES1),0)+COALESCE(SUM(INTER_LOB_SALES2),0)+COALESCE(SUM(IG_SALES),0) +COALESCE(SUM(THIRD_PARTY_SALES),0) +COALESCE(SUM(PROCEEDS_FROM_SERVICE),0) AS SALES_PROCEEDS,
    COALESCE(SUM(EXG_SALES),0)+COALESCE(SUM(EXG_PURCHASES1),0)+COALESCE(SUM(EXG_PURCHASES2),0) AS EXCHANGES,
    COALESCE(SUM(IS_PURCHASES1),0)+ COALESCE(SUM(IS_PURCHASES2),0)+ COALESCE(SUM(IS_PURCHASES3),0)+ COALESCE(SUM(IS_PURCHASES4),0) + COALESCE(SUM(IS_PURCHASES5),0)+ COALESCE(SUM(IS_PURCHASES6),0)+  COALESCE(SUM(IS_SALES1),0) +  COALESCE(SUM(IS_SALES2),0) +  COALESCE(SUM(IS_SALES3),0)+ COALESCE(SUM(IS_SALES4),0)+ COALESCE(SUM(IS_SALES5),0)+ COALESCE(SUM(INTERNAL_SC_FROM),0)+ COALESCE(SUM(INTERNAL_SC_TO),0) AS TRANSFERS, 
    ACQUISITION_COSTS+SALES_PROCEEDS+EXCHANGES+TRANSFERS AS C1_MARGIN_CASH, COALESCE(SUM(COSA_Adj),0) AS COSA_Adj

    FROM 
    (SELECT Commodity_Code,COMMODITY_GRADE, Account_No, Functional_Area,
                
        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV05","SV06") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV05","SV06") THEN SUM(Amt_PC_Local_Curr)*(-1)
                ELSE "" END AS THIRD_PARTY_PURCHASES1,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006330140","0006340020","0006342110","0006342130","0006342150","0006342200","0006342510","0006342520","0006342530","0006351070","0006351800","0006354200","0006900470","0007250210","0006341140","0006341141","0006341142","0006341143","0006365000") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006330140","0006340020","0006342110","0006342130","0006342150","0006342200","0006342510","0006342520","0006342530","0006351070","0006351800","0006354200","0006900470","0007250210","0006341140","0006341141","0006341142","0006341143","0006365000") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS THIRD_PARTY_PURCHASES2,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV01","SV02","SV60","SV61") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006340010","0006342000") AND Functional_Area IN ("SV01","SV02","SV60","SV61")THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IG_PURCHASE_AMOUNT1,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006341200","0006342100","0006342050","0006342120") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006341200","0006342100","0006342050","0006342120") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IG_PURCHASE_AMOUNT2,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006920600","0006920650","0006920675","0006920680") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006920600","0006920650","0006920675","0006920680") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS HEDGE_PAPER,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006301300","0006301310","0006301320") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006301300","0006301310","0006301320") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS DEMURRAGE,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006210010","0006210015","0006210020","0006210060","0006211200","0006211201","0006211202","0006211210","0006211300","0006210310","0006211400","0006211500","0006342410") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006210010","0006210015","0006210020","0006210060","0006211200","0006211201","0006211202","0006211210","0006211300","0006210310","0006211400","0006211500","0006342410") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS EXCISE_DUTY ,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006355100","0006355600","0007240550") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006355100","0006355600","0007240550") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS CSO,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006351400","0006351405","0006351410","0006351415","0006351600","0006351605","0006351610","0006351615") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006351400","0006351405","0006351410","0006351415","0006351600","0006351605","0006351610","0006351615") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS STOCK_ADJUSTMENT,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006211530","0006340410","0006340510","0006340520","0006341150","0006342250","0006342400","0006342405","0006342420","0006344000","0006350210","0006352030","0006354100","0006355630","0006390200","0006390210","0006390220","0006390230","0006390240","0006390250","0006390260","0006390300", "0006820200","0006820250","0007240700","0007492250","0008381500","0008381505","0008381510","0008381515","0008383500","0008383510","0008750200","0009800206","0009800207","0009800208")  THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006211530","0006340410","0006340510","0006340520","0006341150","0006342250","0006342400","0006342405","0006342420","0006344000","0006350210","0006352030","0006354100","0006355630","0006390200","0006390210","0006390220","0006390230","0006390240","0006390250","0006390260","0006390300", "0006820200","0006820250","0007240700","0007492250","0008381500","0008381505","0008381510","0008381515","0008383500","0008383510","0008750200","0009800206","0009800207","0009800208") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS OTHER_COSTS1,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Functional_Area IN ("OP57") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Functional_Area IN ("OP57") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS OTHER_COSTS2,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006380302","0006380303","0006355565") AND Functional_Area IN ("SV46") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006380302","0006380303","0006355565")AND Functional_Area IN ("SV46") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS INTER_LOB_SALES1,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006380305") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006380305") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS INTER_LOB_SALES2,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006001500","0006001700","0006001900","0006002100","0006002200","0006009002","0006110035","0006110135","0006110235","0006520350","0006520360") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006001500","0006001700","0006001900","0006002100","0006002200","0006009002","0006110035","0006110135","0006110235","0006520350","0006520360") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IG_SALES,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006000100","0006000150","0006000151","0006000152","0006000153", "0006000200","0006000250","0006000300","0006000400","0006000500","0006000600","0006000650","0006000700","0006000800","0006002105","0006002205","0006002230","0006009000","0006009005","0006009010","0006009015","0006009050","0006009100","0006009800","0006009920","0006110010","0006110015","0006110020","0006110021","0006110030","0006110031","0006110110","0006110115","0006110120","0006110121","0006110130","0006110131","0006110210","0006110215","0006110220","0006110221","0006110230","0006110231","0006110300","0006110305","0006110310","0006110320","0006110330","0006110340","0006140100","0006140105","0006140200","0006140300","0006140500","0006140700","0006140800","0006340610","0006340620","0006340710","0006342300","0006342500","0006342550","0006920460","0006920461") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006000100","0006000150","0006000151","0006000152","0006000153", "0006000200","0006000250","0006000300","0006000400","0006000500","0006000600","0006000650","0006000700","0006000800","0006002105","0006002205","0006002230","0006009000","0006009005","0006009010","0006009015","0006009050","0006009100","0006009800","0006009920","0006110010","0006110015","0006110020","0006110021","0006110030","0006110031","0006110110","0006110115","0006110120","0006110121","0006110130","0006110131","0006110210","0006110215","0006110220","0006110221","0006110230","0006110231","0006110300","0006110305","0006110310","0006110320","0006110330","0006110340","0006140100","0006140105","0006140200","0006140300","0006140500","0006140700","0006140800","0006340610","0006340620","0006340710","0006342300","0006342500","0006342550","0006920460","0006920461") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS THIRD_PARTY_SALES,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006140025","0006520050","0006520100","0006520300","0006520310","0006520400","0006590100","0006610100","0006610200","0006610300","0006610400","0006900200","0006900205","0006900250","0006900400","0007220560") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006140025","0006520050","0006520100","0006520300","0006520310","0006520400","0006590100","0006610100","0006610200","0006610300","0006610400","0006900200","0006900205","0006900250","0006900400","0007220560") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS PROCEEDS_FROM_SERVICE,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006002220","0006350510","0006190200") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006002220","0006350510","0006190200") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS EXG_SALES, 

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006342000","0006340010") AND Functional_Area IN ("SV10","SV11","SV12","SV50","SV51") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006342000","0006340010")AND Functional_Area IN ("SV10","SV11","SV12","SV50","SV51") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS EXG_PURCHASES1,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0008399100","0006350520") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0008399100","0006350520") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS EXG_PURCHASES2,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006380200","0006380201","0006380306","0006390100","0006390400") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006380200","0006380201","0006380306","0006390100","0006390400") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IS_PURCHASES1,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No = "0006355595" AND Functional_Area IN (" ","SV44","SV45","SV48","SV49") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No = "0006355595" AND Functional_Area IN (" ","SV44","SV45","SV48","SV49") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IS_PURCHASES2,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No = "0006380202" AND Functional_Area IN ("SV44","SV45","SV48","SV56","SV58"," ") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No = "0006380202" AND Functional_Area IN ("SV44","SV45","SV48","SV56","SV58"," ") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IS_PURCHASES3,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No = "0006380203" AND Functional_Area IN ("SV03","SV48","SV49","SV53","SV55"," ") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No = "0006380203" AND Functional_Area IN ("SV03","SV48","SV49","SV53","SV55"," ") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IS_PURCHASES4,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No = "0006380302" AND Functional_Area IN ("SV33","SV57") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No = "0006380302" AND Functional_Area IN ("SV33","SV57") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IS_PURCHASES5,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No = "0006380303" AND Functional_Area IN ("SV54") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No = "0006380303" AND Functional_Area IN ("SV54") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IS_PURCHASES6,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN("0006380100","0006380300","0006380301")  THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN("0006380100","0006380300","0006380301") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IS_SALES1,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No = "0006355095" AND Functional_Area IN ("SV48") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No = "0006355095" AND Functional_Area IN ("SV48") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IS_SALES2,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN("0006355565","0006380302") AND Functional_Area IN ("SV34","SV47","") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN("0006355565","0006380302")AND Functional_Area IN ("SV34","SV47","") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IS_SALES3,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN("0006380202","0006380203") AND Functional_Area IN ("SV49") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN("0006380202","0006380203") AND Functional_Area IN ("SV49") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IS_SALES4,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No = "0006380303" AND Functional_Area IN ("SV03","SV32","SV47","") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No = "0006380303" AND Functional_Area IN ("SV03","SV32","SV47","") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS IS_SALES5,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No = "0006380203" AND Functional_Area = "SV30" THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No = "0006380203" AND Functional_Area = "SV30" THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS INTERNAL_SC_FROM,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No = "0006380303" AND Functional_Area = "SV31" THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No = "0006380303" AND Functional_Area = "SV31" THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS INTERNAL_SC_TO,

        CASE WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" AND Account_No IN ("0006336120","0006336110","0006380305","0006380307") THEN SUM(Amt_Comp_Code_Curr)*(-1)
                    WHEN {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" AND Account_No IN ("0006336120","0006336110","0006380305","0006380307") THEN SUM(Amt_PC_Local_Curr)*(-1)
                    ELSE "" END AS COSA_Adj                

        FROM temp 
        GROUP BY Commodity_Code,COMMODITY_GRADE, Account_No, Functional_Area
    )
    GROUP BY Commodity_Code,COMMODITY_GRADE  
    ) a LEFT JOIN

    (SELECT Commodity_Code, COMMODITY_GRADE, SUM(PRICE_EFFECT_INTERMEDIATE) AS PRICE_EFFECT,SUM(STOCK_EFFECT_INTERMEDIATE)+SUM(STOCK_EFFECT_EXTRA) AS STOCK_EFFECT
    FROM 
        (SELECT Commodity_Code, COMMODITY_GRADE, Posting_Period, Material_Number, Plant,  
        CASE WHEN (ISNULL(CC1) OR ISNULL(UOM1) OR (UOM1=0)) AND  (ISNULL(CC2) OR ISNULL(UOM2) OR (UOM2=0)) THEN (-1*CC1)-CC2 
            WHEN ((ISNULL(UOM1) OR (UOM1=0)) AND (ISNOTNULL(CC1) AND CC1<>0)) THEN (-1*CC1)
            WHEN ((ISNULL(CC1) OR CC1=0)AND (ISNOTNULL(CC2) AND CC2<>0)) THEN 0
            WHEN (ISNULL(UOM2) OR UOM2=0 ) THEN (-1*CC2)
            ELSE (((CC2/UOM2)-(CC1/UOM1))*(-1*UOM2)) END 
        AS PRICE_EFFECT_INTERMEDIATE,

        CASE WHEN ((-1*UOM2)-UOM1)=0 OR ISNULL((-1*UOM2)-UOM1 ) THEN 0
            WHEN (ISNULL(CC1/UOM1) OR (CC1/UOM1=0)) AND (((CC1 <> 0) AND ISNOTNULL(CC1)) AND ((CC2 <> 0) AND ISNOTNULL(CC2)))
            THEN (-1*CC2) 
            WHEN (ISNULL(CC1/UOM1) OR (CC1/UOM1=0))  AND (CC1 <> 0 AND ISNOTNULL(CC1)) THEN 0
            WHEN (ISNULL(CC1/UOM1) OR (CC1/UOM1=0))  AND (CC1 = 0 OR ISNULL(CC1)) THEN (-1*CC2)
            WHEN (ISNULL(CC2/UOM2) OR (CC2/UOM2=0)) THEN -(CC1)
            WHEN (CC1 = 0 OR ISNULL(CC1)) AND (CC2 <> 0 AND ISNULL(CC2)) THEN (CC1 -CC2)
            WHEN ISNULL(((-1*UOM2)-UOM1)*(-CC1/-UOM1)) THEN 0
            ELSE (((-1*UOM2)-UOM1)*(CC1/UOM1))
            END AS STOCK_EFFECT_INTERMEDIATE,

        CASE WHEN Plant="D314" AND  Material_Number="000000000400001991" THEN ((-1*CC2)-CC1) 
            ELSE 0 END
        AS STOCK_EFFECT_EXTRA

        FROM
        (
            SELECT Commodity_Code, COMMODITY_GRADE, Posting_Period,Material_Number,Plant, SUM(CC1) AS CC1, SUM(CC2) AS CC2, SUM(UOM1) AS UOM1, SUM(UOM2) AS UOM2 
            FROM 
            (
                SELECT Commodity_Code, COMMODITY_GRADE, Posting_Period,Material_Number,Plant,Account_No, 
                CASE WHEN Account_No IN ("0006341010","0006341020","0006341030","0006341040","0006341050","0006341060") AND {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" THEN SUM(Amt_Comp_Code_Curr)  
                WHEN Account_No IN ("0006341010","0006341020","0006341030","0006341040","0006341050","0006341060") AND {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" THEN SUM(Amt_PC_Local_Curr)             
                ELSE 0 END AS CC1,

                CASE WHEN Account_No IN ("0006341010","0006341020","0006341030","0006341040","0006341050","0006341060") AND {fcb_per_commodity_grade_security_function_name}.UOM="KG" THEN SUM(Weight_Qty) 
                WHEN Account_No IN ("0006341010","0006341020","0006341030","0006341040","0006341050","0006341060") AND {fcb_per_commodity_grade_security_function_name}.UOM="L15" THEN SUM(Volume_Qty)  
                WHEN Account_No IN ("0006341010","0006341020","0006341030","0006341040","0006341050","0006341060") AND {fcb_per_commodity_grade_security_function_name}.UOM="GAL" THEN SUM(Volume_Qty_GAL)  
                WHEN Account_No IN ("0006341010","0006341020","0006341030","0006341040","0006341050","0006341060") AND {fcb_per_commodity_grade_security_function_name}.UOM="BBL" THEN SUM(Volume_Qty_BB6) 
                ELSE 0 END AS UOM1,

                CASE WHEN Account_No IN ("0006341810","0006341820","0006341830","0006341840","0006341850","0006341860") AND {fcb_per_commodity_grade_security_function_name}.CURRENCY="Local Currency" THEN SUM(Amt_Comp_Code_Curr)  
                WHEN Account_No IN ("0006341810","0006341820","0006341830","0006341840","0006341850","0006341860") AND {fcb_per_commodity_grade_security_function_name}.CURRENCY="Group Currency" THEN SUM(Amt_PC_Local_Curr)             
                ELSE 0 END AS CC2,

                CASE WHEN Account_No IN ("0006341810","0006341820","0006341830","0006341840","0006341850","0006341860") AND {fcb_per_commodity_grade_security_function_name}.UOM="KG" THEN SUM(Weight_Qty)
                WHEN Account_No IN ("0006341810","0006341820","0006341830","0006341840","0006341850","0006341860") AND {fcb_per_commodity_grade_security_function_name}.UOM="L15" THEN SUM(Volume_Qty) 
                WHEN Account_No IN ("0006341810","0006341820","0006341830","0006341840","0006341850","0006341860") AND {fcb_per_commodity_grade_security_function_name}.UOM="GAL" THEN SUM(Volume_Qty_GAL)  
                WHEN Account_No IN ("0006341810","0006341820","0006341830","0006341840","0006341850","0006341860") AND {fcb_per_commodity_grade_security_function_name}.UOM="BBL" THEN SUM(Volume_Qty_BB6)
                ELSE 0 END AS UOM2

                FROM temp
                GROUP BY Commodity_Code, COMMODITY_GRADE, Posting_Period,Material_Number,Plant,Account_No
            )
            
            GROUP BY Commodity_Code, COMMODITY_GRADE, Posting_Period,Material_Number,Plant)
        )
    GROUP BY  Commodity_Code, COMMODITY_GRADE
    ) b

    ON ((a.Commodity_Code=b.Commodity_Code) AND (a.COMMODITY_GRADE=b.COMMODITY_GRADE))))""")

    assignFunctionPermission(uc_catalog_name, uc_curated_schema, fcb_per_commodity_grade_security_function_name, tbl_owner_grp,security_end_user_aad_group_name, security_object_aad_group_name, security_functional_dev_aad_group, security_functional_readers_aad_group)


# COMMAND ----------

tfp_exception_security_func_name = uc_catalog_name+"."+uc_curated_schema+"."+tfp_exception_security_function_name
if tfp_exception_security_func_name in function_list:
    spark.sql(f"""DROP FUNCTION `{uc_catalog_name}`.`{uc_curated_schema}`.{tfp_exception_security_function_name}""")
    spark.sql(f"""
    CREATE OR REPLACE FUNCTION `{uc_catalog_name}`.`{uc_curated_schema}`.{tfp_exception_security_function_name}(`Company_Code-Text` STRING,`Profit_Center-Text` STRING, Fiscal_Year STRING, Posting_Period STRING, Division STRING, Account_No STRING, Document_Type STRING, `Plant-Text` STRING,`Mvmt_Type-Text` STRING, `Material-Text` STRING,Product_Sub_Class_Description STRING, CURRENCY STRING, UOM STRING) 
    RETURNS TABLE (`MATERIAL-TEXT` STRING,Plant STRING,`GL-TEXT` STRING, EXCEPTION decimal(25,2),CL_GL_Account_Key STRING) 
    RETURN  

        with temp as (
        select Posting_Period,Material_Number,Plant, Account_No, Amt_Comp_Code_Curr,Amt_PC_Local_Curr, Volume_Qty_GAL, Weight_Qty, Volume_Qty, Volume_Qty_BB6,`Material-Text` as `MATERIAL-TEXT` ,`GL-Text` AS `GL-TEXT`,CL_GL_Account_Key
        from `{uc_catalog_name}`.`{uc_curated_schema}`.{fcb_dynamic_view_name} fcb
        WHERE
        (array_contains(SPLIT({tfp_exception_security_function_name}.`Company_Code-Text`,',') ,fcb.`Company_Code-Text`) OR ({tfp_exception_security_function_name}.`Company_Code-Text`='ALL')) AND
        (array_contains(SPLIT({tfp_exception_security_function_name}.`Profit_Center-Text`,',') ,fcb.`Profit_Center-Text`) OR ({tfp_exception_security_function_name}.`Profit_Center-Text`='ALL')) AND
        (array_contains(SPLIT({tfp_exception_security_function_name}.Fiscal_Year,',') ,fcb.Fiscal_Year) OR ({tfp_exception_security_function_name}.Fiscal_Year='ALL')) AND
        (array_contains(SPLIT({tfp_exception_security_function_name}.Posting_Period,',') ,fcb.Posting_Period) OR ({tfp_exception_security_function_name}.Posting_Period='ALL')) AND
        (array_contains(SPLIT({tfp_exception_security_function_name}.Division,',') ,fcb.Division) OR ({tfp_exception_security_function_name}.Division='ALL')) AND
        (array_contains(SPLIT({tfp_exception_security_function_name}.Account_No,',') ,fcb.Account_No) OR ({tfp_exception_security_function_name}.Account_No='ALL'))AND
        (array_contains(SPLIT({tfp_exception_security_function_name}.Document_Type,',') ,fcb.Document_Type) OR ({tfp_exception_security_function_name}.Document_Type='ALL')) AND
        (array_contains(SPLIT({tfp_exception_security_function_name}.`Plant-Text`,',') ,fcb.`Plant-Text`) OR ({tfp_exception_security_function_name}.`Plant-Text`='ALL')) AND
        (array_contains(SPLIT({tfp_exception_security_function_name}.`Mvmt_Type-Text`,',') ,fcb.`Mvmt_Type-Text`) OR ({tfp_exception_security_function_name}.`Mvmt_Type-Text`='ALL')) AND
        (array_contains(SPLIT({tfp_exception_security_function_name}.`Material-Text`,',') ,fcb.`Material-Text`) OR ({tfp_exception_security_function_name}.`Material-Text`='ALL')) AND
        (array_contains(SPLIT({tfp_exception_security_function_name}.Product_Sub_Class_Description,',') ,fcb.Product_Sub_Class_Description) OR ({tfp_exception_security_function_name}.Product_Sub_Class_Description='ALL')) AND 
        fcb.`GL-Text` IN ("6380202-Transfer Price Paid: Product (S-M or M-M)","6380203-Product Transfers Paid","6380302-Transfer Price Received: Product (S-M or M-M)","6380303-Product Transfers Received")
        AND fcb.OPFLAG != 'D')




    SELECT `Material-Text`,Plant,`GL-Text`,
            CASE WHEN SUM(Volume_Qty)=0 THEN 0  
            WHEN isnull(SUM(Amt_PC_Local_Curr))  OR isnull(SUM(Volume_Qty)) THEN NULL 
            ELSE (SUM(Amt_PC_Local_Curr)/SUM(Volume_Qty)) END
            AS Exception ,
            CL_GL_Account_Key

            FROM temp
    GROUP BY `Material-Text`,Plant,`GL-Text`,CL_GL_Account_Key""")

    assignFunctionPermission(uc_catalog_name, uc_curated_schema, tfp_exception_security_function_name, tbl_owner_grp,security_end_user_aad_group_name, security_object_aad_group_name, security_functional_dev_aad_group, security_functional_readers_aad_group)

    print("tfp_exception_security function dropped and recreated in the schema")
else:
    spark.sql(f"""
    CREATE OR REPLACE FUNCTION `{uc_catalog_name}`.`{uc_curated_schema}`.{tfp_exception_security_function_name}(`Company_Code-Text` STRING,`Profit_Center-Text`STRING, Fiscal_Year STRING, Posting_Period STRING, Division STRING, Account_No STRING, Document_Type STRING, `Plant-Text` STRING,`Mvmt_Type-Text` STRING, `Material-Text` STRING,Product_Sub_Class_Description STRING, CURRENCY STRING, UOM STRING) 
    RETURNS TABLE (`MATERIAL-TEXT` STRING,Plant STRING,`GL-TEXT` STRING, EXCEPTION decimal(25,2),CL_GL_Account_Key STRING) 
    RETURN  

        with temp as (
        select Posting_Period,Material_Number,Plant, Account_No, Amt_Comp_Code_Curr,Amt_PC_Local_Curr, Volume_Qty_GAL, Weight_Qty, Volume_Qty, Volume_Qty_BB6,`Material-Text` AS `MATERIAL-TEXT`,`GL-Text` AS `GL-TEXT`,CL_GL_Account_Key
        from `{uc_catalog_name}`.`{uc_curated_schema}`.{fcb_dynamic_view_name} fcb
        WHERE
        (array_contains(SPLIT({tfp_exception_security_function_name}.`Company_Code-Text`,',') ,fcb.`Company_Code-Text`) OR ({tfp_exception_security_function_name}.`Company_Code-Text`='ALL')) AND
        (array_contains(SPLIT({tfp_exception_security_function_name}.`Profit_Center-Text`,',') ,fcb.`Profit_Center-Text`) OR ({tfp_exception_security_function_name}.`Profit_Center-Text`='ALL')) AND
        (array_contains(SPLIT({tfp_exception_security_function_name}.Fiscal_Year,',') ,fcb.Fiscal_Year) OR ({tfp_exception_security_function_name}.Fiscal_Year='ALL')) AND
        (array_contains(SPLIT({tfp_exception_security_function_name}.Posting_Period,',') ,fcb.Posting_Period) OR ({tfp_exception_security_function_name}.Posting_Period='ALL')) AND
        (array_contains(SPLIT({tfp_exception_security_function_name}.Division,',') ,fcb.Division) OR ({tfp_exception_security_function_name}.Division='ALL')) AND
        (array_contains(SPLIT({tfp_exception_security_function_name}.Account_No,',') ,fcb.Account_No) OR ({tfp_exception_security_function_name}.Account_No='ALL'))AND
        (array_contains(SPLIT({tfp_exception_security_function_name}.Document_Type,',') ,fcb.Document_Type) OR ({tfp_exception_security_function_name}.Document_Type='ALL')) AND
        (array_contains(SPLIT({tfp_exception_security_function_name}.`Plant-Text`,',') ,fcb.`Plant-Text`) OR ({tfp_exception_security_function_name}.`Plant-Text`='ALL')) AND
        (array_contains(SPLIT({tfp_exception_security_function_name}.`Mvmt_Type-Text`,',') ,fcb.`Mvmt_Type-Text`) OR ({tfp_exception_security_function_name}.`Mvmt_Type-Text`='ALL')) AND
        (array_contains(SPLIT({tfp_exception_security_function_name}.`Material-Text`,',') ,fcb.`Material-Text`) OR ({tfp_exception_security_function_name}.`Material-Text`='ALL')) AND
        (array_contains(SPLIT({tfp_exception_security_function_name}.Product_Sub_Class_Description,',') ,fcb.Product_Sub_Class_Description) OR ({tfp_exception_security_function_name}.Product_Sub_Class_Description='ALL')) AND 
        fcb.`GL-Text` IN ("6380202-Transfer Price Paid: Product (S-M or M-M)","6380203-Product Transfers Paid","6380302-Transfer Price Received: Product (S-M or M-M)","6380303-Product Transfers Received")
        AND fcb.OPFLAG != 'D')




    SELECT `Material-Text`,Plant,`GL-Text`,
            CASE WHEN SUM(Volume_Qty)=0 THEN 0  
            WHEN isnull(SUM(Amt_PC_Local_Curr))  OR isnull(SUM(Volume_Qty)) THEN NULL
            ELSE (SUM(Amt_PC_Local_Curr)/SUM(Volume_Qty)) END
            AS Exception ,
            CL_GL_Account_Key

            FROM temp
    GROUP BY `Material-Text`,Plant,`GL-Text`,CL_GL_Account_Key""")

    assignFunctionPermission(uc_catalog_name, uc_curated_schema, tfp_exception_security_function_name, tbl_owner_grp,security_end_user_aad_group_name, security_object_aad_group_name, security_functional_dev_aad_group, security_functional_readers_aad_group)

# COMMAND ----------

detailed_report_security_func_name = uc_catalog_name+"."+uc_curated_schema+"."+detailed_report_security_function_name
if detailed_report_security_func_name in function_list:
    spark.sql(f"""DROP FUNCTION `{uc_catalog_name}`.`{uc_curated_schema}`.{detailed_report_security_function_name}""")
    spark.sql(f"""
    CREATE OR REPLACE FUNCTION `{uc_catalog_name}`.`{uc_curated_schema}`.{detailed_report_security_function_name}(`Company_Code-Text` STRING,`Profit_Center-Text` STRING, Fiscal_Year STRING, Posting_Period STRING, Division STRING, Account_No STRING, Document_Type STRING, `Plant-Text` STRING,`Mvmt_Type-Text` STRING, `Material-Text` STRING,Product_Sub_Class_Description STRING,Partner_Profit_Center STRING, CURRENCY STRING, UOM STRING,Display_Unit STRING) 
    RETURNS TABLE (`Company Code` STRING,`Profit Center` STRING, `Distribution Channel` STRING,`Distribution Channel-Text` STRING, `G/L Account Key` STRING,  `G/L Account Name` STRING, `Functional Area` STRING, `Plant` STRING, `Plant Name` STRING,`Plant Type` STRING, `Material Key` STRING,  `Material Name` STRING, `Commodity Code` STRING,  `Commodity Grade` STRING, `MM Movement Type` STRING, `Movement Type`  STRING, `PCA Doc Type` STRING, `R/3 Amount` decimal(25,2), `R/3 Quantity` decimal(25,2)) 

    RETURN  

        with temp as (
        select Company_Code , CL_Profit_Center , Distribution_Channel ,`Distribution_Channel-Text`, CL_GL_Account_Key , GL_Account_Long_Text , Functional_Area , Plant  , KNA1_Name_1 , T001W_Dist_Prof_Plant , Material_Key , Material_Description , Commodity_Code , Commodity_Code_Text ,MM_Mvmt_Type ,Mvmt_Type  ,Document_Type , Amt_Comp_Code_Curr, Amt_PC_Local_Curr, Weight_Qty, Volume_Qty, Volume_Qty_GAL, Volume_Qty_BB6
        from `{uc_catalog_name}`.`{uc_curated_schema}`.{fcb_dynamic_view_name} fcb
        WHERE
        (array_contains(SPLIT({detailed_report_security_function_name}.`Company_Code-Text`,',') ,fcb.`Company_Code-Text`) OR ({detailed_report_security_function_name}.`Company_Code-Text`='ALL')) AND
        (array_contains(SPLIT({detailed_report_security_function_name}.`Profit_Center-Text`,',') ,fcb.`Profit_Center-Text`) OR ({detailed_report_security_function_name}.`Profit_Center-Text`='ALL')) AND
        (array_contains(SPLIT({detailed_report_security_function_name}.Fiscal_Year,',') ,fcb.Fiscal_Year) OR ({detailed_report_security_function_name}.Fiscal_Year='ALL')) AND
        (array_contains(SPLIT({detailed_report_security_function_name}.Posting_Period,',') ,fcb.Posting_Period) OR ({detailed_report_security_function_name}.Posting_Period='ALL')) AND
        (array_contains(SPLIT({detailed_report_security_function_name}.Division,',') ,fcb.Division) OR ({detailed_report_security_function_name}.Division='ALL')) AND
        (array_contains(SPLIT({detailed_report_security_function_name}.Account_No,',') ,fcb.Account_No) OR ({detailed_report_security_function_name}.Account_No='ALL'))AND
        (array_contains(SPLIT({detailed_report_security_function_name}.Document_Type,',') ,fcb.Document_Type) OR ({detailed_report_security_function_name}.Document_Type='ALL')) AND
        (array_contains(SPLIT({detailed_report_security_function_name}.`Plant-Text`,',') ,fcb.`Plant-Text`) OR ({detailed_report_security_function_name}.`Plant-Text`='ALL')) AND
        (array_contains(SPLIT({detailed_report_security_function_name}.`Mvmt_Type-Text` ,',') ,fcb.`Mvmt_Type-Text` ) OR ({detailed_report_security_function_name}.`Mvmt_Type-Text` ='ALL')) AND
        (array_contains(SPLIT({detailed_report_security_function_name}.Partner_Profit_Center,',') ,fcb.Partner_Profit_Center) OR ({detailed_report_security_function_name}.Partner_Profit_Center='ALL')) AND
        (array_contains(SPLIT({detailed_report_security_function_name}.`Material-Text`,',') ,fcb.`Material-Text`) OR ({detailed_report_security_function_name}.`Material-Text`='ALL')) AND
        (array_contains(SPLIT({detailed_report_security_function_name}.Product_Sub_Class_Description,',') ,fcb.Product_Sub_Class_Description) OR ({detailed_report_security_function_name}.Product_Sub_Class_Description='ALL'))
        AND fcb.OPFLAG != 'D')

    SELECT `Company Code`, `Profit Center`, `Distribution Channel`,`Distribution Channel-Text`,`G/L Account Key`, `G/L Account Name`, `Functional Area`, `Plant`, `Plant Name`, `Plant Type`, `Material Key`, `Material Name`, `Commodity Code`, `Commodity Grade`, `MM Movement Type`, `Movement Type`, `PCA Doc Type`,
    CASE WHEN {detailed_report_security_function_name}.Display_Unit = 'Actual Value' THEN `R/3 Amount`
    WHEN {detailed_report_security_function_name}.Display_Unit = 'Thousands' THEN `R/3 Amount`*0.001
    WHEN {detailed_report_security_function_name}.Display_Unit = 'Millions' THEN `R/3 Amount`*0.000001
    WHEN {detailed_report_security_function_name}.Display_Unit = 'Billions' THEN `R/3 Amount`*0.000000001
    END
    AS `R/3 Amount`,

    CASE WHEN {detailed_report_security_function_name}.Display_Unit = 'Actual Value' THEN `R/3 Quantity`
    WHEN {detailed_report_security_function_name}.Display_Unit = 'Thousands' THEN `R/3 Quantity`*0.001
    WHEN {detailed_report_security_function_name}.Display_Unit = 'Millions' THEN `R/3 Quantity`*0.000001
    WHEN {detailed_report_security_function_name}.Display_Unit = 'Billions' THEN `R/3 Quantity`*0.000000001
    END
    AS `R/3 Quantity`

    FROM    


    (SELECT Company_Code AS `Company Code`, CL_Profit_Center AS `Profit Center`, Distribution_Channel AS `Distribution Channel`,`Distribution_Channel-Text` as `Distribution Channel-Text`, CL_GL_Account_Key AS `G/L Account Key`, GL_Account_Long_Text AS `G/L Account Name`, Functional_Area AS `Functional Area`, Plant AS `Plant`, 
    KNA1_Name_1 AS `Plant Name`, T001W_Dist_Prof_Plant AS `Plant Type`, Material_Key AS `Material Key`, Material_Description AS `Material Name`, Commodity_Code AS `Commodity Code`, Commodity_Code_Text AS `Commodity Grade`,MM_Mvmt_Type AS `MM Movement Type`,Mvmt_Type AS `Movement Type` ,Document_Type AS `PCA Doc Type`, 
    CASE WHEN {detailed_report_security_function_name}.CURRENCY="Local Currency" THEN SUM(Amt_Comp_Code_Curr)  
        WHEN {detailed_report_security_function_name}.CURRENCY="Group Currency" THEN SUM(Amt_PC_Local_Curr) END AS `R/3 Amount`,

    CASE  WHEN {detailed_report_security_function_name}.UOM="KG" THEN SUM(Weight_Qty)
        WHEN  {detailed_report_security_function_name}.UOM="L15" THEN SUM(Volume_Qty) 
        WHEN  {detailed_report_security_function_name}.UOM="GAL" THEN SUM(Volume_Qty_GAL)  
        WHEN  {detailed_report_security_function_name}.UOM="BBL" THEN SUM(Volume_Qty_BB6)
        ELSE 0 END AS `R/3 Quantity`
    FROM temp
    GROUP BY `Company Code`,`Profit Center`, `Distribution Channel`,`Distribution Channel-Text`, `G/L Account Key`,  `G/L Account Name`, `Functional Area`, `Plant`, `Plant Name`,`Plant Type`, `Material Key`,  `Material Name`, `Commodity Code`,  `Commodity Grade`, `MM Movement Type`, `Movement Type` , `PCA Doc Type`)""")

    assignFunctionPermission(uc_catalog_name, uc_curated_schema, detailed_report_security_function_name, tbl_owner_grp,security_end_user_aad_group_name, security_object_aad_group_name, security_functional_dev_aad_group, security_functional_readers_aad_group)

    print("detailed_report_security function dropped and recreated in the schema")
else:
    spark.sql(f"""
    CREATE OR REPLACE FUNCTION `{uc_catalog_name}`.`{uc_curated_schema}`.{detailed_report_security_function_name}(`Company_Code-Text` STRING,`Profit_Center-Text` STRING, Fiscal_Year STRING, Posting_Period STRING, Division STRING, Account_No STRING, Document_Type STRING, `Plant-Text` STRING,`Mvmt_Type-Text` STRING, `Material-Text` STRING,Product_Sub_Class_Description STRING,Partner_Profit_Center STRING, CURRENCY STRING, UOM STRING,Display_Unit STRING) 
    RETURNS TABLE (`Company Code` STRING,`Profit Center` STRING, `Distribution Channel` STRING,`Distribution Channel-Text` STRING, `G/L Account Key` STRING,  `G/L Account Name` STRING, `Functional Area` STRING, `Plant` STRING, `Plant Name` STRING,`Plant Type` STRING, `Material Key` STRING,  `Material Name` STRING, `Commodity Code` STRING,  `Commodity Grade` STRING, `MM Movement Type` STRING, `Movement Type`  STRING, `PCA Doc Type` STRING,`R/3 Amount` decimal(25,2), `R/3 Quantity` decimal(25,2)) 
    RETURN  

        with temp as (
        select Company_Code , CL_Profit_Center , Distribution_Channel ,`Distribution_Channel-Text`, CL_GL_Account_Key , GL_Account_Long_Text , Functional_Area , Plant  , KNA1_Name_1 , T001W_Dist_Prof_Plant , Material_Key , Material_Description , Commodity_Code , Commodity_Code_Text ,MM_Mvmt_Type ,Mvmt_Type  ,Document_Type , Amt_Comp_Code_Curr, Amt_PC_Local_Curr, Weight_Qty, Volume_Qty, Volume_Qty_GAL, Volume_Qty_BB6
        from `{uc_catalog_name}`.`{uc_curated_schema}`.{fcb_dynamic_view_name} fcb
        WHERE
        (array_contains(SPLIT({detailed_report_security_function_name}.`Company_Code-Text`,',') ,fcb.`Company_Code-Text`) OR ({detailed_report_security_function_name}.`Company_Code-Text`='ALL')) AND
        (array_contains(SPLIT({detailed_report_security_function_name}.`Profit_Center-Text`,',') ,fcb.`Profit_Center-Text`) OR ({detailed_report_security_function_name}.`Profit_Center-Text`='ALL')) AND
        (array_contains(SPLIT({detailed_report_security_function_name}.Fiscal_Year,',') ,fcb.Fiscal_Year) OR ({detailed_report_security_function_name}.Fiscal_Year='ALL')) AND
        (array_contains(SPLIT({detailed_report_security_function_name}.Posting_Period,',') ,fcb.Posting_Period) OR ({detailed_report_security_function_name}.Posting_Period='ALL')) AND
        (array_contains(SPLIT({detailed_report_security_function_name}.Division,',') ,fcb.Division) OR ({detailed_report_security_function_name}.Division='ALL')) AND
        (array_contains(SPLIT({detailed_report_security_function_name}.Account_No,',') ,fcb.Account_No) OR ({detailed_report_security_function_name}.Account_No='ALL'))AND
        (array_contains(SPLIT({detailed_report_security_function_name}.Document_Type,',') ,fcb.Document_Type) OR ({detailed_report_security_function_name}.Document_Type='ALL')) AND
        (array_contains(SPLIT({detailed_report_security_function_name}.`Plant-Text`,',') ,fcb.`Plant-Text`) OR ({detailed_report_security_function_name}.`Plant-Text`='ALL')) AND
        (array_contains(SPLIT({detailed_report_security_function_name}.`Mvmt_Type-Text` ,',') ,fcb.`Mvmt_Type-Text` ) OR ({detailed_report_security_function_name}.`Mvmt_Type-Text` ='ALL')) AND
        (array_contains(SPLIT({detailed_report_security_function_name}.Partner_Profit_Center,',') ,fcb.Partner_Profit_Center) OR ({detailed_report_security_function_name}.Partner_Profit_Center='ALL')) AND
        (array_contains(SPLIT({detailed_report_security_function_name}.`Material-Text`,',') ,fcb.`Material-Text`) OR ({detailed_report_security_function_name}.`Material-Text`='ALL')) AND
        (array_contains(SPLIT({detailed_report_security_function_name}.Product_Sub_Class_Description,',') ,fcb.Product_Sub_Class_Description) OR ({detailed_report_security_function_name}.Product_Sub_Class_Description='ALL'))
        AND fcb.OPFLAG != 'D')

    SELECT `Company Code`, `Profit Center`, `Distribution Channel`,`Distribution Channel-Text`, `G/L Account Key`, `G/L Account Name`, `Functional Area`, `Plant`, `Plant Name`, `Plant Type`, `Material Key`, `Material Name`, `Commodity Code`, `Commodity Grade`, `MM Movement Type`, `Movement Type`, `PCA Doc Type`,
    CASE WHEN {detailed_report_security_function_name}.Display_Unit = 'Actual Value' THEN `R/3 Amount`
    WHEN {detailed_report_security_function_name}.Display_Unit = 'Thousands' THEN `R/3 Amount`*0.001
    WHEN {detailed_report_security_function_name}.Display_Unit = 'Millions' THEN `R/3 Amount`*0.000001
    WHEN {detailed_report_security_function_name}.Display_Unit = 'Billions' THEN `R/3 Amount`*0.000000001
    END
    AS `R/3 Amount`,

    CASE WHEN {detailed_report_security_function_name}.Display_Unit = 'Actual Value' THEN `R/3 Quantity`
    WHEN {detailed_report_security_function_name}.Display_Unit = 'Thousands' THEN `R/3 Quantity`*0.001
    WHEN {detailed_report_security_function_name}.Display_Unit = 'Millions' THEN `R/3 Quantity`*0.000001
    WHEN {detailed_report_security_function_name}.Display_Unit = 'Billions' THEN `R/3 Quantity`*0.000000001
    END
    AS `R/3 Quantity`

    FROM    


    (SELECT Company_Code AS `Company Code`, CL_Profit_Center AS `Profit Center`, Distribution_Channel AS `Distribution Channel`,`Distribution_Channel-Text` as `Distribution Channel-Text` , CL_GL_Account_Key AS `G/L Account Key`, GL_Account_Long_Text AS `G/L Account Name`, Functional_Area AS `Functional Area`, Plant AS `Plant`, 
    KNA1_Name_1 AS `Plant Name`, T001W_Dist_Prof_Plant AS `Plant Type`, Material_Key AS `Material Key`, Material_Description AS `Material Name`, Commodity_Code AS `Commodity Code`, Commodity_Code_Text AS `Commodity Grade`,MM_Mvmt_Type AS `MM Movement Type`,Mvmt_Type AS `Movement Type` ,Document_Type AS `PCA Doc Type`, 
    CASE WHEN {detailed_report_security_function_name}.CURRENCY="Local Currency" THEN SUM(Amt_Comp_Code_Curr)  
        WHEN {detailed_report_security_function_name}.CURRENCY="Group Currency" THEN SUM(Amt_PC_Local_Curr) END AS `R/3 Amount`,

    CASE  WHEN {detailed_report_security_function_name}.UOM="KG" THEN SUM(Weight_Qty)
        WHEN  {detailed_report_security_function_name}.UOM="L15" THEN SUM(Volume_Qty) 
        WHEN  {detailed_report_security_function_name}.UOM="GAL" THEN SUM(Volume_Qty_GAL)  
        WHEN  {detailed_report_security_function_name}.UOM="BBL" THEN SUM(Volume_Qty_BB6)
        ELSE 0 END AS `R/3 Quantity`
    FROM temp
    GROUP BY `Company Code`,`Profit Center`, `Distribution Channel`,`Distribution Channel-Text`, `G/L Account Key`,  `G/L Account Name`, `Functional Area`, `Plant`, `Plant Name`,`Plant Type`, `Material Key`,  `Material Name`, `Commodity Code`,  `Commodity Grade`, `MM Movement Type`, `Movement Type` , `PCA Doc Type`)""")

    assignFunctionPermission(uc_catalog_name, uc_curated_schema, detailed_report_security_function_name, tbl_owner_grp,security_end_user_aad_group_name, security_object_aad_group_name, security_functional_dev_aad_group, security_functional_readers_aad_group)

# COMMAND ----------

gains_and_losses_security_func_name = uc_catalog_name+"."+uc_curated_schema+"."+gains_and_losses_security_function_name
if gains_and_losses_security_func_name in function_list:
    spark.sql(f"""DROP FUNCTION `{uc_catalog_name}`.`{uc_curated_schema}`.{gains_and_losses_security_function_name}""")
    spark.sql(f"""
    CREATE OR REPLACE FUNCTION `{uc_catalog_name}`.`{uc_curated_schema}`.{gains_and_losses_security_function_name}(`Company_Code-Text` STRING,`Profit_Center-Text` STRING, Fiscal_Year STRING, Posting_Period STRING, Division STRING, Account_No STRING, Document_Type STRING, `Plant-Text` STRING,`Mvmt_Type-Text` STRING,`Material-Text` STRING, CURRENCY STRING, UOM STRING, Display_Unit STRING) 

    RETURNS TABLE (`Profit Center` STRING,`Movement Type+` STRING,`Commodity Code` STRING, `Commodity Grade` STRING, `Plant_Type` STRING, `Movement Type Code`  STRING,`R/3 Amount` decimal(25,2), `R/3 Quantity` decimal(25,2))

    RETURN 

    with temp as (
    select CL_Profit_Center AS `Profit Center`,fcb.CL_Mvmt_Type_Plus AS `Movement Type+`,Commodity_Code AS `Commodity Code`, Commodity_Code_Text AS `Commodity Grade`,T001W_Dist_Prof_Plant AS `Plant Type`,Mvmt_Type AS `Movement Type Code` ,Amt_Comp_Code_Curr,Amt_PC_Local_Curr, Volume_Qty_GAL, Weight_Qty, Volume_Qty, Volume_Qty_BB6
    from `{uc_catalog_name}`.`{uc_curated_schema}`.{fcb_dynamic_view_name} fcb
    WHERE
    (array_contains(SPLIT({gains_and_losses_security_function_name}.`Company_Code-Text`,',') ,fcb.`Company_Code-Text`) OR ({gains_and_losses_security_function_name}.`Company_Code-Text`='ALL')) AND
    (array_contains(SPLIT({gains_and_losses_security_function_name}.`Profit_Center-Text`,',') ,fcb.`Profit_Center-Text`) OR ({gains_and_losses_security_function_name}.`Profit_Center-Text`='ALL')) AND
    (array_contains(SPLIT({gains_and_losses_security_function_name}.Fiscal_Year,',') ,fcb.Fiscal_Year) OR ({gains_and_losses_security_function_name}.Fiscal_Year='ALL')) AND
    (array_contains(SPLIT({gains_and_losses_security_function_name}.Posting_Period,',') ,fcb.Posting_Period) OR ({gains_and_losses_security_function_name}.Posting_Period='ALL')) AND
    (array_contains(SPLIT({gains_and_losses_security_function_name}.Division,',') ,fcb.Division) OR ({gains_and_losses_security_function_name}.Division='ALL')) AND
    (array_contains(SPLIT({gains_and_losses_security_function_name}.Account_No,',') ,fcb.Account_No) OR ({gains_and_losses_security_function_name}.Account_No='ALL'))AND
    (array_contains(SPLIT({gains_and_losses_security_function_name}.Document_Type,',') ,fcb.Document_Type) OR ({gains_and_losses_security_function_name}.Document_Type='ALL')) AND
    (array_contains(SPLIT({gains_and_losses_security_function_name}.`Plant-Text`,',') ,fcb.`Plant-Text`) OR ({gains_and_losses_security_function_name}.`Plant-Text`='ALL')) AND
    (array_contains(SPLIT({gains_and_losses_security_function_name}.`Mvmt_Type-Text`,',') ,fcb.`Mvmt_Type-Text`) OR ({gains_and_losses_security_function_name}.`Mvmt_Type-Text`='ALL')) AND
    (array_contains(SPLIT({gains_and_losses_security_function_name}.`Material-Text`,',') ,fcb.`Material-Text`) OR ({gains_and_losses_security_function_name}.`Material-Text`='ALL')) AND
    (fcb.T001W_Dist_Prof_Plant<>"ZLI" AND fcb.T001W_Dist_Prof_Plant<>"ZTS") AND 
    Account_No IN ("0006300220","0006330020","0006330030","0006330160","0006330202","0006330203","0006330220","0007480300") AND
    Mvmt_Type IN ("911","912","915","Y20","Y15","Y51","Y52","Y53","Y03","Y05","Y06","Y07","701","551","552","951","952","702","Y08","Y54","Y16","Y21","916")
    AND fcb.OPFLAG != 'D'
    )
    SELECT `Profit Center`,`Movement Type+`,`Commodity Code`, `Commodity Grade`, `Plant Type`, `Movement Type Code`,
    CASE WHEN {gains_and_losses_security_function_name}.Display_Unit = 'Actual Value' THEN `R/3 Amount`
    WHEN {gains_and_losses_security_function_name}.Display_Unit = 'Thousands' THEN `R/3 Amount`*0.001
    WHEN {gains_and_losses_security_function_name}.Display_Unit = 'Millions' THEN `R/3 Amount`*0.000001
    WHEN {gains_and_losses_security_function_name}.Display_Unit = 'Billions' THEN `R/3 Amount`*0.000000001
    END
    AS `R/3 Amount`,

    CASE WHEN {gains_and_losses_security_function_name}.Display_Unit = 'Actual Value' THEN `R/3 Quantity`
    WHEN {gains_and_losses_security_function_name}.Display_Unit = 'Thousands' THEN `R/3 Quantity`*0.001
    WHEN {gains_and_losses_security_function_name}.Display_Unit = 'Millions' THEN `R/3 Quantity`*0.000001
    WHEN {gains_and_losses_security_function_name}.Display_Unit = 'Billions' THEN `R/3 Quantity`*0.000000001
    END
    AS `R/3 Quantity`

    FROM
    (
    SELECT  `Profit Center`,`Movement Type+`,`Commodity Code`, `Commodity Grade`, `Plant Type`, `Movement Type Code`, 
    CASE WHEN {gains_and_losses_security_function_name}.CURRENCY="Local Currency" THEN SUM(Amt_Comp_Code_Curr)  
        WHEN {gains_and_losses_security_function_name}.CURRENCY="Group Currency" THEN SUM(Amt_PC_Local_Curr) END AS `R/3 Amount`,

    CASE  WHEN {gains_and_losses_security_function_name}.UOM="KG" THEN SUM(Weight_Qty)
        WHEN  {gains_and_losses_security_function_name}.UOM="L15" THEN SUM(Volume_Qty) 
        WHEN  {gains_and_losses_security_function_name}.UOM="GAL" THEN SUM(Volume_Qty_GAL)  
        WHEN  {gains_and_losses_security_function_name}.UOM="BBL" THEN SUM(Volume_Qty_BB6)
        ELSE 0 END AS `R/3 Quantity`

    FROM temp

    GROUP BY  `Profit Center`,`Movement Type+`,`Commodity Code`, `Commodity Grade`, `Plant Type`, `Movement Type Code`)""")

    assignFunctionPermission(uc_catalog_name, uc_curated_schema, gains_and_losses_security_function_name, tbl_owner_grp,security_end_user_aad_group_name, security_object_aad_group_name, security_functional_dev_aad_group, security_functional_readers_aad_group)

    print("gains_and_losses_security function dropped and recreated in the schema")
else:
    spark.sql(f"""
    CREATE OR REPLACE FUNCTION `{uc_catalog_name}`.`{uc_curated_schema}`.{gains_and_losses_security_function_name}(`Company_Code-Text` STRING,`Profit_Center-Text` STRING, Fiscal_Year STRING, Posting_Period STRING, Division STRING, Account_No STRING, Document_Type STRING, `Plant-Text` STRING,`Mvmt_Type-Text` STRING,`Material-Text` STRING, CURRENCY STRING, UOM STRING, Display_Unit STRING) 

    RETURNS TABLE (`Profit Center` STRING,`Movement Type+` STRING,`Commodity Code` STRING, `Commodity Grade` STRING, `Plant_Type` STRING, `Movement Type Code`  STRING,`R/3 Amount` decimal(25,2), `R/3 Quantity` decimal(25,2))

    RETURN 

    with temp as (
    select CL_Profit_Center AS `Profit Center`,fcb.CL_Mvmt_Type_Plus AS `Movement Type+`,Commodity_Code AS `Commodity Code`, Commodity_Code_Text AS `Commodity Grade`,T001W_Dist_Prof_Plant AS `Plant Type`,Mvmt_Type AS `Movement Type Code` ,Amt_Comp_Code_Curr,Amt_PC_Local_Curr, Volume_Qty_GAL, Weight_Qty, Volume_Qty, Volume_Qty_BB6
    from `{uc_catalog_name}`.`{uc_curated_schema}`.{fcb_dynamic_view_name} fcb
    WHERE
    (array_contains(SPLIT({gains_and_losses_security_function_name}.`Company_Code-Text`,',') ,fcb.`Company_Code-Text`) OR ({gains_and_losses_security_function_name}.`Company_Code-Text`='ALL')) AND
    (array_contains(SPLIT({gains_and_losses_security_function_name}.`Profit_Center-Text`,',') ,fcb.`Profit_Center-Text`) OR ({gains_and_losses_security_function_name}.`Profit_Center-Text`='ALL')) AND
    (array_contains(SPLIT({gains_and_losses_security_function_name}.Fiscal_Year,',') ,fcb.Fiscal_Year) OR ({gains_and_losses_security_function_name}.Fiscal_Year='ALL')) AND
    (array_contains(SPLIT({gains_and_losses_security_function_name}.Posting_Period,',') ,fcb.Posting_Period) OR ({gains_and_losses_security_function_name}.Posting_Period='ALL')) AND
    (array_contains(SPLIT({gains_and_losses_security_function_name}.Division,',') ,fcb.Division) OR ({gains_and_losses_security_function_name}.Division='ALL')) AND
    (array_contains(SPLIT({gains_and_losses_security_function_name}.Account_No,',') ,fcb.Account_No) OR ({gains_and_losses_security_function_name}.Account_No='ALL'))AND
    (array_contains(SPLIT({gains_and_losses_security_function_name}.Document_Type,',') ,fcb.Document_Type) OR ({gains_and_losses_security_function_name}.Document_Type='ALL')) AND
    (array_contains(SPLIT({gains_and_losses_security_function_name}.`Plant-Text`,',') ,fcb.`Plant-Text`) OR ({gains_and_losses_security_function_name}.`Plant-Text`='ALL')) AND
    (array_contains(SPLIT({gains_and_losses_security_function_name}.`Material-Text`,',') ,fcb.`Material-Text`) OR ({gains_and_losses_security_function_name}.`Material-Text`='ALL')) AND
    (array_contains(SPLIT({gains_and_losses_security_function_name}.`Mvmt_Type-Text`,',') ,fcb.`Mvmt_Type-Text`) OR ({gains_and_losses_security_function_name}.`Mvmt_Type-Text`='ALL')) AND
    (fcb.T001W_Dist_Prof_Plant<>"ZLI" AND fcb.T001W_Dist_Prof_Plant<>"ZTS") AND 
    Account_No IN ("0006300220","0006330020","0006330030","0006330160","0006330202","0006330203","0006330220","0007480300") AND
    Mvmt_Type IN ("911","912","915","Y20","Y15","Y51","Y52","Y53","Y03","Y05","Y06","Y07","701","551","552","951","952","702","Y08","Y54","Y16","Y21","916")
    AND fcb.OPFLAG != 'D'
    )
    SELECT `Profit Center`,`Movement Type+`,`Commodity Code`, `Commodity Grade`, `Plant Type`, `Movement Type Code`,
    CASE WHEN {gains_and_losses_security_function_name}.Display_Unit = 'Actual Value' THEN `R/3 Amount`
    WHEN {gains_and_losses_security_function_name}.Display_Unit = 'Thousands' THEN `R/3 Amount`*0.001
    WHEN {gains_and_losses_security_function_name}.Display_Unit = 'Millions' THEN `R/3 Amount`*0.000001
    WHEN {gains_and_losses_security_function_name}.Display_Unit = 'Billions' THEN `R/3 Amount`*0.000000001
    END
    AS `R/3 Amount`,

    CASE WHEN {gains_and_losses_security_function_name}.Display_Unit = 'Actual Value' THEN `R/3 Quantity`
    WHEN {gains_and_losses_security_function_name}.Display_Unit = 'Thousands' THEN `R/3 Quantity`*0.001
    WHEN {gains_and_losses_security_function_name}.Display_Unit = 'Millions' THEN `R/3 Quantity`*0.000001
    WHEN {gains_and_losses_security_function_name}.Display_Unit = 'Billions' THEN `R/3 Quantity`*0.000000001
    END
    AS `R/3 Quantity`
    FROM
    (
    SELECT  `Profit Center`,`Movement Type+`,`Commodity Code`, `Commodity Grade`, `Plant Type`, `Movement Type Code`, 
    CASE WHEN {gains_and_losses_security_function_name}.CURRENCY="Local Currency" THEN SUM(Amt_Comp_Code_Curr)  
        WHEN {gains_and_losses_security_function_name}.CURRENCY="Group Currency" THEN SUM(Amt_PC_Local_Curr) END AS `R/3 Amount`,

    CASE  WHEN {gains_and_losses_security_function_name}.UOM="KG" THEN SUM(Weight_Qty)
        WHEN  {gains_and_losses_security_function_name}.UOM="L15" THEN SUM(Volume_Qty) 
        WHEN  {gains_and_losses_security_function_name}.UOM="GAL" THEN SUM(Volume_Qty_GAL)  
        WHEN  {gains_and_losses_security_function_name}.UOM="BBL" THEN SUM(Volume_Qty_BB6)
        ELSE 0 END AS `R/3 Quantity`

    FROM temp

    GROUP BY  `Profit Center`,`Movement Type+`,`Commodity Code`, `Commodity Grade`, `Plant Type`, `Movement Type Code`)""")

    assignFunctionPermission(uc_catalog_name, uc_curated_schema, gains_and_losses_security_function_name, tbl_owner_grp,security_end_user_aad_group_name, security_object_aad_group_name, security_functional_dev_aad_group, security_functional_readers_aad_group)
