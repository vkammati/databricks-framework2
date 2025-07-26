# Databricks notebook source
# DBTITLE 1,Calling Common Functions
# MAGIC %run ./common_functions

# COMMAND ----------

fact_fi_act_line_item_cols = ['Client',
'Rec_No_Line_Itm_Rec',
'Ledger',
'Fiscal_Year',
'Currency_Key',
'Debit_Credit_Indicator',
'Posting_Period',
'Accounting_Document_No',
'Document_Line',
'Company_Code',
'Profit_Center',
'Functional_Area',
'Account_No',
'Partner_Profit_Center',
'Amt_Comp_Code_Curr',
'Amt_PC_Local_Curr',
'Document_Date',
'Document_Posting_Date',
'Ref_Document_No',
'Ref_Fiscal_Year',
'Line_Itm_Account_Doc_No',
'Ref_Document_Type',
'Plant',
'Material_Number',
'Customer_Number',
'Account_No_Supplier',
'Purchasing_Document_No',
'Sales_Order_Doc_No',
'Distribution_Channel',
'Division',
'Mvmt_Type',
'Document_Type',
'Receiving_Issue_Plant',
'Ext_Mode_Transport',
'Weight_Qty',
'Volume_Unit_L15',
'Volume_Qty',
'Volume_GAL',
'Volume_BBL',
'No_Principal_Prchg_Agmt',
'Doc_No_Ref_Doc',
'Parcel_ID',
'Smart_Contract_No',
'OIL_TSW_Deal_Number',
'Document_Date_In_Doc',
'External_Bill_Of_Lading',
'Endur_Cargo_ID',
'BGI_Delivery_ID',
'Endur_Parcel_ID',
'Volume_Qty_GAL',
'Volume_Unit_GAL',
'Volume_Qty_BB6',
'Volume_Unit_BB6',
'CL_LT_Reference_Doc_No',
'CL_RT_Reference_Doc_No',
'Material_Document_Number',
'Material_Document_Year',
'Material_Document_Itm',
'MM_Mvmt_Type',
'Debit_Or_Credit_Indicator',
'MM_Plant',
'MM_Receiving_Plant',
'MM_Material',
'MM_Receiving_Material',
'MM_Volume_L15',
'CL_Account_No_Functional_Area',
'MM_Material_Key',
'CL_Receiving_Issuing_Mat_Key',
'CL_UOM_In_GAL',
'CL_Lcl_Currency',
'CL_Mvmt_Type_Plus',
'CL_Profit_Center',
'CL_Mvmt_Group',
'CL_GL_Account_Key',
'Material_Key',
'ingested_at',
'Source_ID',
'OPFLAG'
]

md_fi_gl_account_cols = ['Language_Key',
'Chart_of_Accounts',
'GL_Account_No',
'GL_Account_Long_Text',
'Source_ID'
]

md_mm_material_cols = ['Material_Description',
'Material_Group',
'Material_Number',
'Product_Group_Code',
'Product_Group_Description',
'Product_Sub_Class',
'Product_Sub_Class_Description',
'Material_Group_Description',
'Source_ID'
]

md_mm_vendor_cols = ['Account_No_Supplier',
'Name_1',
'Source_ID'
]

md_mm_plant_cols = ['Plant',
'Name',
'Sales_District',
'Dist_Profile_Plant',
'Description',
'Source_ID'
]

md_sd_customer_cols = ['Customer_Number',
'Name_1',
'Source_ID'
]

md_fi_comp_code_cols = ['Company_Code',
'Currency_Key',
'Name_Comp_Code',
'Source_ID'
]

md_mm_zmaterial_cols = ['Material',
'DEX_Code',
'Commodity_Code',
'Medium_Description',
'Commodity_Code_Text',
'Source_ID'
]

md_mm_mov_type_cols = ['Mvmt_Type',
'Mvmt_Type_Text',
'Source_ID'
]

md_sd_dist_channel_cols = ['Distribution_Channel',
'Distribution_Channel_Text',
'Source_ID'
]

md_fi_profit_center_cols = ['Profit_Center',
'Profit_Center_Text',
'Source_ID'
]

# COMMAND ----------

# DBTITLE 1,Full and Delta Load Logic
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import *
from pyspark.sql.functions import lpad,format_string
spark = SparkSession.getActiveSession()

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

table_name="USE_CASE_FCB_DN_SUPPLY_MARGIN"
path = curated_folder_path+"/"+table_name

df_fact_fi_act_line_item = spark.read.table(f"`{uc_catalog_name}`.`{uc_eh_fi_schema}`.fact_fi_act_line_item")
df_fact_fi_act_line_item  = df_fact_fi_act_line_item.withColumn("Current_Date", date_format(current_date(), "YYYY-MM-dd"))
df_fact_fi_act_line_item  = df_fact_fi_act_line_item.withColumn("Start_Date", add_months(col("Current_Date"),-13))
df_fact_fi_act_line_item = df_fact_fi_act_line_item.filter(df_fact_fi_act_line_item.Document_Posting_Date >= df_fact_fi_act_line_item.Start_Date)
df_fact_fi_act_line_item = df_fact_fi_act_line_item.select(*fact_fi_act_line_item_cols).withColumnRenamed("Source_ID","fact_fi_act_line_item_Source_ID")


df_md_fi_gl_account = spark.read.table(f"`{uc_catalog_name}`.`{uc_eh_md_schema}`.md_fi_gl_account")
df_md_fi_gl_account = df_md_fi_gl_account.select(*md_fi_gl_account_cols).withColumnRenamed("Source_ID","md_fi_gl_account_Source_ID")

df_md_mm_material = spark.read.table(f"`{uc_catalog_name}`.`{uc_eh_md_schema}`.md_mm_material")
df_md_mm_material = df_md_mm_material.select(*md_mm_material_cols).withColumnRenamed("Source_ID","md_mm_material_Source_ID")

df_md_mm_vendor = spark.read.table(f"`{uc_catalog_name}`.`{uc_eh_md_schema}`.md_mm_vendor")
df_md_mm_vendor = df_md_mm_vendor.select(*md_mm_vendor_cols).withColumnRenamed("Source_ID","md_mm_vendor_Source_ID")

df_md_mm_plant = spark.read.table(f"`{uc_catalog_name}`.`{uc_eh_md_schema}`.md_mm_plant")
df_md_mm_plant = df_md_mm_plant.select(*md_mm_plant_cols).withColumnRenamed("Source_ID","md_mm_plant_Source_ID")

df_md_sd_customer = spark.read.table(f"`{uc_catalog_name}`.`{uc_eh_md_schema}`.md_sd_customer")
df_md_sd_customer = df_md_sd_customer.select(*md_sd_customer_cols).withColumnRenamed("Source_ID","md_sd_customer_Source_ID")

df_md_fi_comp_code = spark.read.table(f"`{uc_catalog_name}`.`{uc_eh_md_schema}`.md_fi_comp_code")
df_md_fi_comp_code = df_md_fi_comp_code.select(*md_fi_comp_code_cols).withColumnRenamed("Source_ID","md_fi_comp_code_Source_ID")

df_md_mm_zmaterial = spark.read.table(f"`{uc_catalog_name}`.`{uc_eh_md_schema}`.md_mm_zmaterial")
df_md_mm_zmaterial = df_md_mm_zmaterial.select(*md_mm_zmaterial_cols).withColumnRenamed("Source_ID","md_mm_zmaterial_Source_ID")

df_md_mm_mov_type = spark.read.table(f"`{uc_catalog_name}`.`{uc_eh_md_schema}`.md_mm_mov_type")
df_md_mm_mov_type = df_md_mm_mov_type.select(*md_mm_mov_type_cols).withColumnRenamed("Source_ID","md_mm_mov_type_Source_ID")

df_md_sd_dist_channel = spark.read.table(f"`{uc_catalog_name}`.`{uc_eh_md_schema}`.md_sd_dist_channel")
df_md_sd_dist_channel = df_md_sd_dist_channel.select(*md_sd_dist_channel_cols).withColumnRenamed("Source_ID","md_sd_dist_channel_Source_ID")

df_md_fi_profit_center = spark.read.table(f"`{uc_catalog_name}`.`{uc_eh_md_schema}`.md_fi_profit_center")
df_md_fi_profit_center = df_md_fi_profit_center.select(*md_fi_profit_center_cols).withColumnRenamed("Source_ID","md_fi_profit_center_Source_ID")

if load_type == "DELTA":
    df_fact_fi_act_line_item_delta = get_latest_delta(df_fact_fi_act_line_item, "sede-x-DATTA-FCB-CURATED-workflow", "ingested_at", uc_catalog_name, uc_raw_schema)
    df_fact_fi_act_line_item = df_fact_fi_act_line_item_delta

    #Separate deleted records from inserted / updated records. This is done to have nulls in all fields except primary keys for delete records in driving table.
    df_fact_fi_act_line_item_deleted=df_fact_fi_act_line_item.filter(F.col('OPFLAG')=='D').select('Rec_No_Line_Itm_Rec','OPFLAG',col("fact_fi_act_line_item_Source_ID").alias("Source_ID"), 'Client')

    df_fact_fi_act_line_item_1 = df_fact_fi_act_line_item.filter(F.col('OPFLAG')!='D')

    # Since its creating Null records in star join table, We are reading full record from MD tables
    df_md_mm_material = df_md_mm_material.withColumnRenamed("Material_Number","MATNR_Material_Number")
    
    df_join1 = df_fact_fi_act_line_item_1.join(df_md_fi_gl_account, ((df_fact_fi_act_line_item_1.Account_No == df_md_fi_gl_account.GL_Account_No) & (df_fact_fi_act_line_item_1.fact_fi_act_line_item_Source_ID == df_md_fi_gl_account.md_fi_gl_account_Source_ID)), "leftouter").join(df_md_mm_material, ((df_fact_fi_act_line_item_1.Material_Number == df_md_mm_material.MATNR_Material_Number) & (df_fact_fi_act_line_item_1.fact_fi_act_line_item_Source_ID == df_md_mm_material.md_mm_material_Source_ID)), "leftouter")
    
    df_join1 = df_join1 \
    .withColumn("GL-Text", concat_ws('-', df_join1.CL_GL_Account_Key, df_join1.GL_Account_Long_Text))\
    .withColumn("Material-Text",concat_ws('-', df_join1.Material_Key, df_join1.Material_Description))
    
    windowSpec = Window.partitionBy("CL_GL_Account_Key","Plant","Material_Number").orderBy("Material_Number")
    window_df = df_join1.withColumn("Amt_PC_Local_Curr_Sum", sum(col("Amt_PC_Local_Curr")).over(windowSpec)).withColumn("Volume_Qty_Sum", sum(col("Volume_Qty")).over(windowSpec))\
                  
    df_join2 = window_df.withColumn("CL_RR_UNIT_RATE", window_df.Amt_PC_Local_Curr_Sum/window_df.Volume_Qty_Sum)
    df_join2 = df_join2.select(*[col(column_name) for column_name in df_join2.columns if column_name not in {'Add_Oil_Gas_Qty','GL_Account_No','MATNR_Material_Number','ingested_at'}])

    df_md_mm_vendor = df_md_mm_vendor.withColumnRenamed("Account_No_Supplier", 
                                                                  "LFA1_Account_No_Supplier")
                                        
    df_join3 = df_join2.join(df_md_mm_vendor, ((df_join2.Account_No_Supplier == df_md_mm_vendor.LFA1_Account_No_Supplier) & (df_join2.fact_fi_act_line_item_Source_ID == df_md_mm_vendor.md_mm_vendor_Source_ID)), "leftouter")
    df_join3 = df_join3.select(*[col(column_name) for column_name in df_join3.columns if column_name not in {'LFA1_Account_No_Supplier','ingested_at'}])
  
    df_md_mm_plant_1 = df_md_mm_plant.withColumnRenamed("Plant", "T001W_Plant")\
                                            .withColumnRenamed("Name", "MSEG_Name")\
                                             .withColumnRenamed("Sales_District", "MSEG_Sales_District")
                                             
    df_join4 = df_join3.join(df_md_mm_plant_1, ((df_join3.MM_Plant == df_md_mm_plant_1.T001W_Plant) & (df_join3.fact_fi_act_line_item_Source_ID == df_md_mm_plant_1.md_mm_plant_Source_ID)), "leftouter")
    df_join4 = df_join4.withColumn('Plant-Text',concat_ws('-', df_join4.T001W_Plant,df_join4.MSEG_Name))
    df_join4 = df_join4.select(*[col(column_name) for column_name in df_join4.columns if column_name not in {'T001W_Plant','ingested_at'}])

    df_md_fi_comp_code = df_md_fi_comp_code.withColumnRenamed("Company_Code", "T001_Company_Code")\
                                        .withColumnRenamed("Currency_Key", "T001_Currency_Key")
    df_join5 = df_join4.join(df_md_fi_comp_code, ((df_join4.Company_Code == df_md_fi_comp_code.T001_Company_Code) & (df_join4.fact_fi_act_line_item_Source_ID == df_md_fi_comp_code.md_fi_comp_code_Source_ID)), "inner")  
    df_join5 = df_join5.withColumn('Company_Code-Text',concat_ws('-', df_join5.Company_Code,df_join5.Name_Comp_Code))                  
    df_join5 = df_join5.select(*[col(column_name) for column_name in df_join5.columns if column_name not in {'T001_Company_Code','ingested_at'}])

    df_md_mm_plant_2 = df_md_mm_plant.withColumnRenamed("Plant","T001W_Plant")\
                            .withColumnRenamed("Name","T001W_Name")\
                            .withColumnRenamed("Sales_District","T001W_Sales_District")\
                            .withColumnRenamed("Dist_Profile_Plant","T001W_Dist_Prof_Plant")\
                            .withColumnRenamed("Description","T001W_Description")\
                            .withColumnRenamed("md_mm_plant_Source_ID","md_mm_plant_2_Source_ID")
    df_join6 = df_join5.join(df_md_mm_plant_2, ((df_join5.Plant == df_md_mm_plant_2.T001W_Plant) & (df_join5.fact_fi_act_line_item_Source_ID == df_md_mm_plant_2.md_mm_plant_2_Source_ID)), "leftouter")   
    df_join6 = df_join6.select(*[col(column_name) for column_name in df_join6.columns if column_name not in {'T001W_Plant','ingested_at'}])

    df_md_sd_customer = df_md_sd_customer.withColumnRenamed("Customer_Number", "KNA1_Customer_Number")\
                                            .withColumnRenamed("Name_1", "KNA1_Name_1")
    df_join7 = df_join6.join(df_md_sd_customer, ((df_join6.Customer_Number == df_md_sd_customer.KNA1_Customer_Number) & (df_join6.fact_fi_act_line_item_Source_ID == df_md_sd_customer.md_sd_customer_Source_ID)),"leftouter")
    df_join7 = df_join7.select(*[col(column_name) for column_name in df_join7.columns if column_name not in {'KNA1_Customer_Number','ingested_at'}])    

    df_md_mm_plant_3 = df_md_mm_plant.withColumnRenamed("Plant","T001W_Plant")\
                            .withColumnRenamed("Name","T001W_Name_3")\
                            .withColumnRenamed("Sales_District","T001W_Sales_District_3")\
                            .withColumnRenamed("Dist_Profile_Plant","T001W_Dist_Prof_Plant_3")\
                            .withColumnRenamed("Description","T001W_Description_3")\
                            .withColumnRenamed("md_mm_plant_Source_ID","md_mm_plant_3_Source_ID")
    df_join8 = df_join7.join(df_md_mm_plant_3, ((df_join7.Receiving_Issue_Plant == df_md_mm_plant_3.T001W_Plant) & (df_join7.fact_fi_act_line_item_Source_ID == df_md_mm_plant_3.md_mm_plant_3_Source_ID)), "leftouter")

    df_join8 = df_join8.select(*[col(column_name) for column_name in df_join8.columns if column_name not in {'T001W_Plant','ingested_at'}])
   

    df_md_mm_plant_4 = df_md_mm_plant.withColumnRenamed("Plant","T001W_Plant")\
                                            .withColumnRenamed("Name", "MSEG_Name_4")\
                                            .withColumnRenamed("Sales_District", "MSEG_Sales_District_4")\
                                            .withColumnRenamed("Dist_Profile_Plant","MSEG_Dist_Prof_Plant_4")\
                                            .withColumnRenamed("Description","MSEG_Description_4")\
                                            .withColumnRenamed("md_mm_plant_Source_ID","md_mm_plant_4_Source_ID")
    df_join9 = df_join8.join(df_md_mm_plant_4, ((df_join8.MM_Receiving_Plant == df_md_mm_plant_4.T001W_Plant) & (df_join8.fact_fi_act_line_item_Source_ID == df_md_mm_plant_4.md_mm_plant_4_Source_ID)), "leftouter")
    df_join9 = df_join9.select(*[col(column_name) for column_name in df_join9.columns if column_name not in {'T001W_Plant','ingested_at'}])
    
    df_md_mm_mov_type = df_md_mm_mov_type.withColumnRenamed("Mvmt_Type","T156HT_Mvmt_Type")
    df_join10 = df_join9.join(df_md_mm_mov_type,((df_join9.Mvmt_Type == df_md_mm_mov_type.T156HT_Mvmt_Type) & (df_join9.fact_fi_act_line_item_Source_ID == df_md_mm_mov_type.md_mm_mov_type_Source_ID)),"leftouter")
    df_join10 = df_join10.withColumn('Mvmt_Type-Text', concat_ws('-', df_join10.Mvmt_Type, df_join10.Mvmt_Type_Text))
        
    df_md_sd_dist_channel = df_md_sd_dist_channel.withColumnRenamed("Distribution_Channel","TVTWT_Distribution_Channel")
    df_md_sd_dist_channel=df_md_sd_dist_channel.withColumn("TVTWT_Distribution_Channel",lpad("TVTWT_Distribution_Channel",2,"0"))
    df_join10 = df_join10.withColumn("Distribution_Channel",lpad("Distribution_Channel",2,"0"))
    df_join11 = df_join10.join(df_md_sd_dist_channel,((df_join10.Distribution_Channel == df_md_sd_dist_channel.TVTWT_Distribution_Channel) & (df_join10.fact_fi_act_line_item_Source_ID == df_md_sd_dist_channel.md_sd_dist_channel_Source_ID)),"leftouter")
    df_join11 = df_join11.withColumn('Distribution_Channel-Text',concat_ws('-', df_join11.TVTWT_Distribution_Channel, df_join11.Distribution_Channel_Text))
    
    df_md_fi_profit_center = df_md_fi_profit_center.withColumnRenamed("Profit_Center","CEPCT_Profit_Center")
    df_join12 = df_join11.join(df_md_fi_profit_center,((df_join11.Profit_Center == df_md_fi_profit_center.CEPCT_Profit_Center) & (df_join11.fact_fi_act_line_item_Source_ID == df_md_fi_profit_center.md_fi_profit_center_Source_ID)),"leftouter")

    df_join13 = df_join12.withColumn("Profit_Center",regexp_replace(col("Profit_Center"), r'^[0]*', ''))
    df_join13 = df_join13.withColumn('Profit_Center-Text',concat_ws('-', df_join13.Profit_Center, df_join13.Profit_Center_Text))
    df_join13 = df_join13.select(*[col(column_name) for column_name in df_join13.columns if column_name not in {'ingested_at'}])

    df_final = df_join13.join(df_md_mm_zmaterial, ((df_join13.Material_Number == df_md_mm_zmaterial.Material) & (df_join13.fact_fi_act_line_item_Source_ID == df_md_mm_zmaterial.md_mm_zmaterial_Source_ID)), "leftouter")

    df_final = df_final.select("Client","Rec_No_Line_Itm_Rec","Ledger","Fiscal_Year","Debit_Credit_Indicator","Posting_Period","Accounting_Document_No","Document_Line","Company_Code","Profit_Center","Functional_Area","Account_No","Amt_Comp_Code_Curr","Amt_PC_Local_Curr","Ref_Document_No","Ref_Fiscal_Year","Line_Itm_Account_Doc_No","Ref_Document_Type","Plant","Material_Number","Customer_Number","Account_No_Supplier","Sales_Order_Doc_No","Mvmt_Type","Document_Type","Purchasing_Document_No","Currency_Key","Distribution_Channel","Partner_Profit_Center","Division","Document_Date","Document_Posting_Date","Receiving_Issue_Plant","Weight_Qty","Volume_Unit_L15","Volume_Qty","Volume_GAL","Volume_BBL","Ext_Mode_Transport","No_Principal_Prchg_Agmt","Doc_No_Ref_Doc","Smart_Contract_No","External_Bill_Of_Lading","Parcel_ID","Document_Date_In_Doc","OIL_TSW_Deal_Number","Endur_Cargo_ID","BGI_Delivery_ID","Endur_Parcel_ID","Volume_Qty_GAL","Volume_Unit_GAL","Volume_Qty_BB6","Volume_Unit_BB6","CL_LT_Reference_Doc_No","CL_RT_Reference_Doc_No","Material_Document_Number","Material_Document_Year","Material_Document_Itm","MM_Mvmt_Type","Debit_Or_Credit_Indicator","MM_Plant","MM_Receiving_Plant","MM_Material","MM_Receiving_Material","MM_Volume_L15","CL_Account_No_Functional_Area","MM_Material_Key","CL_Receiving_Issuing_Mat_Key","CL_UOM_In_GAL","CL_Lcl_Currency","CL_Mvmt_Type_Plus","CL_Profit_Center","CL_Mvmt_Group","CL_GL_Account_Key","Material_Key","GL-Text","Material-Text","Language_Key","Chart_of_Accounts","GL_Account_Long_Text","Material_Group","Material_Group_Description","Product_Group_Code","Product_Group_Description","Material_Description","Product_Sub_Class","Product_Sub_Class_Description","Name_1","MSEG_Name","MSEG_Sales_District","T001_Currency_Key","T001W_Dist_Prof_Plant","T001W_Sales_District","T001W_Name","T001W_Description","KNA1_Name_1","T001W_Name_3","T001W_Sales_District_3","MSEG_Name_4","MSEG_Sales_District_4","DEX_Code","Commodity_Code","Medium_Description","Commodity_Code_Text","Mvmt_Type-Text","Distribution_Channel-Text","Profit_Center-Text","Plant-Text","Company_Code-Text",col("fact_fi_act_line_item_Source_ID").alias("Source_ID"), "OPFLAG")

    #Union deleted records from driving table keeping only primary keys and OPFLAG as D
    df_final = df_final.unionByName(df_fact_fi_act_line_item_deleted, allowMissingColumns=True)

    df_final=df_final.distinct()    
    deltaTable = DeltaTable.forName(spark, f"`{uc_catalog_name}`.`{uc_curated_schema}`.{table_name}")

    (deltaTable.alias('target') \
    .merge(df_final.alias('source'), "target.Source_ID = source.Source_ID and target.Rec_No_Line_Itm_Rec = source.Rec_No_Line_Itm_Rec")	
    .whenMatchedUpdate( set =
    {
      "Client": "source.Client",
        "Rec_No_Line_Itm_Rec": "source.Rec_No_Line_Itm_Rec",
        "Ledger": "source.Ledger",
        "Fiscal_Year": "source.Fiscal_Year",
        "Debit_Credit_Indicator": "source.Debit_Credit_Indicator",
        "Posting_Period": "source.Posting_Period",
        "Accounting_Document_No": "source.Accounting_Document_No",
        "Document_Line": "source.Document_Line",
        "Company_Code": "source.Company_Code",
        "Profit_Center": "source.Profit_Center",
        "Functional_Area": "source.Functional_Area",
        "Account_No": "source.Account_No",
        "Amt_Comp_Code_Curr": "source.Amt_Comp_Code_Curr",
        "Amt_PC_Local_Curr": "source.Amt_PC_Local_Curr",
        "Ref_Document_No": "source.Ref_Document_No",
        "Ref_Fiscal_Year": "source.Ref_Fiscal_Year",
        "Line_Itm_Account_Doc_No": "source.Line_Itm_Account_Doc_No",
        "Ref_Document_Type": "source.Ref_Document_Type",
        "Plant": "source.Plant",
        "Material_Number": "source.Material_Number",
        "Customer_Number": "source.Customer_Number",
        "Account_No_Supplier": "source.Account_No_Supplier",
        "Sales_Order_Doc_No": "source.Sales_Order_Doc_No",
        "Mvmt_Type": "source.Mvmt_Type",
        "Document_Type": "source.Document_Type",
        "Purchasing_Document_No": "source.Purchasing_Document_No",
        "Currency_Key": "source.Currency_Key",
        "Distribution_Channel": "source.Distribution_Channel",
        "Partner_Profit_Center": "source.Partner_Profit_Center",
        "Division": "source.Division",
        "Document_Date": "source.Document_Date",
        "Document_Posting_Date": "source.Document_Posting_Date",
        "Receiving_Issue_Plant": "source.Receiving_Issue_Plant",
        "Weight_Qty": "source.Weight_Qty",
        "Volume_Unit_L15": "source.Volume_Unit_L15",
        "Volume_Qty": "source.Volume_Qty",
        "Volume_GAL": "source.Volume_GAL",
        "Volume_BBL": "source.Volume_BBL",
        "Ext_Mode_Transport": "source.Ext_Mode_Transport",
        "No_Principal_Prchg_Agmt": "source.No_Principal_Prchg_Agmt",
        "Doc_No_Ref_Doc": "source.Doc_No_Ref_Doc",
        "Smart_Contract_No": "source.Smart_Contract_No",
        "External_Bill_Of_Lading": "source.External_Bill_Of_Lading",
        "Parcel_ID": "source.Parcel_ID",
        "Document_Date_In_Doc": "source.Document_Date_In_Doc",
        "OIL_TSW_Deal_Number": "source.OIL_TSW_Deal_Number",
        "Endur_Cargo_ID": "source.Endur_Cargo_ID",
        "BGI_Delivery_ID": "source.BGI_Delivery_ID",
        "Endur_Parcel_ID": "source.Endur_Parcel_ID",
        "Volume_Qty_GAL": "source.Volume_Qty_GAL",
        "Volume_Unit_GAL": "source.Volume_Unit_GAL",
        "Volume_Qty_BB6": "source.Volume_Qty_BB6",
        "Volume_Unit_BB6": "source.Volume_Unit_BB6",
        "CL_LT_Reference_Doc_No": "source.CL_LT_Reference_Doc_No",
        "CL_RT_Reference_Doc_No": "source.CL_RT_Reference_Doc_No",
        "Material_Document_Number": "source.Material_Document_Number",
        "Material_Document_Year": "source.Material_Document_Year",
        "Material_Document_Itm": "source.Material_Document_Itm",
        "MM_Mvmt_Type": "source.MM_Mvmt_Type",
        "Debit_Or_Credit_Indicator": "source.Debit_Or_Credit_Indicator",
        "MM_Plant": "source.MM_Plant",
        "MM_Receiving_Plant": "source.MM_Receiving_Plant",
        "MM_Material": "source.MM_Material",
        "MM_Receiving_Material": "source.MM_Receiving_Material",
        "MM_Volume_L15": "source.MM_Volume_L15",
        "CL_Account_No_Functional_Area": "source.CL_Account_No_Functional_Area",
        "MM_Material_Key": "source.MM_Material_Key",
        "CL_Receiving_Issuing_Mat_Key": "source.CL_Receiving_Issuing_Mat_Key",
        "CL_UOM_In_GAL": "source.CL_UOM_In_GAL",
        "CL_Lcl_Currency": "source.CL_Lcl_Currency",
        "CL_Mvmt_Type_Plus": "source.CL_Mvmt_Type_Plus",
        "CL_Profit_Center": "source.CL_Profit_Center",
        "CL_Mvmt_Group": "source.CL_Mvmt_Group",
        "CL_GL_Account_Key": "source.CL_GL_Account_Key",
        "Material_Key": "source.Material_Key",
        "`GL-Text`": "source.`GL-Text`",
        "`Material-Text`": "source.`Material-Text`",
        "Language_Key": "source.Language_Key",
        "Chart_of_Accounts": "source.Chart_of_Accounts",
        "GL_Account_Long_Text": "source.GL_Account_Long_Text",
        "Material_Group": "source.Material_Group",
        "Material_Group_Description": "source.Material_Group_Description",
        "Product_Group_Code": "source.Product_Group_Code",
        "Product_Group_Description": "source.Product_Group_Description",
        "Material_Description": "source.Material_Description",
        "Product_Sub_Class": "source.Product_Sub_Class",
        "Product_Sub_Class_Description": "source.Product_Sub_Class_Description",
        "Name_1": "source.Name_1",
        "MSEG_Name": "source.MSEG_Name",
        "MSEG_Sales_District": "source.MSEG_Sales_District",
        "T001_Currency_Key": "source.T001_Currency_Key",
        "T001W_Dist_Prof_Plant": "source.T001W_Dist_Prof_Plant",
        "T001W_Sales_District": "source.T001W_Sales_District",
        "T001W_Name": "source.T001W_Name",
        "T001W_Description": "source.T001W_Description",
        "KNA1_Name_1": "source.KNA1_Name_1",
        "T001W_Name_3": "source.T001W_Name_3",
        "T001W_Sales_District_3": "source.T001W_Sales_District_3",
        "MSEG_Name_4": "source.MSEG_Name_4",
        "MSEG_Sales_District_4": "source.MSEG_Sales_District_4",
        "DEX_Code": "source.DEX_Code",
        "Commodity_Code": "source.Commodity_Code",
        "Medium_Description": "source.Medium_Description",
        "Commodity_Code_Text": "source.Commodity_Code_Text",
        "`Mvmt_Type-Text`": "source.`Mvmt_Type-Text`",
        "`Distribution_Channel-Text`": "source.`Distribution_Channel-Text`",
        "`Profit_Center-Text`": "source.`Profit_Center-Text`",
        "`Plant-Text`": "source.`Plant-Text`",
        "`Company_Code-Text`": "source.`Company_Code-Text`",
        "ingested_at": current_timestamp(),
        "Source_ID": "source.Source_ID",
        "OPFLAG":"source.OPFLAG"
    }) \
    .whenNotMatchedInsert(values =
    {
      "Client": "source.Client",
        "Rec_No_Line_Itm_Rec": "source.Rec_No_Line_Itm_Rec",
        "Ledger": "source.Ledger",
        "Fiscal_Year": "source.Fiscal_Year",
        "Debit_Credit_Indicator": "source.Debit_Credit_Indicator",
        "Posting_Period": "source.Posting_Period",
        "Accounting_Document_No": "source.Accounting_Document_No",
        "Document_Line": "source.Document_Line",
        "Company_Code": "source.Company_Code",
        "Profit_Center": "source.Profit_Center",
        "Functional_Area": "source.Functional_Area",
        "Account_No": "source.Account_No",
        "Amt_Comp_Code_Curr": "source.Amt_Comp_Code_Curr",
        "Amt_PC_Local_Curr": "source.Amt_PC_Local_Curr",
        "Ref_Document_No": "source.Ref_Document_No",
        "Ref_Fiscal_Year": "source.Ref_Fiscal_Year",
        "Line_Itm_Account_Doc_No": "source.Line_Itm_Account_Doc_No",
        "Ref_Document_Type": "source.Ref_Document_Type",
        "Plant": "source.Plant",
        "Material_Number": "source.Material_Number",
        "Customer_Number": "source.Customer_Number",
        "Account_No_Supplier": "source.Account_No_Supplier",
        "Sales_Order_Doc_No": "source.Sales_Order_Doc_No",
        "Mvmt_Type": "source.Mvmt_Type",
        "Document_Type": "source.Document_Type",
        "Purchasing_Document_No": "source.Purchasing_Document_No",
        "Currency_Key": "source.Currency_Key",
        "Distribution_Channel": "source.Distribution_Channel",
        "Partner_Profit_Center": "source.Partner_Profit_Center",
        "Division": "source.Division",
        "Document_Date": "source.Document_Date",
        "Document_Posting_Date": "source.Document_Posting_Date",
        "Receiving_Issue_Plant": "source.Receiving_Issue_Plant",
        "Weight_Qty": "source.Weight_Qty",
        "Volume_Unit_L15": "source.Volume_Unit_L15",
        "Volume_Qty": "source.Volume_Qty",
        "Volume_GAL": "source.Volume_GAL",
        "Volume_BBL": "source.Volume_BBL",
        "Ext_Mode_Transport": "source.Ext_Mode_Transport",
        "No_Principal_Prchg_Agmt": "source.No_Principal_Prchg_Agmt",
        "Doc_No_Ref_Doc": "source.Doc_No_Ref_Doc",
        "Smart_Contract_No": "source.Smart_Contract_No",
        "External_Bill_Of_Lading": "source.External_Bill_Of_Lading",
        "Parcel_ID": "source.Parcel_ID",
        "Document_Date_In_Doc": "source.Document_Date_In_Doc",
        "OIL_TSW_Deal_Number": "source.OIL_TSW_Deal_Number",
        "Endur_Cargo_ID": "source.Endur_Cargo_ID",
        "BGI_Delivery_ID": "source.BGI_Delivery_ID",
        "Endur_Parcel_ID": "source.Endur_Parcel_ID",
        "Volume_Qty_GAL": "source.Volume_Qty_GAL",
        "Volume_Unit_GAL": "source.Volume_Unit_GAL",
        "Volume_Qty_BB6": "source.Volume_Qty_BB6",
        "Volume_Unit_BB6": "source.Volume_Unit_BB6",
        "CL_LT_Reference_Doc_No": "source.CL_LT_Reference_Doc_No",
        "CL_RT_Reference_Doc_No": "source.CL_RT_Reference_Doc_No",
        "Material_Document_Number": "source.Material_Document_Number",
        "Material_Document_Year": "source.Material_Document_Year",
        "Material_Document_Itm": "source.Material_Document_Itm",
        "MM_Mvmt_Type": "source.MM_Mvmt_Type",
        "Debit_Or_Credit_Indicator": "source.Debit_Or_Credit_Indicator",
        "MM_Plant": "source.MM_Plant",
        "MM_Receiving_Plant": "source.MM_Receiving_Plant",
        "MM_Material": "source.MM_Material",
        "MM_Receiving_Material": "source.MM_Receiving_Material",
        "MM_Volume_L15": "source.MM_Volume_L15",
        "CL_Account_No_Functional_Area": "source.CL_Account_No_Functional_Area",
        "MM_Material_Key": "source.MM_Material_Key",
        "CL_Receiving_Issuing_Mat_Key": "source.CL_Receiving_Issuing_Mat_Key",
        "CL_UOM_In_GAL": "source.CL_UOM_In_GAL",
        "CL_Lcl_Currency": "source.CL_Lcl_Currency",
        "CL_Mvmt_Type_Plus": "source.CL_Mvmt_Type_Plus",
        "CL_Profit_Center": "source.CL_Profit_Center",
        "CL_Mvmt_Group": "source.CL_Mvmt_Group",
        "CL_GL_Account_Key": "source.CL_GL_Account_Key",
        "Material_Key": "source.Material_Key",
        "`GL-Text`": "source.`GL-Text`",
        "`Material-Text`": "source.`Material-Text`",
        "Language_Key": "source.Language_Key",
        "Chart_of_Accounts": "source.Chart_of_Accounts",
        "GL_Account_Long_Text": "source.GL_Account_Long_Text",
        "Material_Group": "source.Material_Group",
        "Material_Group_Description": "source.Material_Group_Description",
        "Product_Group_Code": "source.Product_Group_Code",
        "Product_Group_Description": "source.Product_Group_Description",
        "Material_Description": "source.Material_Description",
        "Product_Sub_Class": "source.Product_Sub_Class",
        "Product_Sub_Class_Description": "source.Product_Sub_Class_Description",
        "Name_1": "source.Name_1",
        "MSEG_Name": "source.MSEG_Name",
        "MSEG_Sales_District": "source.MSEG_Sales_District",
        "T001_Currency_Key": "source.T001_Currency_Key",
        "T001W_Dist_Prof_Plant": "source.T001W_Dist_Prof_Plant",
        "T001W_Sales_District": "source.T001W_Sales_District",
        "T001W_Name": "source.T001W_Name",
        "T001W_Description": "source.T001W_Description",
        "KNA1_Name_1": "source.KNA1_Name_1",
        "T001W_Name_3": "source.T001W_Name_3",
        "T001W_Sales_District_3": "source.T001W_Sales_District_3",
        "MSEG_Name_4": "source.MSEG_Name_4",
        "MSEG_Sales_District_4": "source.MSEG_Sales_District_4",
        "DEX_Code": "source.DEX_Code",
        "Commodity_Code": "source.Commodity_Code",
        "Medium_Description": "source.Medium_Description",
        "Commodity_Code_Text": "source.Commodity_Code_Text",
        "`Mvmt_Type-Text`": "source.`Mvmt_Type-Text`",
        "`Distribution_Channel-Text`": "source.`Distribution_Channel-Text`",
        "`Profit_Center-Text`": "source.`Profit_Center-Text`",
        "`Plant-Text`": "source.`Plant-Text`",
        "`Company_Code-Text`": "source.`Company_Code-Text`",
        "ingested_at": current_timestamp(),
        "Source_ID": "source.Source_ID",
        "OPFLAG": "source.OPFLAG"
    })
    .execute()
    )  
else:

    print("Full Load!!")  
    #Separate deleted records from inserted / updated records. This is done to have nulls in all fields except primary keys for delete records in driving table.
    df_fact_fi_act_line_item_deleted=df_fact_fi_act_line_item.filter(F.col('OPFLAG')=='D').select('Rec_No_Line_Itm_Rec','OPFLAG',col("fact_fi_act_line_item_Source_ID").alias("Source_ID"), 'Client')

    df_fact_fi_act_line_item_1 = df_fact_fi_act_line_item.filter(F.col('OPFLAG')!='D')

    df_md_mm_material = df_md_mm_material.withColumnRenamed("Material_Number","MATNR_Material_Number")

    df_join1 = df_fact_fi_act_line_item_1.join(df_md_fi_gl_account, ((df_fact_fi_act_line_item_1.Account_No == df_md_fi_gl_account.GL_Account_No) & (df_fact_fi_act_line_item_1.fact_fi_act_line_item_Source_ID == df_md_fi_gl_account.md_fi_gl_account_Source_ID)), "leftouter")
    df_join1 = df_join1.join(df_md_mm_material, ((df_fact_fi_act_line_item_1.Material_Number == df_md_mm_material.MATNR_Material_Number) & (df_fact_fi_act_line_item_1.fact_fi_act_line_item_Source_ID == df_md_mm_material.md_mm_material_Source_ID)), "leftouter")\
                       .withColumn("GL-Text", concat_ws('-', df_fact_fi_act_line_item_1.CL_GL_Account_Key, df_md_fi_gl_account.GL_Account_Long_Text))\
                       .withColumn("Material-Text",concat_ws('-', df_fact_fi_act_line_item_1.Material_Key, df_md_mm_material.Material_Description))
    
    windowSpec = Window.partitionBy("CL_GL_Account_Key","Plant","Material_Number").orderBy("Material_Number")
    window_df = df_join1.withColumn("Amt_PC_Local_Curr_Sum", sum(col("Amt_PC_Local_Curr")).over(windowSpec)).withColumn("Volume_Qty_Sum", sum(col("Volume_Qty")).over(windowSpec))\
                 
    df_join2 = window_df.withColumn("CL_RR_UNIT_RATE", window_df.Amt_PC_Local_Curr_Sum/window_df.Volume_Qty_Sum)
    df_join2 = df_join2.select(*[col(column_name) for column_name in df_join2.columns if column_name not in {'Add_Oil_Gas_Qty','GL_Account_No','MATNR_Material_Number','ingested_at'}]) 
    df_md_mm_vendor = df_md_mm_vendor.withColumnRenamed("Account_No_Supplier", 
                                                                  "LFA1_Account_No_Supplier")                         
    df_join3 = df_join2.join(df_md_mm_vendor, ((df_join2.Account_No_Supplier == df_md_mm_vendor.LFA1_Account_No_Supplier) & (df_join2.fact_fi_act_line_item_Source_ID == df_md_mm_vendor.md_mm_vendor_Source_ID)), "leftouter")
    df_join3 = df_join3.select(*[col(column_name) for column_name in df_join3.columns if column_name not in {'LFA1_Account_No_Supplier','ingested_at'}])
  
    df_md_mm_plant_1 = df_md_mm_plant.withColumnRenamed("Plant", "T001W_Plant")\
                                            .withColumnRenamed("Name", "MSEG_Name")\
                                             .withColumnRenamed("Sales_District", "MSEG_Sales_District")
                                             
    df_join4 = df_join3.join(df_md_mm_plant_1, ((df_join3.MM_Plant == df_md_mm_plant_1.T001W_Plant) & (df_join3.fact_fi_act_line_item_Source_ID == df_md_mm_plant_1.md_mm_plant_Source_ID)), "leftouter")
    df_join4 = df_join4.withColumn('Plant-Text',concat_ws('-', df_join4.T001W_Plant,df_join4.MSEG_Name))
    df_join4 = df_join4.select(*[col(column_name) for column_name in df_join4.columns if column_name not in {'T001W_Plant','ingested_at'}])

    df_md_fi_comp_code = df_md_fi_comp_code.withColumnRenamed("Company_Code", "T001_Company_Code")\
                                        .withColumnRenamed("Currency_Key", "T001_Currency_Key")
    df_join5 = df_join4.join(df_md_fi_comp_code, ((df_join4.Company_Code == df_md_fi_comp_code.T001_Company_Code) & (df_join4.fact_fi_act_line_item_Source_ID == df_md_fi_comp_code.md_fi_comp_code_Source_ID)), "inner")  
    df_join5 = df_join5.withColumn('Company_Code-Text',concat_ws('-', df_join5.Company_Code,df_join5.Name_Comp_Code))                  
    df_join5 = df_join5.select(*[col(column_name) for column_name in df_join5.columns if column_name not in {'T001_Company_Code','ingested_at'}])

    df_md_mm_plant_2 = df_md_mm_plant.withColumnRenamed("Plant","T001W_Plant")\
                            .withColumnRenamed("Name","T001W_Name")\
                            .withColumnRenamed("Sales_District","T001W_Sales_District")\
                            .withColumnRenamed("Dist_Profile_Plant","T001W_Dist_Prof_Plant")\
                            .withColumnRenamed("Description","T001W_Description")\
                            .withColumnRenamed("md_mm_plant_Source_ID","md_mm_plant_2_Source_ID")
    df_join6 = df_join5.join(df_md_mm_plant_2, ((df_join5.Plant == df_md_mm_plant_2.T001W_Plant) & (df_join5.fact_fi_act_line_item_Source_ID == df_md_mm_plant_2.md_mm_plant_2_Source_ID)), "leftouter")
    df_join6 = df_join6.select(*[col(column_name) for column_name in df_join6.columns if column_name not in {'T001W_Plant','ingested_at'}])

    df_md_sd_customer = df_md_sd_customer.withColumnRenamed("Customer_Number", "KNA1_Customer_Number")\
                                            .withColumnRenamed("Name_1", "KNA1_Name_1")
    df_join7 = df_join6.join(df_md_sd_customer, ((df_join6.Customer_Number == df_md_sd_customer.KNA1_Customer_Number) & (df_join6.fact_fi_act_line_item_Source_ID == df_md_sd_customer.md_sd_customer_Source_ID)),"leftouter")
    df_join7 = df_join7.select(*[col(column_name) for column_name in df_join7.columns if column_name not in {'KNA1_Customer_Number','ingested_at'}])    

    df_md_mm_plant_3 = df_md_mm_plant.withColumnRenamed("Plant","T001W_Plant")\
                            .withColumnRenamed("Name","T001W_Name_3")\
                            .withColumnRenamed("Sales_District","T001W_Sales_District_3")\
                            .withColumnRenamed("Dist_Profile_Plant","T001W_Dist_Prof_Plant_3")\
                            .withColumnRenamed("Description","T001W_Description_3")\
                            .withColumnRenamed("md_mm_plant_Source_ID","md_mm_plant_3_Source_ID")
    df_join8 = df_join7.join(df_md_mm_plant_3, ((df_join7.Receiving_Issue_Plant == df_md_mm_plant_3.T001W_Plant) & (df_join7.fact_fi_act_line_item_Source_ID == df_md_mm_plant_3.md_mm_plant_3_Source_ID)), "leftouter")

    df_join8 = df_join8.select(*[col(column_name) for column_name in df_join8.columns if column_name not in {'T001W_Plant','ingested_at'}])
   
    df_md_mm_plant_4 = df_md_mm_plant.withColumnRenamed("Plant","T001W_Plant")\
                                            .withColumnRenamed("Name", "MSEG_Name_4")\
                                            .withColumnRenamed("Sales_District", "MSEG_Sales_District_4")\
                                            .withColumnRenamed("Dist_Profile_Plant","MSEG_Dist_Prof_Plant_4")\
                                            .withColumnRenamed("Description","MSEG_Description_4")\
                                            .withColumnRenamed("md_mm_plant_Source_ID","md_mm_plant_4_Source_ID")
    df_join9 = df_join8.join(df_md_mm_plant_4, ((df_join8.MM_Receiving_Plant == df_md_mm_plant_4.T001W_Plant) & (df_join8.fact_fi_act_line_item_Source_ID == df_md_mm_plant_4.md_mm_plant_4_Source_ID)), "leftouter")
    df_join9 = df_join9.select(*[col(column_name) for column_name in df_join9.columns if column_name not in {'T001W_Plant','ingested_at'}])
    
    df_md_mm_mov_type = df_md_mm_mov_type.withColumnRenamed("Mvmt_Type","T156HT_Mvmt_Type")
    df_join10 = df_join9.join(df_md_mm_mov_type,((df_join9.Mvmt_Type == df_md_mm_mov_type.T156HT_Mvmt_Type) & (df_join9.fact_fi_act_line_item_Source_ID == df_md_mm_mov_type.md_mm_mov_type_Source_ID)),"leftouter")
    df_join10 = df_join10.withColumn('Mvmt_Type-Text', concat_ws('-', df_join10.Mvmt_Type, df_join10.Mvmt_Type_Text))
    df_join10 = df_join10.select(*[col(column_name) for column_name in df_join10.columns if column_name not in {'T156HT_Mvmt_Type','ingested_at'}])    
    
    df_md_sd_dist_channel = df_md_sd_dist_channel.withColumnRenamed("Distribution_Channel","TVTWT_Distribution_Channel")
    df_md_sd_dist_channel=df_md_sd_dist_channel.withColumn("TVTWT_Distribution_Channel",lpad("TVTWT_Distribution_Channel",2,"0"))
    df_join10 = df_join10.withColumn("Distribution_Channel",lpad("Distribution_Channel",2,"0"))
    df_join11 = df_join10.join(df_md_sd_dist_channel,((df_join10.Distribution_Channel == df_md_sd_dist_channel.TVTWT_Distribution_Channel) & (df_join10.fact_fi_act_line_item_Source_ID == df_md_sd_dist_channel.md_sd_dist_channel_Source_ID)),"leftouter")
    df_join11 = df_join11.withColumn('Distribution_Channel-Text', concat_ws('-', df_join11.Distribution_Channel,df_join11. Distribution_Channel_Text))
    df_join11 = df_join11.select(*[col(column_name) for column_name in df_join11.columns if column_name not in {'TVTWT_Distribution_Channel','ingested_at'}])   

    df_md_fi_profit_center = df_md_fi_profit_center.withColumnRenamed("Profit_Center","CEPCT_Profit_Center")
    df_join12 = df_join11.join(df_md_fi_profit_center,((df_join11.Profit_Center == df_md_fi_profit_center.CEPCT_Profit_Center) & (df_join11.fact_fi_act_line_item_Source_ID == df_md_fi_profit_center.md_fi_profit_center_Source_ID)),"leftouter")

    df_join13 = df_join12.withColumn("Profit_Center",regexp_replace(col("Profit_Center"), r'^[0]*', ''))
    df_join13 = df_join13.withColumn('Profit_Center-Text',concat_ws('-', df_join13.Profit_Center, df_join13.Profit_Center_Text))
    df_join13 = df_join13.select(*[col(column_name) for column_name in df_join13.columns if column_name not in {'ingested_at'}]) 

    df_final = df_join13.join(df_md_mm_zmaterial, ((df_join13.Material_Number == df_md_mm_zmaterial.Material) & (df_join13.fact_fi_act_line_item_Source_ID == df_md_mm_zmaterial.md_mm_zmaterial_Source_ID)), "leftouter")

    df_final = df_final.select("Client","Rec_No_Line_Itm_Rec","Ledger","Fiscal_Year","Debit_Credit_Indicator","Posting_Period","Accounting_Document_No","Document_Line","Company_Code","Profit_Center","Functional_Area","Account_No","Amt_Comp_Code_Curr","Amt_PC_Local_Curr","Ref_Document_No","Ref_Fiscal_Year","Line_Itm_Account_Doc_No","Ref_Document_Type","Plant","Material_Number","Customer_Number","Account_No_Supplier","Sales_Order_Doc_No","Mvmt_Type","Document_Type","Purchasing_Document_No","Currency_Key","Distribution_Channel","Partner_Profit_Center","Division","Document_Date","Document_Posting_Date","Receiving_Issue_Plant","Weight_Qty","Volume_Unit_L15","Volume_Qty","Volume_GAL","Volume_BBL","Ext_Mode_Transport","No_Principal_Prchg_Agmt","Doc_No_Ref_Doc","Smart_Contract_No","External_Bill_Of_Lading","Parcel_ID","Document_Date_In_Doc","OIL_TSW_Deal_Number","Endur_Cargo_ID","BGI_Delivery_ID","Endur_Parcel_ID","Volume_Qty_GAL","Volume_Unit_GAL","Volume_Qty_BB6","Volume_Unit_BB6","CL_LT_Reference_Doc_No","CL_RT_Reference_Doc_No","Material_Document_Number","Material_Document_Year","Material_Document_Itm","MM_Mvmt_Type","Debit_Or_Credit_Indicator","MM_Plant","MM_Receiving_Plant","MM_Material","MM_Receiving_Material","MM_Volume_L15","CL_Account_No_Functional_Area","MM_Material_Key","CL_Receiving_Issuing_Mat_Key","CL_UOM_In_GAL","CL_Lcl_Currency","CL_Mvmt_Type_Plus","CL_Profit_Center","CL_Mvmt_Group","CL_GL_Account_Key","Material_Key","GL-Text","Material-Text","Language_Key","Chart_of_Accounts","GL_Account_Long_Text","Material_Group","Material_Group_Description","Product_Group_Code","Product_Group_Description","Material_Description","Product_Sub_Class","Product_Sub_Class_Description","Name_1","MSEG_Name","MSEG_Sales_District","T001_Currency_Key","T001W_Dist_Prof_Plant","T001W_Sales_District","T001W_Name","T001W_Description","KNA1_Name_1","T001W_Name_3","T001W_Sales_District_3","MSEG_Name_4","MSEG_Sales_District_4","DEX_Code","Commodity_Code","Medium_Description","Commodity_Code_Text","Mvmt_Type-Text","Distribution_Channel-Text","Profit_Center-Text","Plant-Text","Company_Code-Text", col("fact_fi_act_line_item_Source_ID").alias("Source_ID"), "OPFLAG")

    #Union deleted records from driving table keeping only primary keys and OPFLAG as D
    df_final = df_final.unionByName(df_fact_fi_act_line_item_deleted, allowMissingColumns=True)

    df_final=df_final.distinct()    
    df_final = df_final.withColumn("ingested_at",current_timestamp())
    
    df_final.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{path}/")

# COMMAND ----------

# DBTITLE 1,Deleting the "D" record from Curated layer Logic
# if load_type == "DELTA":
#     df_aecorsoft_GLPCA = spark.read.table(f"`{uc_catalog_name}`.`{uc_raw_schema}`.aecorsoft_glpca")
    
#     df_aecorsoft_GLPCA = glpca_filter(df_aecorsoft_GLPCA)

#     df_GLPCA_delete_records = get_latest_delete_records(df_aecorsoft_GLPCA, "sede-x-DATTA-FCB-CURATED-workflow", "ingested_at", "OPFLAG", uc_catalog_name, uc_raw_schema)

#     df_GLPCA_delete_records.createOrReplaceTempView("GLPCA_delete_records")

#     spark.sql(f"""DELETE FROM `{uc_catalog_name}`.`{uc_curated_schema}`.{table_name} AS star_join_table WHERE EXISTS (SELECT GL_SIRID FROM GLPCA_delete_records WHERE star_join_table.Rec_No_Line_Itm_Rec = GL_SIRID)""")
