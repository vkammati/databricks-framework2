from pyspark.sql.functions import *
from datta_pipeline_library.core.spark_init import spark
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.window import Window
from pyspark.sql.functions import col
from pyspark.sql import *;
from pyspark.sql.functions import *;
from pyspark.sql import functions as F
from pyspark.sql.types import *;

def insert_into_table(table_name, process_config_project_name, process_order, process_name,dlt_workflow_name,dlt_workflow_job_id):
    """Insert into delta table based on the input provided

    :param:table_name : name of the process_status table
    :param:project_name : project name
    :param:process_order : process order sequence number
    :param:process_name : process name 
    :return: insert statement
    """
    print("process_status_dataframe.py file insert_into_table()!!!:",table_name )

    df= spark.read.table(table_name).filter(col("dlt_workflow_name")==dlt_workflow_name)
    
    columns = ["project_name", "process_order", "process_name","dlt_workflow_name","dlt_workflow_job_id"]
    data = [(str(process_config_project_name), str(process_order), str(process_name),str(dlt_workflow_name),str(dlt_workflow_job_id))]
    
    new_row = spark.createDataFrame(data, columns)
    new_row = new_row.withColumn("run_start_date", current_timestamp()).withColumn("run_update_date", current_timestamp()).withColumn("run_id", lit(0)).withColumn("status", lit("started"))
    
    new_row = new_row.select(*[col(column_name) for column_name in new_row.columns if column_name not in {'id'}])
    print("process_status_dataframe.py file insert_into_table() new_row value:::", new_row)
    
    df = df.unionByName(new_row)
    df = df.withColumn("idx", monotonically_increasing_id())
    w = Window().orderBy("idx","run_start_date")
    df=df.withColumn("row_num", (0 + row_number().over(w)))   
    df = df.select(*[col(column_name) for column_name in df.columns if column_name not in {'run_id','idx'}])
    df = df.withColumnRenamed('row_num', 'run_id')
    df=df.select("run_id","project_name","process_order","process_name","dlt_workflow_name","dlt_workflow_job_id","run_start_date","run_update_date","status")
    
    dlt_workflow_name = "dlt_workflow_name"+"=="+ "'"+ dlt_workflow_name+ "'"
    print("dlt_workflow_name",dlt_workflow_name)
    df.write.format("delta").option("replaceWhere",dlt_workflow_name).mode("overwrite").insertInto(table_name)
    
    return True
    
def update_table(table_name,workflow_name,workflow_job_id):
    """Get Unity Catalog's table name.
    :param:table_name : name of the process_status table
    :return: max run id
    """

    df= spark.read.table(table_name)
    df= df.filter(df.dlt_workflow_name==workflow_name)

    df1=df.agg(max('run_id').alias("max_run_id"))
    df=df.join(df1)
    
    df = df.withColumn("run_update_date_new", F.when(F.col("run_id") == F.col("max_run_id"), current_timestamp()).otherwise(df["run_update_date"])).withColumn("status_new", F.when(F.col("run_id") == F.col("max_run_id"), lit("completed")).otherwise(df["status"]))
    print("df withColumn::",df)

    df = df.select(*[col(column_name) for column_name in df.columns if column_name not in {'max_run_id','run_update_date','status'}])
    
    df = df.withColumnRenamed('run_update_date_new', 'run_update_date')
    df = df.withColumnRenamed('status_new', 'status')
    
    dlt_workflow_name = "dlt_workflow_name"+"=="+ "'"+ workflow_name+ "'"
    print("dlt_workflow_name",dlt_workflow_name)
    df.write.format("delta").option("replaceWhere",dlt_workflow_name).mode("overwrite").insertInto(table_name)

    return True