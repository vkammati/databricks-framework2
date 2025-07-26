# Databricks notebook source
pip install msal

# COMMAND ----------

pip install adal

# COMMAND ----------

dbutils.library.restartPython() 

# COMMAND ----------

from datta_pipeline_library.helpers.adls import configure_spark_to_use_spn_to_write_to_adls_gen2
from datta_pipeline_library.helpers.spn import AzureSPN
from datetime import datetime
import requests
from requests.exceptions import HTTPError
import time
from typing import Optional
from ci_cd_helpers.auth import get_dbx_http_header
from datta_pipeline_library.core.base_config import (
    BaseConfig,
    CollibraConfig,
    CommonConfig,
    EnvConfig
)

# COMMAND ----------

env = dbutils.widgets.get(name="env")
repos_path = dbutils.widgets.get(name="repos_path")
host = dbutils.widgets.get(name="host")
job_name = dbutils.widgets.get(name="job_name")

# host = 'https://adb-7050837418260772.12.azuredatabricks.net'
# #job_name = "sede-x-DATTA-CURATED-main-workflow"
# job_name = "sede-x-DATTA-MD-EH-workflow"

# env = "dev"
# repos_path = "/Repos/DATTA-MVP2/DATTA-FCB-CURATED"

common_conf = CommonConfig.from_file(f"/Workspace/{repos_path.strip('/')}/conf/common/common_conf.json")
print("common_conf !!!")
env_conf = EnvConfig.from_file(f"/Workspace/{repos_path.strip('/')}/conf/{env}/conf.json")

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

spnvalue = spn.generate_ad_token()

# COMMAND ----------

def run_now(job_id: str, host: str, http_header: dict) -> str:
    """Trigger job run.
    
    :param job_id: Databricks job id
    :param host: Databricks host
    :param http_header: HTTP header used for the Databricks REST API
    :return: the run id
    """
    req = requests.post(
        f"{host}/api/2.1/jobs/run-now",
        headers=http_header,
        json={
            "job_id": job_id,
        }
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e

    return req.json()["run_id"]

# TODO: use pagination?
def get_job_id(job_name: str, host: str, http_header: dict) -> Optional[str]:
    """Get job id if it exists, None otherwise.
    
    :param job_name: Databricks job name
    :param host: Databricks host
    :param http_header: HTTP header used for the Databricks REST API
    :return: job id if it exists, None otherwise
    """
    job_id = None
    limit = 1
    expand_tasks = "false"

    query_string = f"name={job_name}&limit={limit}&expand_tasks={expand_tasks}"

    req = requests.get(
        f"{host}/api/2.1/jobs/list?{query_string}",
        headers=http_header,
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e

    jobs_key = "jobs"

    if jobs_key in req.json():
        job_id = req.json()[jobs_key][0]["job_id"]

    return job_id

# COMMAND ----------

http_header = get_dbx_http_header(spnvalue)
job_id = get_job_id(job_name, host, http_header)
print(job_id)

# COMMAND ----------

run_now(job_id, host, http_header)
