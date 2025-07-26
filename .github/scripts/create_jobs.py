"""
This script creates/updates all Databricks resources necessary for the pipeline to run properly:
- the DLT pipeline itself
- the workflow that wraps the DLT pipeline
- the workflow that registers tables to Unity Catalog
- the (interactive) cluster used to register tables to Unity Catalog

For each resource, the process is the same:
- get the name of the resource
- get the configuration template
- render the configuration template
- cast the rendered configuration as a dict
- call the Databricks REST API to create or update the resource
"""
#Added comment to test code

import argparse
from jinja2 import Environment, FileSystemLoader
import json
import os
import sys

from ci_cd_helpers.auth import get_dbx_http_header
from ci_cd_helpers.azure import generate_spn_ad_token
from ci_cd_helpers.helpers import (
    get_cluster_name,
    get_dlt_pipeline_name,
    get_register_table_uc_workflow_name,
    get_repos_parent_folder_path,
    get_repos_path,
    get_unique_repo_branch_id,
    get_wrapping_workflow_name
)
from ci_cd_helpers.clusters import create_or_update_cluster_by_name, get_cluster_id
from ci_cd_helpers.pipelines import create_or_update_pipeline_by_name
from ci_cd_helpers.workflows import create_or_update_workflow_by_name, get_job_id
from ci_cd_helpers.workspace import create_folder_if_not_exists


parser = argparse.ArgumentParser(description="Create all Databricks entites for the pipeline.")

parser.add_argument(
    "--host",
    help="Databricks host.",
    type=str,
)

parser.add_argument(
    "--tenant-id",
    help="Azure tenant id.",
    type=str,
)

parser.add_argument(
    "--spn-client-id",
    help="Service principal client id used to deploy code.",
    type=str,
)

parser.add_argument(
    "--spn-client-secret",
    help="Service principal client secret used to deploy code.",
    type=str,
)

parser.add_argument(
    "--repository",
    help="GitHub repository (with owner).",
    type=str,
)

parser.add_argument(
    "--branch-name",
    help="Name of the branch in GitHub",
    type=str,
)

parser.add_argument(
    "--emails",
    help="List of emails, separated by blank spaces, to contact when jobs fail.",
    type=str,
    nargs="*",
    default=[],
)

parser.add_argument(
    "--timeout",
    help="Timeout in seconds for the wrapping workflow. Default is 0, i.e. no timeout.",
    type=int,
    default=0,
)

parser.add_argument(
    "--env",
    help="Databricks/GitHub environment to use.",
    choices=["dev", "tst", "pre", "prd"],
    type=str,
)

parser.add_argument(
    "--deployment-type",
    help="Deployment type, either 'manual' or 'automatic'. Taken into account only when env=dev.",
    choices=["manual", "automatic"],
    type=str,
)

parser.add_argument(
    "--integration-test",
    help="Integration test type, either 'true' or 'false'. Taken into account only when env=dev.",
    choices=["true", "false"],
    type=str,
)

args = parser.parse_args()

#if args.env == "dev" and not args.deployment_type:
#    raise ValueError("When env=dev, parameter 'type' must be either 'manual' or 'automatic', not None.")


dlt_pipeline_conf_filename = "dlt_pipeline_conf.json"
workflow_conf_filename = "workflow_conf.json"
register_table_uc_conf_filename = "register_table_uc_conf.json"
cluster_conf_filename = "cluster_conf.json"
security_workflow_conf_filename = "security_workflow_conf.json"
optimize_vacuum_workflow_conf_filename = "optimize_vacuum_workflow_conf.json"
euh_main_workflow_conf_filename = "euh_main_workflow_conf.json"
eh_main_workflow_conf_filename = "eh_main_workflow_conf.json"
curated_weekly_full_load_workflow_conf_filename = "curated_weekly_full_load_workflow_conf.json"
mars_load_workflow_conf_filename = "mars_load_workflow_conf.json"
mars_sp_load_workflow_conf_filename = "mars_sp_load_workflow_conf.json"
mars_security_workflow_conf_filename = "mars_security_workflow_conf.json"

if args.integration_test == "true":
    unique_repo_branch_id = get_unique_repo_branch_id(args.repository, args.branch_name, args.deployment_type)
    suffix_curated_weekly_full_load = "curated-weekly-full-load-" +args.deployment_type
    unique_repo_branch_id_curated_weekly_full_load = get_unique_repo_branch_id(args.repository, args.branch_name, suffix_curated_weekly_full_load)
    suffix_eh_main = "eh-main-" +args.deployment_type
    unique_repo_branch_id_eh_main = get_unique_repo_branch_id(args.repository, args.branch_name, suffix_eh_main)
    suffix_euh_main = "euh-main-" +args.deployment_type
    unique_repo_branch_id_euh_main = get_unique_repo_branch_id(args.repository, args.branch_name, suffix_euh_main)
    suffix_optimize_vacuum = "optimize-vacuum-" +args.deployment_type
    unique_repo_branch_id_optimize_vacuum = get_unique_repo_branch_id(args.repository, args.branch_name, suffix_optimize_vacuum)
    suffix_security = "security-" +args.deployment_type
    unique_repo_branch_id_security = get_unique_repo_branch_id(args.repository, args.branch_name, suffix_security)
    suffix_mars_load = "mars-load-" +args.deployment_type
    unique_repo_branch_id_mars_load = get_unique_repo_branch_id(args.repository, args.branch_name, suffix_mars_load)
    suffix_mars_sp_load = "mars-sp-load-" +args.deployment_type
    unique_repo_branch_id_mars_sp_load = get_unique_repo_branch_id(args.repository, args.branch_name, suffix_mars_sp_load)
    suffix_mars_security = "mars-security-" +args.deployment_type
    unique_repo_branch_id_mars_security = get_unique_repo_branch_id(args.repository, args.branch_name, suffix_mars_security)
else:
    unique_repo_branch_id = ""
    suffix_curated_weekly_full_load = "-curated-weekly-full-load"
    unique_repo_branch_id_curated_weekly_full_load = ""
    suffix_eh_main = "-eh-main"
    unique_repo_branch_id_eh_main = ""
    suffix_euh_main = "-euh-main"
    unique_repo_branch_id_euh_main = ""
    suffix_optimize_vacuum = "-optimize-vacuum"
    unique_repo_branch_id_optimize_vacuum = ""
    suffix_security = "-security"
    unique_repo_branch_id_security = ""
    suffix_mars_load = "-mars-load"
    unique_repo_branch_id_mars_load = ""
    suffix_mars_sp_load = "-mars-sp-load"
    unique_repo_branch_id_mars_sp_load = ""
    suffix_mars_security = "-mars-security"
    unique_repo_branch_id_mars_security = ""

parent_folder_path = get_repos_parent_folder_path(args.repository)

if args.integration_test == "true":
    repos_path = get_repos_path(args.repository, args.branch_name, args.deployment_type)
else:
    repos_path = parent_folder_path + "/datta_fcb_curated"
requirements_filepath = f"{repos_path}/requirements.txt"

dir_path = os.path.dirname(os.path.abspath(__file__))
conf_folder_path = f"{dir_path}/../../conf"
common_conf_folder_path = f"{conf_folder_path}/common"

# the DLT and wrapping workflow confs are either in "integration_tests" or in "common" based on the environment we are using
#pipeline_conf_folder = "integration_tests" if args.env == "dev" else "common"
# pipeline_conf_folder = "common"
pipeline_conf_folder = "integration_tests" if args.integration_test == "true" else "common"
pipeline_conf_folder_path = f"{conf_folder_path}/{pipeline_conf_folder}"

environment_conf_folder = "integration_tests" if args.integration_test == "true" else args.env
environment_conf_folder_path = f"{conf_folder_path}/{environment_conf_folder}"

# necessary to call the Databricks REST API
ad_token = generate_spn_ad_token(args.tenant_id, args.spn_client_id, args.spn_client_secret)
http_header = get_dbx_http_header(ad_token)

# folders where Jinja templates are stored
common_templates = Environment(loader=FileSystemLoader(common_conf_folder_path))
pipeline_templates = Environment(loader=FileSystemLoader(pipeline_conf_folder_path))
environment_templates = Environment(loader=FileSystemLoader(environment_conf_folder_path))

# create or update cluster
cluster_conf_template = common_templates.get_template(cluster_conf_filename)

if args.integration_test == "true":
    cluster_name = get_cluster_name(args.repository, args.branch_name, args.deployment_type)
    cluster_conf_str = cluster_conf_template.render(cluster_name=cluster_name)
    cluster_conf = json.loads(cluster_conf_str)
    cluster_id = create_or_update_cluster_by_name(cluster_name, cluster_conf, args.host, http_header)
else:
    #cluster_name = "sede-x-datta-fcb-cluster"
    cluster_name = "sede-x-DATTA-MD-TXT-EUH-cluster"
    cluster_id = get_cluster_id(cluster_name, args.host, http_header)
#     cluster_name = get_cluster_name(args.repository)
# cluster_conf_str = cluster_conf_template.render(cluster_name=cluster_name)
# cluster_conf = json.loads(cluster_conf_str)
# cluster_id = create_or_update_cluster_by_name(cluster_name, cluster_conf, args.host, http_header)

# create or update register_table_uc workflow
# register_table_uc_conf_template = common_templates.get_template(register_table_uc_conf_filename)

# if args.integration_test == "true":
#     register_table_uc_name = get_register_table_uc_workflow_name(args.repository, args.branch_name, args.deployment_type)
# else:
#     #register_table_uc_name = "sede-x-datta-fcb-uc"
#     register_table_uc_name = get_register_table_uc_workflow_name(args.repository)

# register_table_uc_conf_str = register_table_uc_conf_template.render(
#     job_name=register_table_uc_name,
#     repos_path=repos_path,
#     cluster_id=cluster_id,
#     emails=args.emails,
# )

# register_table_uc_conf = json.loads(register_table_uc_conf_str)
# register_uc_job_id = create_or_update_workflow_by_name(register_table_uc_name, register_table_uc_conf, args.host, http_header)

# # get DLT pipeline conf
# dlt_conf_template = pipeline_templates.get_template(dlt_pipeline_conf_filename)

# if args.integration_test == "true":
#     dlt_pipeline_name = get_dlt_pipeline_name(args.repository, args.branch_name, args.deployment_type)
# else:
#     #dlt_pipeline_name = "sede-x-datta-fcb-dlt"
#     dlt_pipeline_name = get_dlt_pipeline_name(args.repository)
# is_dlt_development = args.env == "dev" and args.deployment_type == "manual"

# dlt_pipeline_conf_str = dlt_conf_template.render(
#     dlt_pipeline_name=dlt_pipeline_name,
#     requirements_filepath=requirements_filepath,
#     #register_uc_job_id=register_uc_job_id,
#     repos_path=repos_path,
#     host=args.host,
#     env=args.env,
#     unique_repo_branch_id=unique_repo_branch_id,  # this will not be taken into account in tst/pre/prd
#     development=is_dlt_development,
# )

# dlt_pipeline_conf = json.loads(dlt_pipeline_conf_str)
# pipeline_id = create_or_update_pipeline_by_name(dlt_pipeline_name, dlt_pipeline_conf, args.host, http_header)

# getting required job ids for all workflows
md_attr_euh_job_name = "sede-x-DATTA-MD-ATTR-EUH-workflow"
md_attr_euh_job_id = get_job_id(md_attr_euh_job_name, args.host, http_header)
print("md_attr_euh_job_id: ", md_attr_euh_job_id)
md_txt_euh_job_name = "sede-x-DATTA-MD-TXT-EUH-workflow"
md_txt_euh_job_id = get_job_id(md_txt_euh_job_name, args.host, http_header)
print("md_txt_euh_job_id: ", md_txt_euh_job_id)
mm_euh_job_name = "sede-x-DATTA-MM-EUH-workflow"
mm_euh_job_id = get_job_id(mm_euh_job_name, args.host, http_header)
print("mm_euh_job_id: ", mm_euh_job_id)
fi_euh_job_name = "sede-x-DATTA-FI-EUH-workflow"
fi_euh_job_id = get_job_id(fi_euh_job_name, args.host, http_header)
print("fi_euh_job_id: ", fi_euh_job_id)
fi_eh_job_name = "sede-x-DATTA-FI-EH-workflow"
fi_eh_job_id = get_job_id(fi_eh_job_name, args.host, http_header)
print("fi_eh_job_id: ", fi_eh_job_id)
md_eh_job_name = "sede-x-DATTA-MD-EH-workflow"
md_eh_job_id = get_job_id(md_eh_job_name, args.host, http_header)
print("md_eh_job_id: ", md_eh_job_id)

# create or update wrapping workflow
workflow_conf_template = environment_templates.get_template(workflow_conf_filename)

if args.integration_test == "true":
    workflow_name = get_wrapping_workflow_name(args.repository, args.branch_name, args.deployment_type)
else:    
    workflow_name = get_wrapping_workflow_name(args.repository)

workflow_conf_str = workflow_conf_template.render(
    workflow_name=workflow_name,
    unique_repo_branch_id=unique_repo_branch_id,  # this will not be taken into account in tst/pre/prd
    emails=args.emails,
    timeout=args.timeout,
    repos_path=repos_path,
    env=args.env,
    host=args.host,
    #register_table_to_uc_workflow_id=register_uc_job_id,
    cluster_id=cluster_id,
    requirements_filepath=requirements_filepath,
)

workflow_conf = json.loads(workflow_conf_str)
create_or_update_workflow_by_name(workflow_name, workflow_conf, args.host, http_header)

# create or update security wrapping workflow
security_workflow_conf_template = pipeline_templates.get_template(security_workflow_conf_filename)

if args.integration_test == "true":
    security_workflow_name = get_wrapping_workflow_name(args.repository, args.branch_name, suffix_security)
else:    
    security_workflow_full_suffix = args.repository+suffix_security
    security_workflow_name = get_wrapping_workflow_name(security_workflow_full_suffix)

security_workflow_conf_str = security_workflow_conf_template.render(
    security_workflow_name=security_workflow_name,
    unique_repo_branch_id=unique_repo_branch_id_security,  # this will not be taken into account in tst/pre/prd
    emails=args.emails,
    timeout=args.timeout,
    repos_path=repos_path,
    env=args.env,
    host=args.host,
    cluster_id=cluster_id,
)

security_workflow_conf = json.loads(security_workflow_conf_str)
create_or_update_workflow_by_name(security_workflow_name, security_workflow_conf, args.host, http_header)

# create or update optimize vacuum wrapping workflow
optimize_vacuum_workflow_conf_template = environment_templates.get_template(optimize_vacuum_workflow_conf_filename)

if args.integration_test == "true":
    optimize_vacuum_workflow_name = get_wrapping_workflow_name(args.repository, args.branch_name, suffix_optimize_vacuum)
else:    
    optimize_vacuum_workflow_full_suffix = args.repository+suffix_optimize_vacuum
    optimize_vacuum_workflow_name = get_wrapping_workflow_name(optimize_vacuum_workflow_full_suffix)

optimize_vacuum_workflow_conf_str = optimize_vacuum_workflow_conf_template.render(
    optimize_vacuum_workflow_name=optimize_vacuum_workflow_name,
    unique_repo_branch_id=unique_repo_branch_id_optimize_vacuum,  # this will not be taken into account in tst/pre/prd
    emails=args.emails,
    timeout=args.timeout,
    repos_path=repos_path,
    env=args.env,
    host=args.host,
    cluster_id=cluster_id,
)

optimize_vacuum_workflow_conf = json.loads(optimize_vacuum_workflow_conf_str)
create_or_update_workflow_by_name(optimize_vacuum_workflow_name, optimize_vacuum_workflow_conf, args.host, http_header)

# create or update euh main wrapping workflow
euh_main_workflow_conf_template = environment_templates.get_template(euh_main_workflow_conf_filename)

if args.integration_test == "true":
    euh_main_workflow_name = get_wrapping_workflow_name(args.repository, args.branch_name, suffix_euh_main)
else:    
    euh_main_workflow_full_suffix = args.repository+suffix_euh_main
    euh_main_workflow_name = get_wrapping_workflow_name(euh_main_workflow_full_suffix)

euh_main_workflow_conf_str = euh_main_workflow_conf_template.render(
    euh_main_workflow_name=euh_main_workflow_name,
    unique_repo_branch_id=unique_repo_branch_id_euh_main,  # this will not be taken into account in tst/pre/prd
    emails=args.emails,
    timeout=args.timeout,
    repos_path=repos_path,
    env=args.env,
    host=args.host,
    cluster_id=cluster_id,
    md_attr_euh_job_id=md_attr_euh_job_id,
    md_txt_euh_job_id=md_txt_euh_job_id,
    mm_euh_job_id=mm_euh_job_id,
    fi_euh_job_id=fi_euh_job_id,
)

euh_main_workflow_conf = json.loads(euh_main_workflow_conf_str)
create_or_update_workflow_by_name(euh_main_workflow_name, euh_main_workflow_conf, args.host, http_header)

# create or update eh main wrapping workflow
eh_main_workflow_conf_template = environment_templates.get_template(eh_main_workflow_conf_filename)

if args.integration_test == "true":
    eh_main_workflow_name = get_wrapping_workflow_name(args.repository, args.branch_name, suffix_eh_main)
else:    
    eh_main_workflow_full_suffix = args.repository+suffix_eh_main
    eh_main_workflow_name = get_wrapping_workflow_name(eh_main_workflow_full_suffix)

eh_main_workflow_conf_str = eh_main_workflow_conf_template.render(
    eh_main_workflow_name=eh_main_workflow_name,
    unique_repo_branch_id=unique_repo_branch_id_eh_main,  # this will not be taken into account in tst/pre/prd
    emails=args.emails,
    timeout=args.timeout,
    repos_path=repos_path,
    env=args.env,
    host=args.host,
    cluster_id=cluster_id,
    fi_eh_job_id=fi_eh_job_id,
    md_eh_job_id=md_eh_job_id,
)

eh_main_workflow_conf = json.loads(eh_main_workflow_conf_str)
create_or_update_workflow_by_name(eh_main_workflow_name, eh_main_workflow_conf, args.host, http_header)

# create or update curated weekly full load wrapping workflow
curated_weekly_full_load_workflow_conf_template = environment_templates.get_template(curated_weekly_full_load_workflow_conf_filename)

if args.integration_test == "true":
    curated_weekly_full_load_workflow_name = get_wrapping_workflow_name(args.repository, args.branch_name, suffix_curated_weekly_full_load)
else:    
    curated_weekly_full_load_workflow_full_suffix = args.repository+suffix_curated_weekly_full_load
    curated_weekly_full_load_workflow_name = get_wrapping_workflow_name(curated_weekly_full_load_workflow_full_suffix)

curated_weekly_full_load_workflow_conf_str = curated_weekly_full_load_workflow_conf_template.render(
    curated_weekly_full_load_workflow_name=curated_weekly_full_load_workflow_name,
    unique_repo_branch_id=unique_repo_branch_id_curated_weekly_full_load,  # this will not be taken into account in tst/pre/prd
    emails=args.emails,
    timeout=args.timeout,
    repos_path=repos_path,
    env=args.env,
    host=args.host,
    cluster_id=cluster_id,
)

curated_weekly_full_load_workflow_conf = json.loads(curated_weekly_full_load_workflow_conf_str)
create_or_update_workflow_by_name(curated_weekly_full_load_workflow_name, curated_weekly_full_load_workflow_conf, args.host, http_header)

# create or update mars-load wrapping workflow
mars_load_workflow_conf_template = environment_templates.get_template(mars_load_workflow_conf_filename)

if args.integration_test == "true":
    mars_load_workflow_name = get_wrapping_workflow_name(args.repository, args.branch_name, suffix_mars_load)
else:    
    mars_load_workflow_full_suffix = args.repository+suffix_mars_load
    mars_load_workflow_name = get_wrapping_workflow_name(mars_load_workflow_full_suffix)

mars_load_workflow_conf_str = mars_load_workflow_conf_template.render(
    mars_load_workflow_name=mars_load_workflow_name,
    unique_repo_branch_id=unique_repo_branch_id_mars_load,  # this will not be taken into account in tst/pre/prd
    emails=args.emails,
    timeout=args.timeout,
    repos_path=repos_path,
    env=args.env,
    host=args.host,
    cluster_id=cluster_id,
)

mars_load_workflow_conf = json.loads(mars_load_workflow_conf_str)
create_or_update_workflow_by_name(mars_load_workflow_name, mars_load_workflow_conf, args.host, http_header)

# create or update mars-sp-load wrapping workflow
mars_sp_load_workflow_conf_template = environment_templates.get_template(mars_sp_load_workflow_conf_filename)

if args.integration_test == "true":
    mars_sp_load_workflow_name = get_wrapping_workflow_name(args.repository, args.branch_name, suffix_mars_sp_load)
else:    
    mars_sp_load_workflow_full_suffix = args.repository+suffix_mars_sp_load
    mars_sp_load_workflow_name = get_wrapping_workflow_name(mars_sp_load_workflow_full_suffix)

mars_sp_load_workflow_conf_str = mars_sp_load_workflow_conf_template.render(
    mars_sp_load_workflow_name=mars_sp_load_workflow_name,
    unique_repo_branch_id=unique_repo_branch_id_mars_sp_load,  # this will not be taken into account in tst/pre/prd
    emails=args.emails,
    timeout=args.timeout,
    repos_path=repos_path,
    env=args.env,
    host=args.host,
    cluster_id=cluster_id,
)

mars_sp_load_workflow_conf = json.loads(mars_sp_load_workflow_conf_str)
create_or_update_workflow_by_name(mars_sp_load_workflow_name, mars_sp_load_workflow_conf, args.host, http_header)

# create or update mars security wrapping workflow
mars_security_workflow_conf_template = pipeline_templates.get_template(mars_security_workflow_conf_filename)

if args.integration_test == "true":
    mars_security_workflow_name = get_wrapping_workflow_name(args.repository, args.branch_name, suffix_mars_security)
else:    
    mars_security_workflow_full_suffix = args.repository+suffix_mars_security
    mars_security_workflow_name = get_wrapping_workflow_name(mars_security_workflow_full_suffix)

mars_security_workflow_conf_str = mars_security_workflow_conf_template.render(
    mars_security_workflow_name=mars_security_workflow_name,
    unique_repo_branch_id=unique_repo_branch_id_mars_security,  # this will not be taken into account in tst/pre/prd
    emails=args.emails,
    timeout=args.timeout,
    repos_path=repos_path,
    env=args.env,
    host=args.host,
    cluster_id=cluster_id,
)

mars_security_workflow_conf = json.loads(mars_security_workflow_conf_str)
create_or_update_workflow_by_name(mars_security_workflow_name, mars_security_workflow_conf, args.host, http_header)