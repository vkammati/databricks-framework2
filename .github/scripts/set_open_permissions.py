"""
A service principal is used to deploy in dev Databricks entities necessary for the pipeline to run properly.
As a result, only the service principal (and the workspace admins) can access those entities.
In dev, we want the developer to have access to them and interact with them (trigger, update, etc.)

This script sets open permissions for all those entities:
- the Repos
- the DLT pipeline itself
- the workflow that wraps the DLT pipeline
- the workflow that registers tables to Unity Catalog
- the cluster used to register tables to Unity Catalog
"""

import argparse

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
from ci_cd_helpers.repos import get_repos_id, set_repos_open_permissions
from ci_cd_helpers.clusters import get_cluster_id, set_cluster_open_permissions
from ci_cd_helpers.pipelines import get_pipeline_id, set_pipeline_open_permissions
from ci_cd_helpers.workflows import get_job_id, set_workflow_open_permissions
from ci_cd_helpers.workspace import get_folder_id, set_folder_open_permissions

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

'''if args.env == "dev" and not args.deployment_type:
    raise ValueError("When env=dev, parameter 'type' must be either 'manual' or 'automatic', not None.")'''

# suffix for DLT, worflows
if args.integration_test == "true":
    suffix_curated_weekly_full_load = "curated-weekly-full-load-" +args.deployment_type
    suffix_eh_main = "eh-main-" +args.deployment_type
    suffix_euh_main = "euh-main-" +args.deployment_type
    suffix_optimize_vacuum = "optimize-vacuum-" +args.deployment_type
    suffix_security = "security-" +args.deployment_type
    suffix_mars_load = "mars-load-" +args.deployment_type
    suffix_mars_sp_load = "mars-sp-load-" +args.deployment_type
    suffix_mars_security = "mars-security-" +args.deployment_type
else:
    suffix_curated_weekly_full_load = "-curated-weekly-full-load" 
    suffix_eh_main = "-eh-main" 
    suffix_euh_main = "-euh-main" 
    suffix_optimize_vacuum = "-optimize-vacuum" 
    suffix_security = "-security"
    suffix_mars_load = "-mars-load"
    suffix_mars_sp_load = "-mars-sp-load"
    suffix_mars_security = "-mars-security"

# necessary to call the Databricks REST API
ad_token = generate_spn_ad_token(args.tenant_id, args.spn_client_id, args.spn_client_secret)
http_header = get_dbx_http_header(ad_token)
env = args.env

# Repos parent folder
repos_parent_folder_path = get_repos_parent_folder_path(args.repository)
repos_parent_folder_id = get_folder_id(repos_parent_folder_path, args.host, http_header)
set_folder_open_permissions(repos_parent_folder_id, args.host, http_header)

# Repos folder
if args.integration_test == "true":
    repos_path = get_repos_path(args.repository, args.branch_name, args.deployment_type)
else:    
    repos_path = repos_parent_folder_path + "/datta_fcb_curated"
repos_id = get_repos_id(repos_path, args.host, http_header)
set_repos_open_permissions(repos_id, args.host, http_header)

# DLT pipeline
# if args.integration_test == "true":
#     pipeline_name = get_dlt_pipeline_name(args.repository, args.branch_name, args.deployment_type)
# else:    
#     #pipeline_name = "sede-x-datta-fcb-dlt"
#     pipeline_name = get_dlt_pipeline_name(args.repository)
# pipeline_id = get_pipeline_id(pipeline_name, args.host, http_header)
# set_pipeline_open_permissions(pipeline_id, args.host, http_header)

# Wrapping workflow
if args.integration_test == "true":
    workflow_name = get_wrapping_workflow_name(args.repository, args.branch_name, args.deployment_type)
else:
    #workflow_name = "sede-x-datta-fcb-workflow"
    workflow_name = get_wrapping_workflow_name(args.repository)
workflow_id = get_job_id(workflow_name, args.host, http_header)
set_workflow_open_permissions(workflow_id, args.host, http_header, env)

# Security wrapping workflow
if args.integration_test == "true":
    security_workflow_name = get_wrapping_workflow_name(args.repository, args.branch_name, suffix_security)
else:
    security_workflow_full_suffix = args.repository+suffix_security
    security_workflow_name = get_wrapping_workflow_name(security_workflow_full_suffix)
security_workflow_id = get_job_id(security_workflow_name, args.host, http_header)
set_workflow_open_permissions(security_workflow_id, args.host, http_header, env)

# Optimize vacuum wrapping workflow
if args.integration_test == "true":
    optimize_vacuum_workflow_name = get_wrapping_workflow_name(args.repository, args.branch_name, suffix_optimize_vacuum)
else:
    optimize_vacuum_workflow_full_suffix = args.repository+suffix_optimize_vacuum
    optimize_vacuum_workflow_name = get_wrapping_workflow_name(optimize_vacuum_workflow_full_suffix)
optimize_vacuum_workflow_id = get_job_id(optimize_vacuum_workflow_name, args.host, http_header)
set_workflow_open_permissions(optimize_vacuum_workflow_id, args.host, http_header, env)

# EUH main wrapping workflow
if args.integration_test == "true":
    euh_main_workflow_name = get_wrapping_workflow_name(args.repository, args.branch_name, suffix_euh_main)
else:
    euh_main_workflow_full_suffix = args.repository+suffix_euh_main
    euh_main_workflow_name = get_wrapping_workflow_name(euh_main_workflow_full_suffix)
euh_main_workflow_id = get_job_id(euh_main_workflow_name, args.host, http_header)
set_workflow_open_permissions(euh_main_workflow_id, args.host, http_header, env)

# EH main wrapping workflow
if args.integration_test == "true":
    eh_main_workflow_name = get_wrapping_workflow_name(args.repository, args.branch_name, suffix_eh_main)
else:
    eh_main_workflow_full_suffix = args.repository+suffix_eh_main
    eh_main_workflow_name = get_wrapping_workflow_name(eh_main_workflow_full_suffix)
eh_main_workflow_id = get_job_id(eh_main_workflow_name, args.host, http_header)
set_workflow_open_permissions(eh_main_workflow_id, args.host, http_header, env)

# Curated weekly full load wrapping workflow
if args.integration_test == "true":
    curated_weekly_full_load_workflow_name = get_wrapping_workflow_name(args.repository, args.branch_name, suffix_curated_weekly_full_load)
else:
    curated_weekly_full_load_workflow_full_suffix = args.repository+suffix_curated_weekly_full_load
    curated_weekly_full_load_workflow_name = get_wrapping_workflow_name(curated_weekly_full_load_workflow_full_suffix)
curated_weekly_full_load_workflow_id = get_job_id(curated_weekly_full_load_workflow_name, args.host, http_header)
set_workflow_open_permissions(curated_weekly_full_load_workflow_id, args.host, http_header, env)

# mars-load wrapping workflow
if args.integration_test == "true":
    mars_load_workflow_name = get_wrapping_workflow_name(args.repository, args.branch_name, suffix_mars_load)
else:
    mars_load_workflow_full_suffix = args.repository+suffix_mars_load
    mars_load_workflow_name = get_wrapping_workflow_name(mars_load_workflow_full_suffix)
mars_load_workflow_id = get_job_id(mars_load_workflow_name, args.host, http_header)
set_workflow_open_permissions(mars_load_workflow_id, args.host, http_header, env)

# mars-sp-load wrapping workflow
if args.integration_test == "true":
    mars_sp_load_workflow_name = get_wrapping_workflow_name(args.repository, args.branch_name, suffix_mars_sp_load)
else:
    mars_sp_load_workflow_full_suffix = args.repository+suffix_mars_sp_load
    mars_sp_load_workflow_name = get_wrapping_workflow_name(mars_sp_load_workflow_full_suffix)
mars_sp_load_workflow_id = get_job_id(mars_sp_load_workflow_name, args.host, http_header)
set_workflow_open_permissions(mars_sp_load_workflow_id, args.host, http_header, env)

# MARS security wrapping workflow
if args.integration_test == "true":
    mars_security_workflow_name = get_wrapping_workflow_name(args.repository, args.branch_name, suffix_mars_security)
else:
    mars_security_workflow_full_suffix = args.repository+suffix_mars_security
    mars_security_workflow_name = get_wrapping_workflow_name(mars_security_workflow_full_suffix)
mars_security_workflow_id = get_job_id(mars_security_workflow_name, args.host, http_header)
set_workflow_open_permissions(mars_security_workflow_id, args.host, http_header, env)

# UC workflow
# if args.integration_test == "true":
#     register_table_uc_name = get_register_table_uc_workflow_name(args.repository, args.branch_name, args.deployment_type)
# else:
#     #register_table_uc_name = "sede-x-datta-fcb-uc"
#     register_table_uc_name = get_register_table_uc_workflow_name(args.repository)
# register_table_uc_id = get_job_id(register_table_uc_name, args.host, http_header)
# set_workflow_open_permissions(register_table_uc_id, args.host, http_header)

# cluster to register tables to UC
if args.integration_test == "true":
    cluster_name = get_cluster_name(args.repository, args.branch_name, args.deployment_type)
    cluster_id = get_cluster_id(cluster_name, args.host, http_header)
    set_cluster_open_permissions(cluster_id, args.host, http_header)
# else:
#     #cluster_name = "sede-x-datta-fcb-cluster"
#     cluster_name = get_cluster_name(args.repository)
# cluster_id = get_cluster_id(cluster_name, args.host, http_header)
# set_cluster_open_permissions(cluster_id, args.host, http_header)
