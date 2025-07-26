"""
This script deploys code, i.e. a branch, as a Databricks Repos.
"""
import argparse

from ci_cd_helpers.auth import get_dbx_http_header
from ci_cd_helpers.azure import generate_spn_ad_token
from ci_cd_helpers.helpers import get_github_repos_url, get_repos_parent_folder_path, get_repos_path
from ci_cd_helpers.repos import create_or_update_repos_by_path
from ci_cd_helpers.workspace import create_folder_if_not_exists


parser = argparse.ArgumentParser(description="Deploy code as a Databricks Repos.")

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

if args.env == "dev" and not args.deployment_type:
    raise ValueError("When env=dev, parameter 'type' must be either 'manual' or 'automatic', not None.")

# necessary to call the Databricks REST API
ad_token = generate_spn_ad_token(args.tenant_id, args.spn_client_id, args.spn_client_secret)
http_header = get_dbx_http_header(ad_token)

# we need to create the parent folder beforehand to avoid an error to be thrown 
repos_parent_folder_path = get_repos_parent_folder_path(args.repository)
create_folder_if_not_exists(repos_parent_folder_path, args.host, http_header)

# create/update repos
#repos_path = get_repos_path(args.repository, args.branch_name, args.deployment_type)
if args.integration_test == "true":
    repos_path = get_repos_path(args.repository, args.branch_name, args.deployment_type)
else:
    repos_path = repos_parent_folder_path + "/datta_fcb_curated"
github_repos_url = get_github_repos_url(args.repository).replace("sede-x", "sede-enterprise") 
create_or_update_repos_by_path(repos_path, github_repos_url, args.branch_name, args.host, http_header)
