"""
This script runs the integrations tests (a Databricks Workflow).
"""
import argparse
from jinja2 import Environment, FileSystemLoader
import json
import os
import sys

from ci_cd_helpers.auth import get_dbx_http_header
from ci_cd_helpers.azure import generate_spn_ad_token
from ci_cd_helpers.helpers import get_wrapping_workflow_name
from ci_cd_helpers.workflows import get_job_id, poll_run, run_now


parser = argparse.ArgumentParser(description="Run integration tests.")

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
    "--deployment-type",
    help="Deployment type, either 'manual' or 'automatic'.",
    choices=["manual", "automatic"],
    type=str,
    required=True,
)

args = parser.parse_args()

# necessary to call the Databricks REST API
ad_token = generate_spn_ad_token(args.tenant_id, args.spn_client_id, args.spn_client_secret)
http_header = get_dbx_http_header(ad_token)

# get workflow id
workflow_name = get_wrapping_workflow_name(args.repository, args.branch_name, args.deployment_type)
workflow_id = get_job_id(workflow_name, args.host, http_header)
if not workflow_id:
    raise Exception("Integration tests workflow doesn't exist.")

# trigger run for integration tests workflow
run_id = run_now(workflow_id, args.host, http_header)
print(f"New job run triggered with id {run_id}.")

# poll run
run_status = poll_run(run_id, args.host, http_header)

if run_status == "SUCCESS":
    sys.exit(0)
else:
    sys.exit(1)
