"""
This script checks that the passed branch is either a release branch or a hotfix branch.
"""
import argparse
import sys


parser = argparse.ArgumentParser(description="Deploy code as a Databricks Repos.")

parser.add_argument(
    "--branch-name",
    help="Name of the branch in GitHub",
    type=str,
)

args = parser.parse_args()
branch = args.branch_name

if branch.startswith("release/v"):
    print(f"Branch {branch} is a release branch, test passed.")
    sys.exit(0)
elif branch.startswith("hotfix/v"):
    print(f"Branch {branch} is a hotfix branch, test passed.")
    sys.exit(0)
else:
    print(f"Branch {branch} is should be either a release branch or a hotfix branch, test failed.")
    sys.exit(1)
