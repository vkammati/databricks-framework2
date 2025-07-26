import argparse

from ci_cd_helpers.github import (
    check_if_is_hotfix_branch,
    check_if_is_release_branch,
    create_tag,
    get_tag_version,
)


parser = argparse.ArgumentParser(description="Create production tag")

parser.add_argument("--repo", help="GitHub full repository name (including the owner)", type=str)
parser.add_argument("--gh-token", help="GitHub token to call the REST API", type=str)
parser.add_argument("--sha", help="Commit sha used to create the branch", type=str)
parser.add_argument(
    "--head-branch",
    help="Name of the head branch to use, must be of the form 'release/v*' or 'hotfix/v*'.",
    type=str,
)

args = parser.parse_args()

is_release_branch = check_if_is_release_branch(args.head_branch)
is_hotfix_branch = check_if_is_hotfix_branch(args.head_branch)

if not (is_release_branch or is_hotfix_branch):
    raise ValueError(f"Head branch {args.head_branch} must be a release or a hotfix branch.")

tag_version = get_tag_version(args.head_branch)
create_tag(tag_version, args.sha, args.repo, args.gh_token)
