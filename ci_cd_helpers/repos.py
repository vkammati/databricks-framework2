import requests
from requests.exceptions import HTTPError
from typing import Optional

def get_repos(dbx_repos_path: str, host: str, http_header: dict, next_page_token=None) -> dict:
    """Get Repos' metadata that match the specified path prefix.
    :param dbx_repos_path: Repos path in the Databricks workspace of the form
    "/Repos/path/to/project"
    :param host: Databricks host
    :param http_header: HTTP header used for the Databricks REST API
    :param next_page_token: next page token to use for pagination
    :return: matching Repos response JSON
    :raises HTTPError: when any issue happens while calling Databricks' REST API
    """
    query_parameters = f"path_prefix={dbx_repos_path}"

    if next_page_token:
        query_parameters = f"{query_parameters}&next_page_token={next_page_token}"

    req = requests.get(
        f"{host}/api/2.0/repos?{query_parameters}",
        headers=http_header,
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e

    return req.json()


def get_repos_id(dbx_repos_path: str, host: str, http_header: dict) -> Optional[str]:
    """Get Repos id if Repos already exists.
    :param dbx_repos_path: Repos path in the Databricks workspace of the form
    "/Repos/path/to/project"
    :param host: Databricks host
    :param http_header: HTTP header used for the Databricks REST API
    :return: repos id if repos exists, None otherwise
    """
    dbx_repos_id = None
    potential_repos = get_repos(dbx_repos_path, host, http_header)

    while "repos" in potential_repos:
        dbx_repos_id = find_repos_id(potential_repos["repos"], dbx_repos_path)
        is_last_batch = "next_page_token" not in potential_repos

        if dbx_repos_id or is_last_batch:
            break

        print("No matching Repos found in this batch, checking the next batch...")
        next_page_token = potential_repos["next_page_token"]
        potential_repos = get_repos(dbx_repos_path, host, http_header, next_page_token)

    return dbx_repos_id


def find_repos_id(potential_repos: list, dbx_repos_path: str) -> Optional[str]:
    """Find matching Repos' id.
    :param potential_repos: potential matching repos' metadata
    :param dbx_repos_path: Repos path in the Databricks workspace of the form
    "/Repos/path/to/project"
    :return: repos id if repos exists, None otherwise
    """
    dbx_repos_id = None

    if potential_repos:
        # Some Repos might match the repos prefix, but there is only one Repos that
        # has the exact same path, the one we want to update.
        actual_repos = [
            repos["id"]
            for repos in potential_repos
            if repos["path"] == dbx_repos_path
        ]

        if actual_repos:
            dbx_repos_id = actual_repos[0]

    return dbx_repos_id


def create_repos(
        repos_path: str,
        github_repos_url: str,
        host: str,
        http_header: dict) -> str:
    """Create Databricks Repos.
    :param repos_path: path in Databricks where the Repos must be created
    :param github_repos_url: Project's repository url in GitHub
    :param host: Databricks host
    :param http_header: HTTP header used for the Databricks REST API
    :return: the Repos id
    """
    req = requests.post(
        f"{host}/api/2.0/repos",
        headers=http_header,
        json={
            "path": repos_path,
            "provider": "gitHub",
            "url": github_repos_url,
        }
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e

    repos_id = req.json()["id"]
    return repos_id


def update_repos(host: str, http_header: dict, repos_id: str, branch_name: str):
    """Update Repos to the last commit of the specified branch.
    :param host: Databricks host
    :param http_header: HTTP header used for the Databricks REST API
    :param repos_id: Databricks Repos id
    :param branch_name: name of the branch to use when updating the Repos
    """
    req = requests.patch(
        f"{host}/api/2.0/repos/{repos_id}",
        headers=http_header,
        json={
            "branch": branch_name,
        }
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e


def delete_repos(repos_id: str, host: str, http_header: dict):
    """Delete a Databricks Repos.
    
    :param repos_id: Databricks repo id
    :param host: Databricks host
    :param http_header: HTTP header used for the Databricks REST API
    """
    req = requests.delete(
        f"{host}/api/2.0/repos/{repos_id}",
        headers=http_header,
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e


def delete_repos_by_path(repos_path: str, host: str, http_header: dict):
    """Delete a Databricks Repos based on its path.
    
    :param repos_path: Databricks Repos path
    :param host: Databricks host
    :param http_header: HTTP header used for the Databricks REST API
    """
    repos_id = get_repos_id(repos_path, host, http_header)

    if repos_id:
        print(f"Found Repos {repos_path} with id {repos_id}, deleting it...")
        delete_repos(repos_id, host, http_header)
        print(f"Repos {repos_path} with id {repos_id} deleted.")
    else:
        print(f"No Repos found with path {repos_path}.")


def create_or_update_repos_by_path(
    repos_path: str,
    github_repos_url: str,
    branch_name: str,
    host: str,
    http_header: dict) -> str:
    """Update a Repos if it already exists, create it otherwise.
        
    :param repos_path: Repos path
    :param github_repos_url: Project's repository url in GitHub
    :param branch_name: name of the branch to use when updating the Repos
    :param host: Databricks host
    :param http_header: HTTP header used for the Databricks REST API
    :return: the Repos id
    """
    repos_id = get_repos_id(repos_path, host, http_header)
    
    if not repos_id:
        print(f"No Repos {repos_path}. Creating one...")
        # we can't set the branch at Repos creation time, default branch is main
        repos_id = create_repos(repos_path, github_repos_url, host, http_header)
        print(f"Repos {repos_path} created with id {repos_id}.")
        
    print(f"Found Repos {repos_path} with id {repos_id}, updating it...")
    update_repos(host, http_header, repos_id, branch_name)
    print(f"Repos {repos_path} with id {repos_id} updated.")        
    
    return repos_id


def set_repos_open_permissions(repos_id: str, host: str, http_header: dict):
    """Set open permissions for a Repos folder.
    
    NB: all users will be able to read the folder.
    
    :param repos_id: Repos folder id
    :param host: Databricks host
    :param http_header: HTTP header used for the Databricks REST API
    """
    req = requests.patch(
        f"{host}/api/2.0/permissions/repos/{repos_id}",
        headers=http_header,
        json={
            "access_control_list": [
                {
                    "group_name": "users",
                    "permission_level": "CAN_READ"
                }
            ]
        }
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e
