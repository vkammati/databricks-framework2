import requests
from requests.exceptions import HTTPError


def create_folder_if_not_exists(folder_path: str, host: str, http_header: dict):
    """Create folder in Databricks' workspace if it doesn't exist.
    NB: this step is mandatory before creating a Repos under that specific
    folder. If we don't do it, an error will be raised.
    :param folder_path: folder path to create in Workspace (also works for Repos)
    :param host: Databricks host
    :param http_header: HTTP header used for the Databricks REST API
    """
    req = requests.post(
        f"{host}/api/2.0/workspace/mkdirs",
        headers=http_header,
        json={
            "path": folder_path,
        }
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e

        
def get_folder_id(folder_path: str, host: str, http_header: dict) -> str:
    """Get Workspace's folder id.
    
    :param folder_path: Workspace's folder path
    :param host: Databricks host
    :param http_header: HTTP header used for the Databricks REST API
    """
    req = requests.get(
        f"{host}/api/2.0/workspace/get-status",
        headers=http_header,
        json={
            "path": folder_path
        }
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e

    return req.json()["object_id"]


def set_folder_open_permissions(folder_id: str, host: str, http_header: dict):
    """Set open permissions for a Workspace's folder.
    
    NB: all users will be able to read the folder.
    
    :param folder_id: Workspace's folder id
    :param host: Databricks host
    :param http_header: HTTP header used for the Databricks REST API
    """
    req = requests.patch(
        f"{host}/api/2.0/permissions/directories/{folder_id}",
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
