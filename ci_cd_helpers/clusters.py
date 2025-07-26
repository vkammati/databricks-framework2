import requests
from requests.exceptions import HTTPError
from typing import Optional


def create_cluster(conf: dict, host: str, http_header: dict) -> str:
    """Create cluster.
    
    :param conf: cluster configuration
    :param host: Databricks host
    :param http_header: HTTP header used for the Databricks REST API
    :return: cluster id
    """
    req = requests.post(
        f"{host}/api/2.0/clusters/create",
        headers=http_header,
        json=conf,
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e

    return req.json()["cluster_id"]


def get_cluster_id(cluster_name: str, host: str, http_header: dict) -> Optional[str]:
    """Get cluster id if cluster exists, None otherwise.
    
    :param cluster_name: name of the cluster
    :param host: Databricks host
    :param http_header: HTTP header used for the Databricks REST API
    :return: cluster id if it exists, None otherwise
    """
    cluster_id = None

    req = requests.get(
        f"{host}/api/2.0/clusters/list",
        headers=http_header,
        json={
            "cluster_id": cluster_id,
        }
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e

    clusters_key = "clusters"

    if clusters_key in req.json():
        for cluster in req.json()[clusters_key]:
            if cluster["cluster_name"] == cluster_name:
                cluster_id = cluster["cluster_id"]
                break

    return cluster_id


def update_cluster(cluster_id: str, conf: dict, host: str, http_header: dict):
    """Update existing cluster.
    
    :param cluster_id: cluster id
    :param conf: cluster configuration
    :param host: Databricks host
    :param http_header: HTTP header used for the Databricks REST API
    """
    conf["cluster_id"] = cluster_id

    req = requests.post(
        f"{host}/api/2.0/clusters/edit",
        headers=http_header,
        json=conf,
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e

        
def delete_cluster(cluster_id: str, host: str, http_header: dict):
    """Delete a Databricks cluster.
    
    :param cluster_id: cluster id
    :param host: Databricks host
    :param http_header: HTTP header used for the Databricks REST API
    """
    req = requests.post(
        f"{host}/api/2.0/clusters/permanent-delete",
        headers=http_header,
        json={
            "cluster_id": cluster_id,
        }
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e

        
def delete_cluster_by_name(name: str, host: str, http_header: dict):
    """Delete a cluster based on its name.
    
    We assume a 1-1 relationship between a cluster id and its name.
    
    :param name: cluster name
    :param host: Databricks host
    :param http_header: HTTP header used for the Databricks REST API
    """
    cluster_id = get_cluster_id(name, host, http_header)

    if cluster_id:
        print(f"Found cluster {name} with id {cluster_id}, deleting it...")
        delete_cluster(cluster_id, host, http_header)
        print(f"Cluster {name} with id {cluster_id} deleted.")
    else:
        print(f"No cluster found with name {name}.")


def create_or_update_cluster_by_name(
    name: str,
    cluster_conf: dict,
    host: str,
    http_header: dict) -> str:
    """Update a cluster if it already exists, create it otherwise.
    
    We assume a 1-1 relationship between a cluster id and its name.
    
    :param name: cluster name
    :param cluster_conf: cluster configuration
    :param host: Databricks host
    :param http_header: HTTP header used for the Databricks REST API
    :return: the cluster id
    """
    cluster_id = get_cluster_id(name, host, http_header)
    
    if cluster_id:
        print(f"Found cluster {name} with id {cluster_id}, updating it...")
        update_cluster(cluster_id, cluster_conf, host, http_header)
        print(f"Cluster {name} with id {cluster_id} updated.")
    else:
        print(f"No cluster {name}. Creating one...")
        cluster_id = create_cluster(cluster_conf, host, http_header)
        print(f"Cluster {name} created with id {cluster_id}.")
    
    return cluster_id


def set_cluster_open_permissions(cluster_id: str, host: str, http_header: dict):
    """Set open permissions for a Databricks cluster.
    
    NB: all users will be able to restart the cluster.
    
    :param cluster_id: cluster id
    :param host: Databricks host
    :param http_header: HTTP header used for the Databricks REST API
    """
    req = requests.patch(
        f"{host}/api/2.0/permissions/clusters/{cluster_id}",
        headers=http_header,
        json={
            "access_control_list": [
                {
                    "group_name": "users",
                    "permission_level": "CAN_RESTART"
                }
            ]
        }
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e
