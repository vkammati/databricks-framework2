import requests
from requests.exceptions import HTTPError
from typing import Optional


def create_pipeline(conf: dict, host: str, http_header: dict) -> str:
    """Create DLT pipeline.
    :param conf: DLT pipeline configuration
    :param host: Databricks host
    :param http_header: HTTP header used for the Databricks REST API
    :return: the DLT pipeline id
    """
    req = requests.post(
        f"{host}/api/2.0/pipelines",
        headers=http_header,
        json=conf,
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e

    pipeline_id_key = "pipeline_id"

    if pipeline_id_key not in req.json():
        raise Exception(f"Key '{pipeline_id_key}' was not found in response json. Actual response: {req.json()}")

    return req.json()[pipeline_id_key]


def update_pipeline(
        conf: dict,
        pipeline_id: str,
        host: str,
        http_header: dict):
    """Update a DLT pipeline.
    :param conf: DLT pipeline configuration
    :param pipeline_id: DLT pipeline id
    :param host: Databricks host
    :param http_header: HTTP header used for the Databricks REST API
    """
    req = requests.put(
        f"{host}/api/2.0/pipelines/{pipeline_id}",
        headers=http_header,
        json=conf,
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e


# TODO: use pagination?
def get_pipeline_id(pipeline_name: str, host: str, http_header: dict) -> Optional[str]:
    """Get DLT pipeline's id if it exists, None otherwise.
    :param pipeline_name: DLT pipeline name
    :param host: Databricks host
    :param http_header: HTTP header used for the Databricks REST API
    :return: DLT pipeline id if it exists, None otherwise
    """
    pipeline_id = None
    pipeline_name_filter = f"filter=name%20LIKE%20%27%25{pipeline_name}%25%27&max_results=1"

    req = requests.get(
        f"{host}/api/2.0/pipelines?{pipeline_name_filter}",
        headers=http_header,
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e

    if "statuses" in req.json():
        pipeline_id = req.json()["statuses"][0]["pipeline_id"]

    return pipeline_id


def delete_pipeline(pipeline_id: str, host: str, http_header: dict):
    """Delete a DLT pipeline.
    
    :param pipeline_id: DLT pipeline id
    :param host: Databricks host
    :param http_header: HTTP header used for the Databricks REST API
    """
    req = requests.delete(
        f"{host}/api/2.0/pipelines/{pipeline_id}",
        headers=http_header,
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e


def delete_pipeline_by_name(name: str, host: str, http_header: dict):
    """Delete a DLT pipeline by its name.
    
    NB: we assume a 1-1 relationship between a pipeline id and its name.
    
    :param name: DLT pipeline name
    :param host: Databricks host
    :param http_header: HTTP header used for the Databricks REST API
    """
    pipeline_id = get_pipeline_id(name, host, http_header)

    if pipeline_id:
        print(f"Found DLT pipeline {name} with id {pipeline_id}, deleting it...")
        delete_pipeline(pipeline_id, host, http_header)
        print(f"DLT pipeline {name} with id {pipeline_id} deleted.")
    else:
        print(f"No DLT pipeline found with name {name}.")

        
def create_or_update_pipeline_by_name(
    name: str,
    pipeline_conf: dict,
    host: str,
    http_header: dict) -> str:
    """Update a DLT pipeline if it already exists, create it otherwise.
    
    We assume a 1-1 relationship between a DLT pipeline id and its name.
    
    :param name: workflow name
    :param pipeline_conf: DLT pipeline configuration
    :param host: Databricks host
    :param http_header: HTTP header used for the Databricks REST API
    :return: the workflow id
    """
    pipeline_id = get_pipeline_id(name, host, http_header)
    
    if pipeline_id:
        print(f"Found DLT pipeline {name} with id {pipeline_id}, updating it...")
        update_pipeline(pipeline_conf, pipeline_id, host, http_header)
        print(f"DLT pipeline {name} with id {pipeline_id} updated.")
    else:
        print(f"No DLT pipeline {name}. Creating one...")
        pipeline_id = create_pipeline(pipeline_conf, host, http_header)
        print(f"DLT pipeline {name} created with id {pipeline_id}.")
    
    return pipeline_id


def set_pipeline_open_permissions(pipeline_id: str, host: str, http_header: dict, env: str):
    """Set open permissions for a DLT pipeline.
    
    NB: all users will be able to view and run the DLT pipeline.
    
    :param pipeline_id: DLT pipeline id
    :param host: Databricks host
    :param http_header: HTTP header used for the Databricks REST API
    """

    env_upper=env.upper()
    if env_upper=="DEV":
        it_group_name="AZ-EDS-GRP-SG-N-DS-DEV-IT-DEVELOPER"
        it_wf_permission_level="CAN_MANAGE"
        it_dlt_permission_level="CAN_MANAGE"
        it_cluster_permission_level="CAN_RESTART"
        reviewer_group_name="AZ-EDS-GRP-SG-N-DS-DEV-IT-REVIEWER"
        reviewer_wf_permission_level="CAN_VIEW"
        reviewer_dlt_permission_level="CAN_VIEW"
        reviewer_cluster_permission_level="CAN_ATTACH_TO"
        som_functional_group_name="AZ-EDS-GRP-SG-N-DS-DEV-IT-SOM-FUNCTIONAL-RX"
        som_functional_wf_permission_level="CAN_VIEW"
        som_functional_dlt_permission_level="CAN_VIEW"
        som_functional_cluster_permission_level="CAN_ATTACH_TO"
        lead_group_name="AZ-EDS-GRP-SG-N-DS-DEV-IT-TECHLEAD"
        lead_wf_permission_level="CAN_MANAGE"
        lead_dlt_permission_level="CAN_MANAGE"
        lead_cluster_permission_level="CAN_MANAGE"
    

    elif env_upper=="TST":
        it_group_name="AZ-EDS-GRP-SG-N-DS-CAT-IT-DEVELOPER"
        it_wf_permission_level="CAN_MANAGE"
        it_dlt_permission_level="CAN_MANAGE"
        it_cluster_permission_level="CAN_RESTART"
        reviewer_group_name="AZ-EDS-GRP-SG-N-DS-CAT-IT-REVIEWER"
        reviewer_wf_permission_level="CAN_VIEW"
        reviewer_dlt_permission_level="CAN_VIEW"
        reviewer_cluster_permission_level="CAN_ATTACH_TO"
        lead_group_name="AZ-EDS-GRP-SG-N-DS-CAT-IT-TECHLEAD"
        lead_wf_permission_level="CAN_MANAGE"
        lead_dlt_permission_level="CAN_MANAGE"
        lead_cluster_permission_level="CAN_MANAGE"
        som_functional_group_name="AZ-EDS-GRP-SG-N-DS-CAT-IT-SOM-FUNCTIONAL-RX"
        som_functional_wf_permission_level="CAN_VIEW"
        som_functional_dlt_permission_level="CAN_VIEW"
        som_functional_cluster_permission_level="CAN_ATTACH_TO"

    elif env_upper=="PRE":
        it_group_name="AZ-EDS-GRP-SG-P-DS-PSU-IT-DEVELOPER-RX"
        it_wf_permission_level="CAN_VIEW"
        it_dlt_permission_level="CAN_VIEW"
        it_cluster_permission_level="CAN_ATTACH_TO"
        reviewer_group_name="AZ-EDS-GRP-SG-P-DS-PSU-IT-REVIEWER-RX"
        reviewer_wf_permission_level="CAN_VIEW"
        reviewer_dlt_permission_level="CAN_VIEW"
        reviewer_cluster_permission_level="CAN_ATTACH_TO"
        lead_group_name="AZ-EDS-GRP-SG-P-DS-PSU-IT-TECHLEAD"
        lead_wf_permission_level="CAN_VIEW"
        lead_dlt_permission_level="CAN_VIEW"
        lead_cluster_permission_level="CAN_MANAGE"
        som_functional_group_name="AZ-EDS-GRP-SG-P-DS-PSU-IT-SOM-FUNCTIONAL-RW"
        som_functional_wf_permission_level="CAN_MANAGE_RUN"
        som_functional_dlt_permission_level="CAN_VIEW"
        som_functional_cluster_permission_level="CAN_ATTACH_TO"

    elif env_upper=="PRD":
        it_group_name="AZ-EDS-GRP-SG-P-DS-PRD-IT-DEVELOPER-RX"
        it_wf_permission_level="CAN_VIEW"
        it_dlt_permission_level="CAN_VIEW"
        it_cluster_permission_level="CAN_ATTACH_TO"
        reviewer_group_name="AZ-EDS-GRP-SG-P-DS-PRD-IT-REVIEWER-RX"
        reviewer_wf_permission_level="CAN_VIEW"
        reviewer_dlt_permission_level="CAN_VIEW"
        reviewer_cluster_permission_level="CAN_ATTACH_TO"
        lead_group_name="AZ-EDS-GRP-SG-P-DS-PRD-IT-TECHLEAD"
        lead_wf_permission_level="CAN_VIEW"
        lead_dlt_permission_level="CAN_VIEW"
        lead_cluster_permission_level="CAN_MANAGE"
        som_functional_group_name="AZ-EDS-GRP-SG-P-DS-PRD-IT-SOM-FUNCTIONAL-RW"
        som_functional_wf_permission_level="CAN_MANAGE_RUN"
        som_functional_dlt_permission_level="CAN_VIEW"
        som_functional_cluster_permission_level="CAN_ATTACH_TO"

    else:
        it_group_name=""
        it_wf_permission_level=""
        it_dlt_permission_level=""
        it_cluster_permission_level=""
        reviewer_group_name=""
        reviewer_wf_permission_level=""
        reviewer_dlt_permission_level=""
        reviewer_cluster_permission_level=""
        lead_group_name=""
        lead_wf_permission_level=""
        lead_dlt_permission_level=""
        lead_cluster_permission_level=""
        som_functional_group_name=""
        som_functional_wf_permission_level=""
        som_functional_dlt_permission_level=""
        som_functional_cluster_permission_level=""

    req = requests.patch(
        f"{host}/api/2.0/permissions/pipelines/{pipeline_id}",
        headers=http_header,
        json={
            "access_control_list": [
                {
                "group_name":it_group_name,
                "permission_level":it_dlt_permission_level
                },
                {
                "group_name":reviewer_group_name,
                "permission_level":reviewer_dlt_permission_level
                },
                {
                "group_name":som_functional_group_name,
                "permission_level":som_functional_dlt_permission_level
                },
                {
                "group_name":lead_group_name,
                "permission_level":lead_dlt_permission_level
                }
            ]
        }
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e
