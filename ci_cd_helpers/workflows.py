from datetime import datetime
import requests
from requests.exceptions import HTTPError
import time
from typing import Optional


def run_now(job_id: str, host: str, http_header: dict) -> str:
    """Trigger job run.
    
    :param job_id: Databricks job id
    :param host: Databricks host
    :param http_header: HTTP header used for the Databricks REST API
    :return: the run id
    """
    req = requests.post(
        f"{host}/api/2.1/jobs/run-now",
        headers=http_header,
        json={
            "job_id": job_id,
        }
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e

    return req.json()["run_id"]


def create_job(conf: dict, host: str, http_header: dict) -> str:
    """Create a Databricks job.
    
    :param conf: Workflow configuration
    :param host: Databricks host
    :param http_header: HTTP header used for the Databricks REST API
    :return: job id
    """
    req = requests.post(
        f"{host}/api/2.1/jobs/create",
        headers=http_header,
        json=conf,
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e

    return req.json()["job_id"]


def update_job(job_id: str, conf: dict, host: str, http_header: dict):
    """Update a Databricks job.
    
    :param job_id: Databricks job id
    :param conf: Workflow configuration
    :param host: Databricks host
    :param http_header: HTTP header used for the Databricks REST API
    """
    req = requests.post(
        f"{host}/api/2.1/jobs/reset",
        headers=http_header,
        json={
            "job_id": job_id,
            "new_settings": conf,
        }
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e


# TODO: use pagination?
def get_job_id(job_name: str, host: str, http_header: dict) -> Optional[str]:
    """Get job id if it exists, None otherwise.
    
    :param job_name: Databricks job name
    :param host: Databricks host
    :param http_header: HTTP header used for the Databricks REST API
    :return: job id if it exists, None otherwise
    """
    job_id = None
    limit = 1
    expand_tasks = "false"

    query_string = f"name={job_name}&limit={limit}&expand_tasks={expand_tasks}"

    req = requests.get(
        f"{host}/api/2.1/jobs/list?{query_string}",
        headers=http_header,
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e

    jobs_key = "jobs"

    if jobs_key in req.json():
        job_id = req.json()[jobs_key][0]["job_id"]

    return job_id


def poll_run(run_id: str, host: str, http_header: dict, poll_frequency: int = 15) -> str:
    """Poll run.
    
    NB: make sure there is a timeout set at the workflow level or else this
    function could run forever.
    
    :param run_id: Run id
    :param host: Databricks host
    :param http_header: HTTP header used for the Databricks REST API
    :param poll_frequency: frequency in seconds at which to poll the run.
    Default is 15 seconds.
    :return: the run status: if "SUCCESS" then this is success, everything else
    is a failure.
    """
    while True:
        now = datetime.now().strftime("%H:%M:%S")
        print(f"Polling run {run_id} at {now}..")

        req = requests.get(
            f"{host}/api/2.1/jobs/runs/get",
            headers=http_header,
            json={
                "run_id": run_id,
            },
        )

        state = req.json()["state"]

        result_state_key = "result_state"

        is_run_finished = result_state_key in state
        if is_run_finished:
            run_status = state[result_state_key]
            print(f"Run polled at {now} and finished with status {run_status}")
            return run_status

        time.sleep(poll_frequency)


def poll_active_runs(workflow_id: str, host: str, http_header: dict, poll_frequency: int = 30):
    """Poll active runs for a workflow until all of them are finished.
    
    :param job_id: Workflow id
    :param host: Databricks host
    :param http_header: HTTP header used for the Databricks REST API
    :param poll_frequency: frequency in seconds at which to poll the workflow.
    Default is 30 seconds.
    """
    query_parameters = f"job_id={workflow_id}&active_only=true&limit=10"

    while True:
        req = requests.get(
            f"{host}/api/2.1/jobs/runs/list?{query_parameters}",
            headers=http_header,
        )

        try:
            req.raise_for_status()
        except HTTPError as e:
            print(e.response.text)
            raise e
        
        state = req.json()
        are_runs_finished = "runs" not in state
        
        if are_runs_finished:
            print(f"All runs for workflow {workflow_id} have finished.")
            break
        else:
            num_runs = len(state["runs"])
            print(f"At least {num_runs} runs for workflow {workflow_id} are still running, sleeping {poll_frequency} seconds...")
            time.sleep(poll_frequency)

            
def delete_job(job_id: str, host: str, http_header: dict):
    """Delete a Databricks workflow.
    
    :param job_id: Databricks workflow id
    :param host: Databricks host
    :param http_header: HTTP header used for the Databricks REST API
    """
    req = requests.post(
        f"{host}/api/2.1/jobs/delete",
        headers=http_header,
        json={
            "job_id": job_id,
        }
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e


def delete_workflow_by_name(name: str, host: str, http_header: dict):
    """Delete a Databricks workflow by its name.
    
    NB: we assume a 1-1 relationship between a workflow id and its name.
    
    :param name: Databricks workflow name
    :param host: Databricks host
    :param http_header: HTTP header used for the Databricks REST API
    """
    workflow_id = get_job_id(name, host, http_header)

    if workflow_id:
        print(f"Found workflow {name} with id {workflow_id}, deleting it...")
        delete_job(workflow_id, host, http_header)
        print(f"Workflow {name} with id {workflow_id} deleted.")
    else:
        print(f"No workflow found with name {name}.")


def create_or_update_workflow_by_name(
    name: str,
    workflow_conf: dict,
    host: str,
    http_header: dict) -> str:
    """Update a workflow if it already exists, create it otherwise.
    
    We assume a 1-1 relationship between a workflow id and its name.
    
    :param name: workflow name
    :param workflow_conf: workflow configuration
    :param host: Databricks host
    :param http_header: HTTP header used for the Databricks REST API
    :return: the workflow id
    """
    workflow_id = get_job_id(name, host, http_header)
    
    if workflow_id:
        print(f"Found workflow {name} with id {workflow_id}, updating it...")
        update_job(workflow_id, workflow_conf, host, http_header)
        print(f"Workflow {name} with id {workflow_id} updated.")
    else:
        print(f"No workflow {name}. Creating one...")
        workflow_id = create_job(workflow_conf, host, http_header)
        print(f"Workflow {name} created with id {workflow_id}.")
    
    return workflow_id


def set_workflow_open_permissions(workflow_id: str, host: str, http_header: dict, env: str):
    """Set open permissions for a Databricks workflow.
    
    NB: all users will be able to Manage Run the DLT workflow, i.e. view the workflow,
    run it, and cancel it. See
    https://docs.databricks.com/security/access-control/jobs-acl.html#job-permissions
    for more info.
    
    :param workflow_id: Workflow id
    :param host: Databricks host
    :param http_header: HTTP header used for the Databricks REST API
    """
    # adding permissions to AD_GROUPS
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
        f"{host}/api/2.0/permissions/jobs/{workflow_id}",
        headers=http_header,
        json={
            "access_control_list": [
                {
                "group_name":it_group_name,
                "permission_level":it_wf_permission_level
                },
                {
                "group_name":reviewer_group_name,
                "permission_level":reviewer_wf_permission_level
                },
                {
                "group_name":som_functional_group_name,
                "permission_level":som_functional_wf_permission_level
                },
                {
                "group_name":lead_group_name,
                "permission_level":lead_wf_permission_level
                }
            ]
        }
    )
    
    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e


def trigger_one_time_run(workflow_conf: dict, host: str, http_header: dict) -> str:
    """Trigger a one-time job run.
    
    :param workflow_conf: Workflow configuration
    :param host: Databricks host
    :param http_header: HTTP header used for the Databricks REST API
    :return: the run id
    """
    req = requests.post(
        f"{host}/api/2.1/jobs/runs/submit",
        headers=http_header,
        json=workflow_conf,
    )

    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e

    return req.json()["run_id"]
