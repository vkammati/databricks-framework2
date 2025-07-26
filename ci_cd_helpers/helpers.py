def format_repository_name(repo: str) -> str:
    """Format GitHub repository name."""
    return repo.replace('/', '-')


def format_branch_name(branch: str) -> str:
    """Format GitHub branch name."""
    return branch.replace('/', '-')


def get_repos_parent_folder_path(repo: str) -> str:
    """Get parent folder path in Databricks Repos.
    
    The parent folder path in Databricks Repos needs to exist before
    adding a repo, which means that we need to create it beforehand.
    
    Example: for the reference pipeline, parameter repo will be equal to
    "sede-x/enterprise-data-platform-reference-pipeline". The Repos parent
    folder path will be "/Repos/sede-x-enterprise-data-platform-reference-pipeline".
    
    NB: at the moment, there can be only one nested folder, which is the reason why
    we replace slashes by dashes in the repository name.
    
    :param repo: GitHub repository name
    :return: parent folder path in Databricks Repos
    """
    formatted_repository_name = format_repository_name(repo)
    return f"/Repos/{formatted_repository_name}"


def get_repos_path(repo: str, branch: str, suffix: str = None) -> str:
    """Get Databricks Repos path for the specified repo and branch.
    
    Multiple people might be working on the same GitHub repository
    and on different branches at the same time. We need to avoid
    conflicts when deploying code to dev. As a result, we decided
    to have one Repos per branch.
    
    Example: for the reference pipeline with branch "feature/my-new-feature",
    repo will be equal to "sede-x/enterprise-data-platform-reference-pipeline"
    and branch will be equal to "feature/my-new-feature". The Repos path will be
    "/Repos/sede-x-enterprise-data-platform-reference-pipeline/feature-my-new-feature".
    
    NB: at the moment, there can be only one nested folder, which is the reason why
    we replace slashes by dashes in the repository and branch names.
    
    :param repo: GitHub repository name
    :param branch: branch name
    :param suffix: suffix to add
    :return Databricks Repos path for the specified repo and branch
    """
    parent_folder_path = get_repos_parent_folder_path(repo)
    formatted_branch_name = format_branch_name(branch)
    repos_path = f"{parent_folder_path}/{formatted_branch_name}"
    
    if suffix:
        repos_path = f"{repos_path}-{suffix}"
    
    return repos_path


def get_github_repos_url(repo: str) -> str:
    """Get the GitHub repository url.
    
    :param repo: GitHub repository name
    :return: the GitHub repository url
    """
    return f"https://github.com/{repo}.git"


def format_resource_prefix(repo: str, branch: str) -> str:
    """Format resource prefix name to enforce consistency.
    
    :param repo: GitHub repository name
    :param branch: branch name
    :return: formatted resource prefix name
    """
    formatted_repository_name = format_repository_name(repo)
    formatted_branch_name = format_branch_name(branch)
    return f"{formatted_repository_name}-{formatted_branch_name}"


def get_unique_repo_branch_id(repo: str, branch: str, suffix: str = None) -> str:
    """Get the unique repository/branch id.
    
    This will be reused in Unity Catalog's schemas to avoid conflicts.
    
    :param repo: GitHub repository name
    :param branch: branch name
    :param suffix: suffix to add
    :return: formatted unique repository/branch id
    """
    unique_repo_branch_id = format_resource_prefix(repo, branch)

    if suffix:
        unique_repo_branch_id = f"{unique_repo_branch_id}-{suffix}"

    return unique_repo_branch_id.replace("-", "_")


def get_dlt_pipeline_name(repo: str, branch: str=None, suffix: str=None) -> str:
    """Get DLT pipeline name.
    
    Example: for the reference pipeline with branch "feature/my-new-feature",
    the DLT pipeline name will be
    "sede-x-enterprise-data-platform-reference-pipeline-feature-my-new-feature-dlt".
    
    :param repo: GitHub repository name
    :param branch: branch name
    :param suffix: suffix to add
    :return: DLT pipeline name
    """
    if branch:
        prefix_name = format_resource_prefix(repo, branch)
    else:        
        prefix_name = format_repository_name(repo)
    dlt_pipeline_name = f"{prefix_name}-dlt"

    if suffix:
       dlt_pipeline_name = f"{dlt_pipeline_name}-{suffix}"

    return dlt_pipeline_name


def get_wrapping_workflow_name(repo: str, branch: str=None, suffix: str=None) -> str:
    """Get wrapping workflow name.
    
    This workflow wraps the DLT pipeline.
    
    Example: for the reference pipeline with branch "feature/my-new-feature",
    the wrapping workflow name will be
    "sede-x-enterprise-data-platform-reference-pipeline-feature-my-new-feature-workflow".
    
    :param repo: GitHub repository name
    :param branch: branch name
    :param suffix: suffix to add
    :return: workflow name
    """
    if branch:
        prefix_name = format_resource_prefix(repo, branch)
    else:
        prefix_name = format_repository_name(repo)
    wrapping_workflow_name = f"{prefix_name}-workflow"

    if suffix:
       wrapping_workflow_name = f"{wrapping_workflow_name}-{suffix}"

    return wrapping_workflow_name

def get_register_table_uc_workflow_name(repo: str, branch: str=None, suffix: str=None) -> str:
    """Get the name of the workflow that registers tables to UC.
    
    We need this workflow until Databricks releases the DLT integration with UC.
    
    Example: for the reference pipeline with branch "feature/my-new-feature",
    the register table to UC workflow name will be
    "sede-x-enterprise-data-platform-reference-pipeline-feature-my-new-feature-uc".
    
    :param repo: GitHub repository name
    :param branch: branch name
    :param suffix: suffix to add
    :return: register table to UC workflow name
    """
    if branch:
        prefix_name = format_resource_prefix(repo, branch)
    else:
        prefix_name = format_repository_name(repo)

    register_table_uc_workflow_name = f"{prefix_name}-uc"

    if suffix:
       register_table_uc_workflow_name = f"{register_table_uc_workflow_name}-{suffix}"

    return register_table_uc_workflow_name


def get_cluster_name(repo: str, branch: str=None, suffix: str=None) -> str:
    """Get the name of the cluster used to register tables to UC.
    
    This cluster is an interactive one so that we don't launch a new jobs-compute
    cluster per table. 
    
    Example: for the reference pipeline with branch "feature/my-new-feature",
    the cluster name will be
    "sede-x-enterprise-data-platform-reference-pipeline-feature-my-new-feature-cluster".
    
    :param repo: GitHub repository name
    :param branch: branch name
    :param suffix: suffix to add
    :return: name of the cluster that registers tables to UC
    """
    if branch:
        prefix_name = format_resource_prefix(repo, branch)
    else:
        prefix_name = format_repository_name(repo)
    cluster_name = f"{prefix_name}-cluster"

    if suffix:
       cluster_name = f"{cluster_name}-{suffix}"

    return cluster_name
