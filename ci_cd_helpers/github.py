import requests
from requests.exceptions import HTTPError


def get_owner_and_repo(full_repo_name: str) -> (str, str):
    """Get GitHub owner and repository name from the full repository name.
    
    NB: the full repository name, gotten from variable $GITHUB_REPOSITORY, is
    of the form "owner/repo".

    :param full_repo_name: GitHub full repository name
    :return: (owner, repo) tuple
    """
    owner, repo = full_repo_name.split("/")
    owner = owner.strip("/")
    repo = repo.strip("/")
    return owner, repo


def get_rest_headers(token: str) -> dict:
    """Create GitHub REST API headers.
    
    :param token: GitHub token
    :return: GitHub REST API headers
    """
    return {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def create_branch(branch_name: str, sha: str, full_repo_name: str, token: str) -> dict:
    """Create a new branch.

    :param branch_name: name of the branch to create
    :param sha: commit sha to use when creating the branch
    :param full_repo_name: GitHub full repository name (i.e. containing the owner)
    :param token: GitHub token
    :return: output of the REST API call, see https://docs.github.com/en/rest/git/refs?apiVersion=2022-11-28#create-a-reference
    """
    owner, repo = get_owner_and_repo(full_repo_name)
    headers = get_rest_headers(token)
    
    req = requests.post(
        f"https://api.github.com/repos/{owner}/{repo}/git/refs",
        headers=headers,
        json={
            "ref": f"refs/heads/{branch_name}",
            "sha": sha,
        }
    )
  
    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e

    return req.json()


def create_tag(tag_name: str, sha: str, full_repo_name: str, token: str) -> dict:
    """Create a new tag.

    :param tag_name: name of the tag to create
    :param sha: commit sha to use when creating the tag
    :param full_repo_name: GitHub full repository name (i.e. containing the owner)
    :param token: GitHub token
    :return: output of the REST API call, see https://docs.github.com/en/rest/git/refs?apiVersion=2022-11-28#create-a-reference
    """
    owner, repo = get_owner_and_repo(full_repo_name)
    headers = get_rest_headers(token)
    
    req = requests.post(
        f"https://api.github.com/repos/{owner}/{repo}/git/refs",
        headers=headers,
        json={
            "ref": f"refs/tags/{tag_name}",
            "sha": sha,
        }
    )
  
    try:
        req.raise_for_status()
    except HTTPError as e:
        print(e.response.text)
        raise e

    return req.json()


def check_if_is_release_branch(branch_name: str) -> bool:
    """Check if the branch is a release branch.
    
    :param branch_name: branch name
    :return: True if the branch is a release branch, False otherwise.
    """
    return branch_name.startswith("release/v")


def check_if_is_hotfix_branch(branch_name: str) -> bool:
    """Check if the branch is a hotfix branch.
    
    :param branch_name: branch name
    :return: True if the branch is a hotfix branch, False otherwise.
    """
    return branch_name.startswith("hotfix/v")


def get_tag_from_release(release: str) -> str:
    """Retrieve tag from a release branch.
    
    :param release: release branch name, of the form 'release/vx.y.z'
    :return: tag of the form vx.y.z
    """
    prefix = "release/"
    assert release.startswith(f"{prefix}v"), f"A release should be of the form '{prefix}vx.y.z'."
    return release.strip(prefix)


def get_tag_from_hotfix(hotfix: str) -> str:
    """Retrieve tag from a hotfix branch.
    
    :param hotfix: hotfix branch name, of the form 'hotfix/vx.y.z'
    :return: tag of the form vx.y.z
    """
    prefix = "hotfix/"
    assert hotfix.startswith(f"{prefix}v"), f"A hotfix should be of the form '{prefix}vx.y.z'."
    return hotfix.strip(prefix)


def get_tag_version(branch_name: str) -> str:
    """Retrieve tag from a realease or a hotfix branch.
    
    :param branch_name: release/hotfix branch name, of the form 'release/vx.y.z' or 'hotfix/vx.y.z'
    :return: tag of the form vx.y.z
    """
    is_release_branch = check_if_is_release_branch(branch_name)
    is_hotfix_branch = check_if_is_hotfix_branch(branch_name)

    assert is_release_branch or is_hotfix_branch, f"branch_name {branch_name} should be a release or a hotfix branch."

    tag_version = ""

    if is_release_branch:
        tag_version = get_tag_from_release(branch_name)

    if is_hotfix_branch:
        tag_version = get_tag_from_hotfix(branch_name)

    return tag_version
