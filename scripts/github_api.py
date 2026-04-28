# GitHub Workflow API wrapper
import requests
from datetime import datetime


class GitHubWorkflowAPI:
    def __init__(self, github_token: str):
        self.github_token = github_token
        self.headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": "Bearer " + github_token,
            "X-GitHub-Api-Version": "2022-11-28",
        }
        self.time_format = "%Y-%m-%dT%H:%M:%SZ"

    def get_workflow_duration_list(self, repo: str, workflow_id: str, accurate=False, cutoff_date=None, skip_run_ids=None):
        from concurrent.futures import ThreadPoolExecutor, as_completed

        skip_run_ids = skip_run_ids or set()
        payloads = {"per_page": 100, "status": "completed", "page": "1"}
        endpoint = (
            f"https://api.github.com/repos/{repo}/actions/workflows/{workflow_id}/runs"
        )

        first_page_response = requests.get(
            endpoint, headers=self.headers, params=payloads
        ).json()

        if "workflow_runs" not in first_page_response:
            raise RuntimeError(
                f"GitHub API エラー: {first_page_response.get('message', first_page_response)}"
            )
        workflow_runs = first_page_response["workflow_runs"]

        print(f"Total count: {first_page_response['total_count']}")

        # Reuse first_page_response to get the total count of workflow runs
        total_count = first_page_response["total_count"]

        # Calculate the number of pages needed
        pages_needed = (total_count + payloads["per_page"] - 1) // payloads[
            "per_page"
        ]  # This calculates the ceiling of total_count/100

        # Fetch using the list of page numbers; stop early if all runs on page are older than cutoff_date
        for page in range(2, pages_needed + 1):
            payloads["page"] = page
            page_response = requests.get(
                endpoint, headers=self.headers, params=payloads
            ).json()
            page_runs = page_response["workflow_runs"]
            workflow_runs = page_runs + workflow_runs
            if cutoff_date and page_runs:
                oldest_on_page = min(
                    datetime.strptime(r["created_at"], self.time_format)
                    for r in page_runs
                )
                if oldest_on_page < cutoff_date:
                    break

        # Time format conversion (utility function)
        for run in workflow_runs:
            run["created_at"] = datetime.strptime(run["created_at"], self.time_format)
            run["updated_at"] = datetime.strptime(run["updated_at"], self.time_format)

        # Sorting by created_at (oldest to newest, utility function)
        workflow_runs = sorted(workflow_runs, key=lambda k: k["created_at"])

        # Extract duration from each workflow run
        if not accurate:
            # By created_at and updated_at
            for run in workflow_runs:
                run["duration"] = (
                    run["updated_at"] - run["created_at"]
                ).total_seconds()

            return workflow_runs

        # By calling jobs API for each workflow run (parallel, skip known runs)
        runs_needing_fetch = [r for r in workflow_runs if r["id"] not in skip_run_ids]
        print(f"jobs API 呼び出し対象: {len(runs_needing_fetch)} 件 (スキップ: {len(workflow_runs) - len(runs_needing_fetch)} 件)")

        def fetch_duration(run):
            import time
            for attempt in range(3):
                response = requests.get(run["jobs_url"], headers=self.headers)
                data = response.json()
                if "jobs" in data:
                    jobs = data["jobs"]
                    break
                # レート制限 or 一時的エラー: 少し待ってリトライ
                wait = int(response.headers.get("Retry-After", 10)) if response.status_code == 429 else 5
                print(f"jobs API エラー (run_id={run['id']}, status={response.status_code}): {data.get('message', data)}, {wait}秒後リトライ ({attempt+1}/3)")
                time.sleep(wait)
            else:
                print(f"jobs API 3回失敗 (run_id={run['id']}), スキップ")
                return run["id"], 0
            duration = 0
            for job in jobs:
                completed_at = datetime.strptime(job["completed_at"], self.time_format)
                started_at = datetime.strptime(job["started_at"], self.time_format)
                duration += (completed_at - started_at).total_seconds()
            return run["id"], duration

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(fetch_duration, run): run for run in runs_needing_fetch}
            results = {}
            for future in as_completed(futures):
                run_id, duration = future.result()
                results[run_id] = duration

        for run in workflow_runs:
            if run["id"] in results:
                run["duration"] = results[run["id"]]

        return workflow_runs

    def _extract_zip_to_dict(self, zip_content: bytes) -> dict:
        import zipfile
        from io import BytesIO

        binary_extensions = (".gz", ".bin", ".log", ".png", ".jpg")
        zf = zipfile.ZipFile(BytesIO(zip_content))
        result = {}
        for name in zf.namelist():
            data = zf.read(name)
            if any(name.endswith(ext) for ext in binary_extensions):
                result[name] = data
            else:
                result[name] = data.decode("utf-8")
        return result

    def get_run_artifacts(self, repo: str, run_id: int) -> list:
        endpoint = f"https://api.github.com/repos/{repo}/actions/runs/{run_id}/artifacts"
        response = requests.get(endpoint, headers=self.headers).json()
        return response.get("artifacts", [])

    def download_artifact(self, repo: str, artifact_id: int) -> dict:
        endpoint = f"https://api.github.com/repos/{repo}/actions/artifacts/{artifact_id}/zip"
        response = requests.get(endpoint, headers=self.headers, allow_redirects=True).content
        return self._extract_zip_to_dict(response)

    def get_workflow_logs(self, repo: str, run_id: str):
        endpoint = f"https://api.github.com/repos/{repo}/actions/runs/{run_id}/logs"
        response = requests.get(
            endpoint, headers=self.headers, allow_redirects=True
        ).content
        return self._extract_zip_to_dict(response)


class GithubPullRequestAPI:
    def __init__(self, github_token: str):
        self.github_token = github_token
        self.headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": "Bearer " + github_token,
            "X-GitHub-Api-Version": "2022-11-28",
        }
        self.time_format = "%Y-%m-%dT%H:%M:%SZ"

    def get_all_pull_requests(self, repo: str):
        payloads = {"per_page": 100, "page": 1, "state": "all"}
        endpoint = f"https://api.github.com/repos/{repo}/pulls"
        response = requests.get(endpoint, headers=self.headers, params=payloads).json()

        pull_requests = response

        while len(response) == payloads["per_page"]:
            payloads["page"] += 1
            response = requests.get(
                endpoint, headers=self.headers, params=payloads
            ).json()

            pull_requests += response

        for pull_request in pull_requests:
            pull_request["created_at"] = datetime.strptime(
                pull_request["created_at"], self.time_format
            )
            if pull_request["closed_at"] is not None:
                pull_request["closed_at"] = datetime.strptime(
                    pull_request["closed_at"], self.time_format
                )

        return pull_requests


class GithubPackagesAPI:
    def __init__(self, github_token: str):
        self.github_token = github_token
        self.headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": "Bearer " + github_token,
            "X-GitHub-Api-Version": "2022-11-28",
        }
        self.time_format = "%Y-%m-%dT%H:%M:%SZ"

    def get_all_containers(self, org: str, pkg: str):
        payloads = {"per_page": 100, "page": 1}
        endpoint = (
            f"https://api.github.com/orgs/{org}/packages/container/{pkg}/versions"
        )
        response = requests.get(endpoint, headers=self.headers, params=payloads).json()

        packages = response

        while len(response) == payloads["per_page"]:
            payloads["page"] += 1
            response = requests.get(
                endpoint, headers=self.headers, params=payloads
            ).json()

            packages += response

        for package in packages:
            package["created_at"] = datetime.strptime(
                package["created_at"], self.time_format
            )
            package["updated_at"] = datetime.strptime(
                package["updated_at"], self.time_format
            )

        return packages
