import argparse
import json
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

BOOTSTRAP_UID = "ws-bootstrap"
LEFTOVER_UID = "ws-leftover"
NEW_UID = "ws-new"
PUBLIC_KEY = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIStubDeployKeyForTheOnboardingTest paradime"
NOW = "2026-11-05T10:00:00Z"
OPERATION = re.compile(r"\b(?:query|mutation)\s+(\w+)")

Reply = Tuple[int, Dict[str, Any]]


def _workspace(uid: str, name: str, **settings: Any) -> Dict[str, Any]:
    return {
        "uid": uid,
        "name": name,
        "createdAt": NOW,
        "dbtVersion": settings.get("dbtVersion", "1.10"),
        "supportedDbtVersions": ["1.9", "1.10"],
        "dbtPartialParsing": settings.get("dbtPartialParsing", True),
        "defaultBranch": settings.get("defaultBranch"),
        "scheduleDeploymentBranch": settings.get("scheduleDeploymentBranch"),
    }


def _environment(slug: str, type_: str, name: str, **fields: Any) -> Dict[str, Any]:
    return {
        "slug": slug,
        "type": type_,
        "name": name,
        "isDefault": slug == "production",
        "branch": fields.get("branch"),
        "dbtVersion": None,
        "createdAt": NOW,
        "deferEnvironmentSlug": "production" if type_ == "AGENT" else None,
        "deferScheduleSlug": None,
        "hasWarehouseConnection": False,
        "variableNames": [],
    }


def _job(job_id: str, kind: str, status: str, result: Any = None) -> Dict[str, Any]:
    return {
        "id": job_id,
        "kind": kind,
        "status": status,
        "failure": None,
        "result": json.dumps(result) if result is not None else None,
        "createdAt": NOW,
        "updatedAt": NOW,
    }


def _page(nodes: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "edges": [{"node": node} for node in nodes],
        "totalCount": len(nodes),
        "pageInfo": {"endCursor": None, "hasNextPage": False},
    }


def _error(code: str, message: str, status: int = 200, **extensions: Any) -> Reply:
    return status, {
        "data": None,
        "errors": [{"message": message, "extensions": {"code": code, **extensions}}],
    }


class StubAdminApi:
    """Canned Admin API answers for the onboarding example, keyed by operation name."""

    def __init__(self, *, fail_operation: Optional[str] = None, leftover: bool = True) -> None:
        self.fail_operation = fail_operation
        self.workspaces: Dict[str, Dict[str, Any]] = {
            BOOTSTRAP_UID: _workspace(BOOTSTRAP_UID, "bootstrap"),
        }
        if leftover:
            self.workspaces[LEFTOVER_UID] = _workspace(
                LEFTOVER_UID, "onboarding-e2e-20261104-020000"
            )
        self.environments: Dict[str, Dict[str, Any]] = {}
        self.calls: List[Tuple[str, Optional[str]]] = []
        self.deleted: List[str] = []
        self.polls: Dict[str, int] = {}
        self.rate_limited = False
        self.connected = False
        self.github_keys: Dict[int, str] = {41: "onboarding-e2e-20261104-020000"}
        self.lock = threading.Lock()

    def handle(self, query: str, variables: Dict[str, Any], workspace: Optional[str]) -> Reply:
        match = OPERATION.search(query)
        operation = match.group(1) if match else "anonymous"
        with self.lock:
            self.calls.append((operation, workspace))
            if operation == self.fail_operation:
                return _error("CONNECTION_TEST_FAILED", "The warehouse refused the connection.")
            handler: Optional[Callable[[Dict[str, Any], Optional[str]], Reply]] = getattr(
                self, f"op_{operation}", None
            )
            if handler is None:
                return _error("INTERNAL", f"The stub has no answer for {operation}.")
            return handler(variables, workspace)

    def ok(self, data: Dict[str, Any]) -> Reply:
        return 200, {"data": data}

    def op_adminWorkspaces(self, variables: Dict[str, Any], workspace: Optional[str]) -> Reply:
        return self.ok({"workspaces": _page(list(self.workspaces.values()))})

    def op_adminCreateWorkspace(self, variables: Dict[str, Any], workspace: Optional[str]) -> Reply:
        name = variables["input"]["name"]
        self.workspaces[NEW_UID] = _workspace(NEW_UID, name)
        self.environments = {"production": _environment("production", "SCHEDULER", "Production")}
        return self.ok({"createWorkspace": {"workspace": self.workspaces[NEW_UID]}})

    def op_adminUpdateWorkspace(self, variables: Dict[str, Any], workspace: Optional[str]) -> Reply:
        settings = dict(variables["input"])
        uid = settings.pop("workspaceUid")
        current = self.workspaces[uid]
        current.update(settings)
        return self.ok({"updateWorkspace": {"workspace": current}})

    def op_adminDeleteWorkspace(self, variables: Dict[str, Any], workspace: Optional[str]) -> Reply:
        uid = variables["input"]["workspaceUid"]
        deleted = self.workspaces.pop(uid, None) or {"uid": uid, "name": "deleted"}
        self.deleted.append(uid)
        return self.ok({"deleteWorkspace": {"workspace": {"uid": uid, "name": deleted["name"]}}})

    def _repository(self) -> Dict[str, Any]:
        return {
            "mode": "SSH_DEPLOY_KEY",
            "state": "CONNECTED" if self.connected else "NOT_CONNECTED",
            "provider": "GITHUB" if self.connected else None,
            "uri": "git@github.com:acme/analytics.git" if self.connected else None,
            "dbtProjectDir": None,
            "verified": self.connected,
            "deployKey": {"publicKey": PUBLIC_KEY},
            "githubApp": None,
            "gitlabApp": None,
            "githubAppInstallUrl": "https://github.com/apps/paradime/installations/new?state=stub",
        }

    def op_adminGitRepository(self, variables: Dict[str, Any], workspace: Optional[str]) -> Reply:
        return self.ok({"workspace": {"gitRepository": self._repository()}})

    def op_adminConnectGitRepository(
        self, variables: Dict[str, Any], workspace: Optional[str]
    ) -> Reply:
        repository = dict(self._repository(), state="PENDING_VALIDATION")
        job = _job("job-git", "connectGitRepository", "QUEUED")
        return self.ok({"connectGitRepository": {"gitRepository": repository, "job": job}})

    def op_adminJob(self, variables: Dict[str, Any], workspace: Optional[str]) -> Reply:
        job_id = variables["jobId"]
        polls = self.polls[job_id] = self.polls.get(job_id, 0) + 1
        if job_id == "job-git":
            if polls == 1:
                return self.ok({"adminJob": _job(job_id, "connectGitRepository", "RUNNING")})
            self.connected = True
            result = {"uri": "git@github.com:acme/analytics.git"}
            return self.ok({"adminJob": _job(job_id, "connectGitRepository", "SUCCEEDED", result)})
        result = {
            "scheduleParseId": 7,
            "gitHash": "0f3c2a1",
            "schedules": [{"slug": "daily_run", "source": "YAML", "error": None}],
        }
        return self.ok({"adminJob": _job(job_id, "deployBoltSchedules", "SUCCEEDED", result)})

    def op_onboardingSetDevelopmentConnection(
        self, variables: Dict[str, Any], workspace: Optional[str]
    ) -> Reply:
        connection_input = variables["input"]
        saved = {
            "scope": connection_input["scope"],
            "type": "SNOWFLAKE",
            "connectionName": connection_input.get("connectionName"),
            "targetName": connection_input.get("targetName"),
            "updatedAt": NOW,
        }
        return self.ok({"setWarehouseConnection": {"warehouseConnection": saved}})

    def op_adminCreateEnvironment(
        self, variables: Dict[str, Any], workspace: Optional[str]
    ) -> Reply:
        environment_input = variables["input"]
        environment = _environment(
            environment_input["slug"],
            environment_input["type"],
            environment_input["name"],
            branch=environment_input.get("branch"),
        )
        self.environments.setdefault(environment["slug"], environment)
        return self.ok(
            {"createEnvironment": {"environment": self.environments[environment["slug"]]}}
        )

    def op_adminEnvironments(self, variables: Dict[str, Any], workspace: Optional[str]) -> Reply:
        return self.ok({"workspace": {"environments": _page(list(self.environments.values()))}})

    def op_adminSetWarehouseConnection(
        self, variables: Dict[str, Any], workspace: Optional[str]
    ) -> Reply:
        connection_input = variables["input"]
        saved = {
            "scope": connection_input["scope"],
            "environmentSlug": connection_input.get("environmentSlug"),
            "connectionId": "default",
            "type": "SNOWFLAKE",
            "isDefault": True,
            "connectionName": connection_input.get("connectionName"),
            "targetName": connection_input.get("targetName"),
            "threads": connection_input.get("threads"),
            "updatedAt": NOW,
            "settings": None,
        }
        return self.ok({"setWarehouseConnection": {"warehouseConnection": saved}})

    def op_adminSetEnvironmentVariables(
        self, variables: Dict[str, Any], workspace: Optional[str]
    ) -> Reply:
        if not self.rate_limited:
            self.rate_limited = True
            return _error("RATE_LIMITED", "Rate limit exceeded.", status=429, retryAfter=0.05)
        names = [variable["name"] for variable in variables["input"]["variables"]]
        return self.ok(
            {
                "setEnvironmentVariables": {
                    "changedNames": names,
                    "variables": {"totalCount": len(names)},
                }
            }
        )

    def op_adminDeployBoltSchedules(
        self, variables: Dict[str, Any], workspace: Optional[str]
    ) -> Reply:
        job = _job("job-deploy", "deployBoltSchedules", "QUEUED")
        return self.ok({"deployBoltSchedules": {"job": job}})

    def op_adminSetMsTeamsWebhook(
        self, variables: Dict[str, Any], workspace: Optional[str]
    ) -> Reply:
        webhook = {
            "name": variables["input"]["name"],
            "urlIsSet": True,
            "urlHint": "https://acme.webhook.office.com/...",
            "updatedAt": NOW,
        }
        return self.ok({"setMsTeamsWebhook": {"msTeamsWebhook": webhook}})

    def op_adminSetAlertNotifications(
        self, variables: Dict[str, Any], workspace: Optional[str]
    ) -> Reply:
        notifications_input = variables["input"]
        alerts = {
            "category": notifications_input["category"],
            "slack": None,
            "msTeams": notifications_input.get("msTeams"),
            "email": notifications_input.get("email"),
        }
        return self.ok({"setAlertNotifications": {"alertNotifications": alerts}})

    def op_adminSlack(self, variables: Dict[str, Any], workspace: Optional[str]) -> Reply:
        slack = {
            "status": "PENDING_USER_ACTION",
            "installUrl": "https://app.paradime.io/slack/install?state=stub",
            "teamId": None,
            "teamName": None,
            "reconnectRequired": False,
            "missingScopes": [],
        }
        return self.ok({"slack": slack})

    def op_inviteUser(self, variables: Dict[str, Any], workspace: Optional[str]) -> Reply:
        return self.ok({"inviteUser": {"ok": True}})

    def op_CreateBoltSchedule(self, variables: Dict[str, Any], workspace: Optional[str]) -> Reply:
        return self.ok({"createBoltSchedule": {"slug": "onboarding-acceptance-run"}})

    def op_triggerBoltRun(self, variables: Dict[str, Any], workspace: Optional[str]) -> Reply:
        return self.ok({"triggerBoltRun": {"runId": 101}})

    def op_boltRunStatus(self, variables: Dict[str, Any], workspace: Optional[str]) -> Reply:
        polls = self.polls["run"] = self.polls.get("run", 0) + 1
        return self.ok({"boltRunStatus": {"state": "RUNNING" if polls == 1 else "SUCCESS"}})

    def op_onboardingWorkspaceSetupStatus(
        self, variables: Dict[str, Any], workspace: Optional[str]
    ) -> Reply:
        def row(
            step: str, status: str = "DONE", required: bool = True, **fields: Any
        ) -> Dict[str, Any]:
            return {
                "step": step,
                "status": status,
                "required": required,
                "environmentSlug": fields.get("environmentSlug"),
                "installUrl": fields.get("installUrl"),
                "detail": fields.get("detail"),
            }

        steps = [
            row("GIT_REPOSITORY"),
            row(
                "GITHUB_APP",
                "PENDING_USER_ACTION",
                required=False,
                installUrl="https://github.com/apps/paradime/installations/new?state=stub",
            ),
            row("DEVELOPMENT_CONNECTION"),
            row("SCHEDULER_CONNECTION", environmentSlug="production"),
            row("SCHEDULER_CONNECTION", environmentSlug="staging"),
            row("AGENT_CONNECTION", required=False),
            row("ENVIRONMENT_VARIABLES", required=False),
            row("SCHEDULES_DEPLOYED"),
            row("NOTIFICATIONS", required=False),
            row("FIRST_BOLT_RUN"),
        ]
        return self.ok({"workspaceSetupStatus": {"isComplete": True, "steps": steps}})

    def github(self, method: str, path: str, body: Dict[str, Any]) -> Reply:
        with self.lock:
            self.calls.append((f"github {method}", None))
            if method == "GET":
                query = parse_qs(urlparse(path).query)
                per_page = int(query.get("per_page", ["30"])[0])
                page = int(query.get("page", ["1"])[0])
                keys = [
                    {"id": key_id, "title": title} for key_id, title in self.github_keys.items()
                ]
                start = (page - 1) * per_page
                more = len(keys) > start + per_page
                next_query = f"?per_page={per_page}&page={page + 1}" if more else None
                return 200, {"keys": keys[start : start + per_page], "next": next_query}
            if method == "POST":
                key_id = max(self.github_keys, default=0) + 1
                self.github_keys[key_id] = body["title"]
                return 201, {"id": key_id}
            self.github_keys.pop(int(path.rsplit("/", 1)[-1]), None)
            return 204, {}


def _handler(api: StubAdminApi) -> type:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            pass

        def _body(self) -> Dict[str, Any]:
            length = int(self.headers.get("Content-Length") or 0)
            return json.loads(self.rfile.read(length)) if length else {}

        def _send(self, status: int, body: Any, link: Optional[str] = None) -> None:
            data = json.dumps(body).encode() if status != 204 else b""
            self.send_response(status)
            if link:
                self.send_header("Link", f'<{link}>; rel="next"')
            if status == 429:
                self.send_header("Retry-After", "1")
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _github(self, method: str) -> None:
            status, body = api.github(method, self.path, self._body() if method == "POST" else {})
            if method != "GET":
                self._send(status, body)
                return
            base = f"http://{self.headers['Host']}{urlparse(self.path).path}"
            self._send(status, body["keys"], base + body["next"] if body["next"] else None)

        def do_GET(self) -> None:
            self._github("GET")

        def do_DELETE(self) -> None:
            self._github("DELETE")

        def do_POST(self) -> None:
            if self.path.startswith("/github/"):
                self._github("POST")
                return
            body = self._body()
            status, reply = api.handle(
                body["query"], body.get("variables") or {}, self.headers.get("X-Paradime-Workspace")
            )
            self._send(status, reply)

    return Handler


def start_stub(api: StubAdminApi, port: int = 0) -> Tuple[ThreadingHTTPServer, str]:
    server = ThreadingHTTPServer(("127.0.0.1", port), _handler(api))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, f"http://127.0.0.1:{server.server_address[1]}"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Serve the onboarding stub Admin API.")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--fail", dest="fail_operation", default=None)
    arguments = parser.parse_args()
    stub_server, url = start_stub(
        StubAdminApi(fail_operation=arguments.fail_operation), arguments.port
    )
    print(f"GraphQL: {url}/graphql  GitHub: {url}/github", flush=True)
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        stub_server.shutdown()
