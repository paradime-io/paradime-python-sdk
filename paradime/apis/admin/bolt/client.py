from typing import Any, Dict, Iterator, List, Optional, Union

from paradime.apis.admin.bolt.types import (
    BOLT_SCHEDULE_DEFINITION_FIELDS,
    BOLT_SCHEDULE_DEFINITION_LINKS,
    TRIGGER_URL_FIELDS,
    TRIGGER_URL_LINKS,
    BoltScheduleDefinition,
    CreateTriggerUrlInput,
    DeployBoltSchedulesInput,
    TriggerUrl,
    TriggerUrlKeyInput,
    TriggerUrlWithSecret,
)
from paradime.apis.admin.jobs.types import (
    DEPLOY_BOLT_SCHEDULES_RESULT_FIELDS,
    AdminJob,
    admin_job_fields,
)
from paradime.apis.admin.transport import AdminTransport
from paradime.apis.admin.types import (
    CONNECTION_FIELDS,
    DEFAULT_PAGE_SIZE,
    Page,
    check_page_size,
    iterate_pages,
    parse_page,
    serialize_input,
)
from paradime.apis.bolt.client import _serialize_input, _serialize_input_list
from paradime.apis.bolt.types import (
    BoltCommandConfigInput,
    BoltDeferredScheduleConfigInput,
    BoltEnvironmentVariableInput,
    BoltIntegrationsInput,
    BoltNotificationsInput,
    BoltScheduleTriggerInput,
    BoltSelfHealingConfigInput,
)


class AdminBoltClient:
    """
    Bolt administration: changing UI-defined schedules, deploying YAML schedules and managing
    Trigger URLs. Running schedules stays on `paradime.bolt`.
    """

    def __init__(self, transport: AdminTransport) -> None:
        self.transport = transport

    def update_schedule(
        self,
        slug: str,
        *,
        name: str,
        schedule: str,
        environment: str,
        commands: Optional[List[str]] = None,
        command_configs: Optional[List[Union[BoltCommandConfigInput, Dict[str, Any]]]] = None,
        continue_on_error: Optional[bool] = None,
        git_branch: Optional[str] = None,
        description: Optional[str] = None,
        timezone: Optional[str] = None,
        owner_email: Optional[str] = None,
        suspended: Optional[bool] = None,
        sla_seconds: Optional[int] = None,
        trigger_on_merge: Optional[bool] = None,
        trigger_on_webhook: Optional[bool] = None,
        notifications: Optional[Union[BoltNotificationsInput, Dict[str, Any]]] = None,
        integrations: Optional[Union[BoltIntegrationsInput, Dict[str, Any]]] = None,
        self_healing: Optional[Union[BoltSelfHealingConfigInput, Dict[str, Any]]] = None,
        turbo_ci: Optional[Union[BoltDeferredScheduleConfigInput, Dict[str, Any]]] = None,
        deferred_schedule: Optional[Union[BoltDeferredScheduleConfigInput, Dict[str, Any]]] = None,
        schedule_trigger: Optional[Union[BoltScheduleTriggerInput, Dict[str, Any]]] = None,
        env_vars: Optional[List[Union[BoltEnvironmentVariableInput, Dict[str, Any]]]] = None,
        workspace_uid: Optional[str] = None,
    ) -> BoltScheduleDefinition:
        """
        Replaces a UI or API-created schedule's settings, taking them as
        `paradime.bolt.create_schedule` does. Every setting is replaced, except that leaving
        out `suspended`, `continue_on_error` or `env_vars` keeps their values. `name` is the
        display name and never changes the slug.

        Args:
            slug (str): The slug of the schedule to change.
            workspace_uid (str, optional): The workspace to change, for a company API key.

        Returns:
            BoltScheduleDefinition: The schedule, as now stored.
        """

        query = f"""
            mutation adminUpdateBoltSchedule($input: UpdateBoltScheduleInput!) {{
                updateBoltSchedule(input: $input) {{
                    schedule {{
                        {BOLT_SCHEDULE_DEFINITION_FIELDS}
                    }}
                }}
            }}
        """

        settings: Dict[str, Any] = {
            "name": name,
            "schedule": schedule,
            "environment": environment,
            "commands": commands,
            "commandConfigs": _serialize_input_list(command_configs),
            "continueOnError": continue_on_error,
            "gitBranch": git_branch,
            "description": description,
            "timezone": timezone,
            "ownerEmail": owner_email,
            "suspended": suspended,
            "slaSeconds": sla_seconds,
            "triggerOnMerge": trigger_on_merge,
            "triggerOnWebhook": trigger_on_webhook,
            "notifications": _serialize_input(notifications),
            "integrations": _serialize_input(integrations),
            "selfHealing": _serialize_input(self_healing),
            "turboCi": _serialize_input(turbo_ci),
            "deferredSchedule": _serialize_input(deferred_schedule),
            "scheduleTrigger": _serialize_input(schedule_trigger),
            "envVars": _serialize_input_list(env_vars),
        }
        schedule_input = {key: value for key, value in settings.items() if value is not None}
        response = self.transport.execute(
            query,
            {"input": {"slug": slug, "schedule": schedule_input}},
            workspace_uid=workspace_uid,
            linked_fields=BOLT_SCHEDULE_DEFINITION_LINKS,
        )
        return BoltScheduleDefinition.parse_obj(response["updateBoltSchedule"]["schedule"])

    def deploy_schedules(
        self, *, branch: Optional[str] = None, workspace_uid: Optional[str] = None
    ) -> AdminJob:
        """
        Starts a job that deploys the schedules defined in YAML from the workspace's deploy
        branch. A YAML error fails the job with `VALIDATION_FAILED`.

        Args:
            branch (str, optional): The branch you expect the schedules to deploy from; a
                different branch is refused before anything starts.
            workspace_uid (str, optional): The workspace to deploy, for a company API key.

        Returns:
            AdminJob: The job; wait for it with `admin.jobs.wait`. Its result is a
                `DeployBoltSchedulesResult` that holds the first page of the schedules it read,
                each with its source and, when it was not deployed, why. Read them all with
                `admin.jobs.deployed_schedules`.
        """

        query = f"""
            mutation adminDeployBoltSchedules($input: DeployBoltSchedulesInput!) {{
                deployBoltSchedules(input: $input) {{
                    job {{
                        {admin_job_fields(DEPLOY_BOLT_SCHEDULES_RESULT_FIELDS)}
                    }}
                }}
            }}
        """

        deploy_input = (
            DeployBoltSchedulesInput(branch=branch) if branch else DeployBoltSchedulesInput()
        )
        response = self.transport.execute(
            query, {"input": serialize_input(deploy_input)}, workspace_uid=workspace_uid
        )
        return AdminJob.parse_obj(response["deployBoltSchedules"]["job"])

    def list_trigger_urls(
        self,
        schedule_slug: str,
        *,
        workspace_uid: Optional[str] = None,
        page_size: int = DEFAULT_PAGE_SIZE,
    ) -> Iterator[TriggerUrl]:
        """
        Iterates over a schedule's Trigger URLs, oldest first. Their secrets are never returned.

        Args:
            schedule_slug (str): The schedule's slug.
            workspace_uid (str, optional): The workspace to read, for a company API key.
            page_size (int): How many URLs to read per request, at most 500.

        Returns:
            Iterator[TriggerUrl]: The Trigger URLs, read a page at a time.
        """

        check_page_size(page_size)
        query = f"""
            query adminTriggerUrls(
                $workspaceUid: String
                $scheduleSlug: String!
                $first: Int
                $after: String
            ) {{
                workspace(workspaceUid: $workspaceUid) {{
                    triggerUrls(scheduleSlug: $scheduleSlug, first: $first, after: $after) {{
                        edges {{
                            node {{
                                {TRIGGER_URL_FIELDS}
                            }}
                        }}
                        {CONNECTION_FIELDS}
                    }}
                }}
            }}
        """

        def fetch_page(after: Optional[str]) -> Page[TriggerUrl]:
            response = self.transport.execute(
                query,
                {
                    "workspaceUid": workspace_uid,
                    "scheduleSlug": schedule_slug,
                    "first": page_size,
                    "after": after,
                },
                workspace_uid=workspace_uid,
                linked_fields=TRIGGER_URL_LINKS,
            )
            workspace = response["workspace"] or {}
            return parse_page(workspace.get("triggerUrls"), TriggerUrl.parse_obj)

        return iterate_pages(fetch_page)

    def create_trigger_url(
        self, schedule_slug: str, name: str, *, workspace_uid: Optional[str] = None
    ) -> TriggerUrlWithSecret:
        """
        Creates a Trigger URL for a schedule. Its secret is returned only here: store it now.

        Args:
            schedule_slug (str): The slug of the schedule the URL starts.
            name (str): The URL's name, unique among the schedule's.
            workspace_uid (str, optional): The workspace to change, for a company API key.

        Returns:
            TriggerUrlWithSecret: The Trigger URL and its secret, sent as
                ``Authorization: Bearer <secret>``.
        """

        query = f"""
            mutation adminCreateTriggerUrl($input: CreateTriggerUrlInput!) {{
                createTriggerUrl(input: $input) {{
                    triggerUrl {{
                        {TRIGGER_URL_FIELDS}
                    }}
                    secret
                }}
            }}
        """

        create_input = CreateTriggerUrlInput(schedule_slug=schedule_slug, name=name)
        response = self.transport.execute(
            query,
            {"input": serialize_input(create_input)},
            workspace_uid=workspace_uid,
            linked_fields=TRIGGER_URL_LINKS,
        )
        return TriggerUrlWithSecret.parse_obj(response["createTriggerUrl"])

    def rotate_trigger_url_secret(
        self, public_id: str, *, workspace_uid: Optional[str] = None
    ) -> TriggerUrlWithSecret:
        """
        Gives a Trigger URL a new secret; the old one stops working now. The new secret is
        returned only here: store it now.

        Args:
            public_id (str): The Trigger URL's public id.
            workspace_uid (str, optional): The workspace to change, for a company API key.

        Returns:
            TriggerUrlWithSecret: The Trigger URL and its new secret.
        """

        query = f"""
            mutation adminRotateTriggerUrlSecret($input: RotateTriggerUrlSecretInput!) {{
                rotateTriggerUrlSecret(input: $input) {{
                    triggerUrl {{
                        {TRIGGER_URL_FIELDS}
                    }}
                    secret
                }}
            }}
        """

        response = self.transport.execute(
            query,
            {"input": serialize_input(TriggerUrlKeyInput(public_id=public_id))},
            workspace_uid=workspace_uid,
            linked_fields=TRIGGER_URL_LINKS,
        )
        return TriggerUrlWithSecret.parse_obj(response["rotateTriggerUrlSecret"])

    def pause_trigger_url(
        self, public_id: str, *, workspace_uid: Optional[str] = None
    ) -> TriggerUrl:
        """
        Pauses a Trigger URL: a request to it starts no run. Pausing a paused one changes
        nothing.

        Args:
            public_id (str): The Trigger URL's public id.
            workspace_uid (str, optional): The workspace to change, for a company API key.

        Returns:
            TriggerUrl: The Trigger URL, now paused.

        Raises:
            AdminNotFoundException: When there is no Trigger URL with this public id.
        """

        return self._change_trigger_url("pauseTriggerUrl", public_id, workspace_uid)

    def resume_trigger_url(
        self, public_id: str, *, workspace_uid: Optional[str] = None
    ) -> TriggerUrl:
        """
        Resumes a paused Trigger URL: a request to it starts a run again. Resuming one that
        runs changes nothing.

        Args:
            public_id (str): The Trigger URL's public id.
            workspace_uid (str, optional): The workspace to change, for a company API key.

        Returns:
            TriggerUrl: The Trigger URL, now running.

        Raises:
            AdminNotFoundException: When there is no Trigger URL with this public id.
        """

        return self._change_trigger_url("resumeTriggerUrl", public_id, workspace_uid)

    def delete_trigger_url(self, public_id: str, *, workspace_uid: Optional[str] = None) -> str:
        """
        Deletes a Trigger URL. Deleting it again is not an error.

        Args:
            public_id (str): The Trigger URL's public id.
            workspace_uid (str, optional): The workspace to change, for a company API key.

        Returns:
            str: The public id of the deleted Trigger URL.
        """

        query = """
            mutation adminDeleteTriggerUrl($input: DeleteTriggerUrlInput!) {
                deleteTriggerUrl(input: $input) {
                    deletedPublicId
                }
            }
        """

        response = self.transport.execute(
            query,
            {"input": serialize_input(TriggerUrlKeyInput(public_id=public_id))},
            workspace_uid=workspace_uid,
        )
        return response["deleteTriggerUrl"]["deletedPublicId"]

    def _change_trigger_url(
        self, mutation: str, public_id: str, workspace_uid: Optional[str]
    ) -> TriggerUrl:
        input_type = mutation[0].upper() + mutation[1:] + "Input"
        query = f"""
            mutation admin{input_type[:-5]}($input: {input_type}!) {{
                {mutation}(input: $input) {{
                    triggerUrl {{
                        {TRIGGER_URL_FIELDS}
                    }}
                }}
            }}
        """

        response = self.transport.execute(
            query,
            {"input": serialize_input(TriggerUrlKeyInput(public_id=public_id))},
            workspace_uid=workspace_uid,
            linked_fields=TRIGGER_URL_LINKS,
        )
        return TriggerUrl.parse_obj(response[mutation]["triggerUrl"])
