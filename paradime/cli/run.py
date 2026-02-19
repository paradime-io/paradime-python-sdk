from typing import Final

import click

from paradime.cli.integrations.adf import adf_list_pipelines, adf_pipelines
from paradime.cli.integrations.airbyte import airbyte_list_connections, airbyte_sync
from paradime.cli.integrations.fivetran import fivetran_list_connectors, fivetran_sync
from paradime.cli.integrations.montecarlo import montecarlo_artifacts_import
from paradime.cli.integrations.power_bi import power_bi_list_datasets, power_bi_refresh
from paradime.cli.integrations.tableau import (
    tableau_list_datasources,
    tableau_list_workbooks,
    tableau_refresh,
)

help_string: Final = (
    "\nTo set environment variables please go to https://app.paradime.io/settings/env-variables"
)


@click.group(context_settings=dict(max_content_width=160))
def run() -> None:
    """
    Run predefined code runs to automate your workflows.
    """
    pass


run.add_command(tableau_refresh)
run.add_command(tableau_list_workbooks)
run.add_command(tableau_list_datasources)
run.add_command(power_bi_refresh)
run.add_command(power_bi_list_datasets)
run.add_command(fivetran_sync)
run.add_command(fivetran_list_connectors)
run.add_command(airbyte_sync)
run.add_command(airbyte_list_connections)
run.add_command(adf_pipelines)
run.add_command(adf_list_pipelines)
run.add_command(montecarlo_artifacts_import)
