from typing import Final

import click

from paradime.cli.integrations.adf import adf_list_pipelines, adf_pipelines
from paradime.cli.integrations.airbyte import airbyte_list_connections, airbyte_sync
from paradime.cli.integrations.airflow import airflow_list_dags, airflow_trigger
from paradime.cli.integrations.aws_glue import (
    aws_glue_list_jobs,
    aws_glue_list_workflows,
    aws_glue_trigger_jobs,
    aws_glue_trigger_workflows,
)
from paradime.cli.integrations.aws_lambda import aws_lambda_list, aws_lambda_trigger
from paradime.cli.integrations.aws_sagemaker import aws_sagemaker_list, aws_sagemaker_trigger
from paradime.cli.integrations.aws_stepfunctions import (
    aws_stepfunctions_list,
    aws_stepfunctions_trigger,
)
from paradime.cli.integrations.census import census_list_syncs, census_sync
from paradime.cli.integrations.fivetran import fivetran_list_connectors, fivetran_sync
from paradime.cli.integrations.hex import hex_list_projects, hex_trigger
from paradime.cli.integrations.hightouch import (
    hightouch_list_sync_sequences,
    hightouch_list_syncs,
    hightouch_sync,
    hightouch_sync_sequence,
)
from paradime.cli.integrations.matillion import matillion_list_pipelines, matillion_pipeline
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
run.add_command(airflow_trigger)
run.add_command(airflow_list_dags)
run.add_command(aws_glue_trigger_workflows)
run.add_command(aws_glue_trigger_jobs)
run.add_command(aws_glue_list_workflows)
run.add_command(aws_glue_list_jobs)
run.add_command(aws_lambda_trigger)
run.add_command(aws_lambda_list)
run.add_command(aws_sagemaker_trigger)
run.add_command(aws_sagemaker_list)
run.add_command(aws_stepfunctions_trigger)
run.add_command(aws_stepfunctions_list)
run.add_command(census_sync)
run.add_command(census_list_syncs)
run.add_command(adf_pipelines)
run.add_command(adf_list_pipelines)
run.add_command(hex_trigger)
run.add_command(hex_list_projects)
run.add_command(montecarlo_artifacts_import)
run.add_command(hightouch_sync)
run.add_command(hightouch_sync_sequence)
run.add_command(hightouch_list_syncs)
run.add_command(hightouch_list_sync_sequences)
run.add_command(matillion_pipeline)
run.add_command(matillion_list_pipelines)
