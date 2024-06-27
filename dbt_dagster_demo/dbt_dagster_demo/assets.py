from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

from .project import lake_modeling_project


@dbt_assets(manifest=lake_modeling_project.manifest_path)
def lake_modeling_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
    

