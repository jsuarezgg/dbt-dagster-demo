from dagster import Definitions
from dagster_dbt import DbtCliResource
from .assets import lake_modeling_dbt_assets
from .project import lake_modeling_project
from .schedules import schedules

defs = Definitions(
    assets=[lake_modeling_dbt_assets],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=lake_modeling_project),
    },
)

