from pathlib import Path

from dagster_dbt import DbtProject

lake_modeling_project = DbtProject(
    project_dir=Path(__file__).joinpath("..", "..", "..").resolve(),
    packaged_project_dir=Path(__file__).joinpath("..", "..", "dbt-project").resolve(),
)
lake_modeling_project.prepare_if_dev()

