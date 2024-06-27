from setuptools import find_packages, setup

setup(
    name="dagster_config",
    version="0.0.1",
    packages=find_packages(),
    package_data={
        "dagster_config": [
            "dbt-project/**/*",
        ],
    },
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-dbt",
        "dbt-databricks<1.9",
        "dbt-databricks<1.9",
        "dbt-databricks<1.9",
        "dbt-databricks<1.9",
        "dbt-databricks<1.9",
        "dbt-databricks<1.9",
        "dbt-databricks<1.9",
        "dbt-databricks<1.9",
        "dbt-databricks<1.9",
    ],
    extras_require={
        "dev": [
            "dagster-webserver",
        ]
    },
)

