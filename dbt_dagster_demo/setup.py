from setuptools import find_packages, setup

setup(
    name="dbt_dagster_demo",
    version="0.0.1",
    packages=find_packages(),
    package_data={
        "dbt_dagster_demo": [
            "dbt-project/**/*",
        ],
    },
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-dbt",
        "dbt-databricks<1.9",
        "dbt-databricks<1.9",
    ],
    extras_require={
        "dev": [
            "dagster-webserver",
        ]
    },
)

