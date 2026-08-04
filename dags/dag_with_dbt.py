import os
from datetime import datetime
from airflow.decorators import dag
from cosmos import (DbtTaskGroup, ExecutionConfig, ExecutionMode, LoadMode, ProfileConfig, ProjectConfig, RenderConfig)
from cosmos.constants import InvocationMode, TestBehavior

default_args = {
    "start_date": datetime(2025, 4, 15),
    "end_date": datetime(2025, 4, 15),
}

dag_args = {
    "default_args": default_args,
    "schedule_interval": "@daily",
    "max_active_runs": 1,
    "max_active_tasks": 1,
}

@dag(dag_id="dag_with_dbt", **dag_args)
def dag_with_dbt():
    DbtTaskGroup(
        project_config=ProjectConfig(
            dbt_project_path="/opt/airflow/dbt/portfolio",
            project_name="portfolio",
        ),
        profile_config=ProfileConfig(
            profile_name="portfolio",
            target_name="dev",
            profiles_yml_filepath="/opt/airflow/.dbt/profiles.yml",
        ),
        render_config=RenderConfig(
            load_method=LoadMode.AUTOMATIC,
            test_behavior=TestBehavior.AFTER_EACH,
            select=["*"],
        ),
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.LOCAL,
            invocation_mode=InvocationMode.DBT_RUNNER,
        ),
        operator_args={
            "install_deps": True,
        },
    )

dag_with_dbt()


