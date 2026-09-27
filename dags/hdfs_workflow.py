from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

HDFS_DIR = "/user/student/cli_lab"
LOCAL_FILE = "/tmp/cli_test.csv"
HDFS_FILE = f"{HDFS_DIR}/cli_test.csv"

with DAG(
    dag_id="student_cli_lab",
    description="Generate a CSV, upload it to HDFS, and verify the content",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["hdfs", "cli-lab"],
) as dag:

    generate_data = BashOperator(
        task_id="generate_data",
        bash_command=(
            "set -euo pipefail; "
            f"printf 'id,status\\n1,success\\n' > {LOCAL_FILE}; "
            "echo 'Generated local file:'; "
            f"cat {LOCAL_FILE}"
        ),
    )

    create_hdfs_folder = BashOperator(
        task_id="create_hdfs_folder",
        bash_command=(
            "set -euo pipefail; "
            f"hdfs dfs -mkdir -p {HDFS_DIR}"
        ),
    )

    upload_data = BashOperator(
        task_id="upload_data",
        bash_command=(
            "set -euo pipefail; "
            f"hdfs dfs -put -f {LOCAL_FILE} {HDFS_FILE}"
        ),
    )

    verify_data = BashOperator(
        task_id="verify_data",
        bash_command=(
            "set -euo pipefail; "
            f"hdfs dfs -test -e {HDFS_FILE}; "
            "echo 'Verified HDFS file:'; "
            f"hdfs dfs -cat {HDFS_FILE}"
        ),
    )

    [generate_data, create_hdfs_folder] >> upload_data >> verify_data

