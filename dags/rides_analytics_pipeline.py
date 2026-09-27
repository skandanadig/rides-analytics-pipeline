from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

BASE_DIR = "/home/seed/rides-analytics"
HADOOP_HOME = "/home/seed/hadoop-3.3.6"
SEATUNNEL_HOME = "/home/seed/apache-seatunnel"
HDFS_BASE = "/user/student/rides_pipeline"

COMMON_ENV = f"""
export HADOOP_HOME={HADOOP_HOME}
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
"""

with DAG(
    dag_id="rides_analytics_pipeline",
    description="Rides analytics batch pipeline",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["rides", "hdfs", "seatunnel", "mapreduce"],
) as dag:

    seatunnel_transform = BashOperator(
        task_id="seatunnel_transform",
        bash_command=f"""
        set -euo pipefail
        {COMMON_ENV}
        hdfs dfs -rm -r -f {HDFS_BASE}/clean_trips
        {SEATUNNEL_HOME}/bin/seatunnel.sh --config \
          {BASE_DIR}/seatunnel/clean_load.conf -m local
        """,
    )

    mr_avg_speed_by_city = BashOperator(
        task_id="mr_avg_speed_by_city",
        bash_command=f"""
        set -euo pipefail
        {COMMON_ENV}
        hdfs dfs -rm -r -f {HDFS_BASE}/avg_speed
        hadoop jar {BASE_DIR}/rides-analytics.jar AvgSpeedByCity \
          {HDFS_BASE}/clean_trips {HDFS_BASE}/avg_speed
        """,
    )

    mr_avg_fare_per_km_by_city = BashOperator(
        task_id="mr_avg_fare_per_km_by_city",
        bash_command=f"""
        set -euo pipefail
        {COMMON_ENV}
        hdfs dfs -rm -r -f {HDFS_BASE}/avg_fare_per_km
        hadoop jar {BASE_DIR}/rides-analytics.jar AvgFarePerKmByCity \
          {HDFS_BASE}/clean_trips {HDFS_BASE}/avg_fare_per_km
        """,
    )

    route_to_local = BashOperator(
        task_id="route_to_local",
        bash_command=f"""
        set -euo pipefail
        {COMMON_ENV}

        rm -f {BASE_DIR}/output/*.csv

        {SEATUNNEL_HOME}/bin/seatunnel.sh --config \
          {BASE_DIR}/seatunnel/costly.conf -m local
        {SEATUNNEL_HOME}/bin/seatunnel.sh --config \
          {BASE_DIR}/seatunnel/cheap.conf -m local
        {SEATUNNEL_HOME}/bin/seatunnel.sh --config \
          {BASE_DIR}/seatunnel/lowtraffic.conf -m local
        {SEATUNNEL_HOME}/bin/seatunnel.sh --config \
          {BASE_DIR}/seatunnel/hightraffic.conf -m local

        cp {BASE_DIR}/output/*_costly_0.csv {BASE_DIR}/output/costly.csv
        cp {BASE_DIR}/output/*_cheap_0.csv {BASE_DIR}/output/cheap.csv
        cp {BASE_DIR}/output/*_lowtraffic_0.csv {BASE_DIR}/output/lowtraffic.csv
        cp {BASE_DIR}/output/*_hightraffic_0.csv {BASE_DIR}/output/hightraffic.csv
        """,
    )

    (
        seatunnel_transform
        >> mr_avg_speed_by_city
        >> mr_avg_fare_per_km_by_city
        >> route_to_local
    )
