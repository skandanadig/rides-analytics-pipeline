# 🚌 Rides Analytics Pipeline — Big Data Assignment 1

**Course:** Big Data (UE24CS343AB2) · 5th Semester  
**Assignment:** Batch pipeline for ride-hailing trip analytics across 10 Indian cities

---

## 1 · Pipeline Overview

```
trips.csv  →  SeaTunnel (clean + load)  →  HDFS /clean_trips
                                               │
                        ┌──────────────────────┤
                        ▼                      ▼
             MapReduce: avg speed      MapReduce: avg fare/km
             (/avg_speed)              (/avg_fare_per_km)
                        │                      │
                        └──────────────────────┘
                                    │
                               SeaTunnel (route)
                    ┌──────────────┼──────────────┐
                    ▼              ▼               ▼              ▼
               costly.csv     cheap.csv    lowtraffic.csv  hightraffic.csv
```

All four stages run as tasks in **one Airflow DAG** (`rides_analytics_pipeline`), triggered manually once.  
Nothing is simulated — SeaTunnel genuinely reads/writes HDFS; MapReduce jobs run on YARN.

---

## 2 · Getting Started

> **Reference guides** for installing and configuring the tools used in this project are in the [`docs/`](docs/) folder:
>
> | Guide | Description |
> |-------|-------------|
> | [`docs/Airflow_Install_Guide.pdf`](docs/Airflow_Install_Guide.pdf) | Step-by-step Airflow installation and simple task setup |
> | [`docs/Apache_SeaTunnel_Guide.pdf`](docs/Apache_SeaTunnel_Guide.pdf) | Apache SeaTunnel setup and connector reference |

### Requirements

| Component | Version used | Install guide |
|-----------|-------------|---------------|
| **Java JDK** | 8 or later | `sudo apt install openjdk-8-jdk` |
| **Hadoop** (HDFS + YARN) | 3.3.6 | See SeaTunnel guide, Section: Hadoop setup |
| **Apache SeaTunnel** | 2.x | `docs/Apache_SeaTunnel_Guide.pdf` |
| **Apache Airflow** | 2.x | `docs/Airflow_Install_Guide.pdf` |
| **Python** | 3.8+ | Required by Airflow |

Verify everything is running before triggering the DAG:
```bash
jps
# Expected: NameNode, DataNode, ResourceManager, NodeManager, SecondaryNameNode
airflow version
$SEATUNNEL_HOME/bin/seatunnel.sh --version
```

---

## 3 · Repository Structure

```
rides-analytics-pipeline/
├── docs/
│   ├── Airflow_Install_Guide.pdf      # Airflow setup reference
│   └── Apache_SeaTunnel_Guide.pdf     # SeaTunnel setup reference
├── dags/
│   ├── rides_analytics_pipeline.py   # Main Airflow DAG (all 4 tasks)
│   └── hdfs_workflow.py              # HDFS CLI lab / smoke-test DAG
├── src/
│   ├── AvgSpeedByCity.java            # MapReduce Job 1 – avg speed
│   └── AvgFarePerKmByCity.java        # MapReduce Job 2 – avg fare/km
├── seatunnel/
│   ├── clean_load.conf                # Stage 1 – clean CSV → HDFS
│   ├── costly.conf                    # Stage 4 – fare/km > 17
│   ├── cheap.conf                     # Stage 4 – fare/km ≤ 17
│   ├── lowtraffic.conf                # Stage 4 – avg speed > 45
│   └── hightraffic.conf               # Stage 4 – avg speed ≤ 45
├── seatunnel-verification/
│   ├── employees.csv                  # Verification sample data
│   ├── students.csv                   # Verification sample data
│   ├── task1_local_console.conf       # SeaTunnel smoke test (local→console)
│   └── task2_local_hdfs.conf          # SeaTunnel smoke test (local→HDFS)
├── input/
│   └── trips.csv                      # Raw dataset (1 000 rows, 10 cities)
├── output/
│   ├── costly.csv                     # Cities with avg fare/km > 17
│   ├── cheap.csv                      # Cities with avg fare/km ≤ 17
│   ├── lowtraffic.csv                 # Cities with avg speed > 45 km/h
│   └── hightraffic.csv                # Cities with avg speed ≤ 45 km/h
├── rides-analytics.jar                # Compiled MapReduce JAR
└── README.md
```

---

## 4 · Quick-Start Deployment

### 4.1 Place the project

```bash
cp -r rides-analytics-pipeline/ /home/seed/rides-analytics/
```

The DAG and SeaTunnel configs hard-code `/home/seed/rides-analytics` as `BASE_DIR`.  
If your user differs, update `BASE_DIR`, `HADOOP_HOME`, and `SEATUNNEL_HOME` in  
`dags/rides_analytics_pipeline.py` and the `path` fields in every `.conf` file.

### 4.2 Create HDFS working directory

```bash
hdfs dfs -mkdir -p /user/student/rides_pipeline
```

### 4.3 Register the DAG with Airflow

```bash
cp dags/rides_analytics_pipeline.py $AIRFLOW_HOME/dags/
# wait ~30 s for the scheduler to pick it up, then:
airflow dags list | grep rides_analytics_pipeline
```

### 4.4 Trigger the DAG

```bash
airflow dags trigger rides_analytics_pipeline
```

Or use the Airflow web UI → **DAGs → rides_analytics_pipeline → Trigger DAG**.

---

## 5 · Stage-by-Stage Details

### Stage 1 — Clean & Load (SeaTunnel)

**Config:** `seatunnel/clean_load.conf`

Reads `input/trips.csv` and drops any row where:
- `distance_km` is missing or ≤ 0
- `fare` is missing or empty
- `dropoff_ts` is not strictly after `pickup_ts`

Surviving rows are written to **HDFS** at `/user/student/rides_pipeline/clean_trips`.

> **Observed run:** 1 000 rows read → **920 clean rows** written to HDFS.

### Stage 2 — Average Speed per City (MapReduce)

**Source:** `src/AvgSpeedByCity.java`

For every clean trip:

```
duration_min  = dropoff_ts − pickup_ts   (minutes)
speed_kmph    = distance_km / (duration_min / 60)
```

Reduces to one row per city: `city \t avg_speed_kmph`  
Output HDFS path: `/user/student/rides_pipeline/avg_speed`

### Stage 3 — Average Fare per km per City (MapReduce)

**Source:** `src/AvgFarePerKmByCity.java`

For every clean trip:

```
fare_per_km = fare / distance_km
```

Reduces to one row per city: `city \t avg_fare_per_km`  
Output HDFS path: `/user/student/rides_pipeline/avg_fare_per_km`

### Stage 4 — Route to 4 CSV Files (SeaTunnel)

**Configs:** `seatunnel/{costly,cheap,lowtraffic,hightraffic}.conf`

Fixed thresholds (same for every student):

| Threshold | Output file |
|-----------|-------------|
| `avg_fare_per_km > 17.0` | `output/costly.csv` |
| `avg_fare_per_km ≤ 17.0` | `output/cheap.csv` |
| `avg_speed_kmph > 45.0` | `output/lowtraffic.csv` |
| `avg_speed_kmph ≤ 45.0` | `output/hightraffic.csv` |

Each city appears in exactly one fare file and exactly one speed file.

---

## 6 · Airflow DAG

**File:** `dags/rides_analytics_pipeline.py`  
**DAG ID:** `rides_analytics_pipeline`

```
seatunnel_transform  →  mr_avg_speed_by_city  →  mr_avg_fare_per_km_by_city  →  route_to_local
```

- `schedule=None` — triggered manually once
- `max_active_runs=1` — prevents duplicate runs
- Each task cleans up its HDFS output directory before writing, so re-runs are idempotent

---

## 7 · Output Results

### costly.csv — avg fare/km > 17 (high-fare cities)

| City | avg_fare_per_km |
|------|----------------|
| Ahmedabad | 21.0599 |
| Chennai | 19.4493 |
| Delhi | 21.7812 |
| Hyderabad | 20.1916 |
| Jaipur | 21.8834 |
| Kolkata | 20.3489 |
| Lucknow | 20.6058 |
| Pune | 20.9817 |

### cheap.csv — avg fare/km ≤ 17 (low-fare cities)

| City | avg_fare_per_km |
|------|----------------|
| Bangalore | 13.3485 |
| Mumbai | 13.6367 |

### lowtraffic.csv — avg speed > 45 km/h (free-flowing cities)

| City | avg_speed_kmph |
|------|---------------|
| Ahmedabad | 52.6252 |
| Hyderabad | 57.7184 |
| Jaipur | 55.7652 |
| Mumbai | 60.9566 |

### hightraffic.csv — avg speed ≤ 45 km/h (congested cities)

| City | avg_speed_kmph |
|------|---------------|
| Bangalore | 36.4481 |
| Chennai | 37.5359 |
| Delhi | 30.2348 |
| Kolkata | 24.4685 |
| Lucknow | 25.0537 |
| Pune | 32.6754 |

---

## 8 · Evaluation Checklist

| Check | What the evaluator looks for |
|-------|------------------------------|
| ✅ Airflow DAG | `rides_analytics_pipeline` — all 4 tasks green |
| ✅ YARN | Both MapReduce applications: FINISHED / SUCCEEDED |
| ✅ HDFS NameNode | `/user/student/rides_pipeline/clean_trips`, `/avg_speed`, `/avg_fare_per_km` present |
| ✅ Output CSVs | `costly.csv`, `cheap.csv`, `lowtraffic.csv`, `hightraffic.csv` match dataset |
| ✅ Viva | Explain each stage and design choices |

---

## 9 · Marks Breakdown

| Component | Marks |
|-----------|-------|
| Airflow orchestration (DAG structure, full run success) | 2 |
| SeaTunnel Job 1 — clean and load to HDFS | 2 |
| MapReduce Job 1 — average speed per city | 1 |
| MapReduce Job 2 — average fare per km per city | 1 |
| SeaTunnel Job 2 — route to 4 CSV files | 2 |
| Viva | 2 |
| **Total** | **10** |

> Final gradebook entry = total above scaled to **5 marks**.

---

## 10 · Notes

- Stages 1 & 4 use genuine **SeaTunnel**; stages 2 & 3 use genuine **Hadoop MapReduce** — no substitutes.
- Thresholds `17.0` (fare) and `45.0` (speed) are fixed and must not be changed.
- Every student's dataset differs; your numbers will differ from classmates — that is expected.
- Re-running the DAG is safe: each task removes its previous output before writing new output.

---

*Dataset obtained from [https://shrivardhan10.github.io/Student_Dataset_Download/](https://shrivardhan10.github.io/Student_Dataset_Download/) using student SRN.*
