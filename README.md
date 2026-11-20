# Real-Time-End-to-End_Data_Pipeline-Using-Kafka-Airflow-DBT-Snowflake

A banking data engineering project that demonstrates how customer, account and transaction data can be captured from PostgreSQL, processed through Kafka, stored in MinIO and transformed in Snowflake using dbt.

The project uses Python, Debezium, Apache Kafka, MinIO, Apache Airflow, Snowflake and dbt to implement a CDC-based data pipeline with batch-based file processing.

## Project Overview

Banking systems generate continuous changes to customer, account and transaction records. Extracting complete tables repeatedly can increase database load and processing time.

This project explores an alternative using Change Data Capture (CDC). Debezium captures database changes from PostgreSQL and publishes them to Kafka. A Python consumer processes the events into Parquet batches, which are stored in MinIO before being loaded into Snowflake for analytical transformations.

The source data is generated using Python and Faker to simulate banking transactions.

## Architecture

```text
       Python / Faker
              |
              ▼
        PostgreSQL 15
        (Source Database)
              |
              ▼
         Debezium CDC
              |
              ▼
         Apache Kafka
         (Change Events)
              |
              ▼
        Python Consumer
        (Batch Processing)
              |
              ▼
         MinIO (Parquet)
              |
              ▼
        Apache Airflow
        (Workflow Scheduling)
              |
              ▼
          Snowflake
        (Data Warehouse)
              |
              ▼
             dbt
       ├── Staging Models
       ├── Snapshots
       ├── Dimensions
       └── Fact Table
```

*The diagram represents the intended data flow. The complete integration requires the configuration and compatibility fixes described under Known Limitations.*

## Technologies

| Technology | Role in the Project |
|---|---|
| Python & Faker | Generates synthetic banking records |
| PostgreSQL 15 | Stores customer, account and transaction data |
| Debezium | Captures PostgreSQL changes using logical replication |
| Apache Kafka | Transports database change events |
| Python Consumer | Reads Kafka events and creates Parquet batches |
| MinIO | Stores Parquet files using S3-compatible storage |
| Apache Airflow | Schedules ingestion and dbt workflows |
| Snowflake | Stores data for analytical processing |
| dbt Core | Builds staging, snapshot, dimension and fact models |
| GitHub Actions | Automates configured dbt validation and execution workflows |
| Docker Compose | Runs the local infrastructure |

## Implementation

### 1. Source Data and Change Capture

- Created PostgreSQL tables for customers, accounts and transactions.
- Generated synthetic records using Python and Faker.
- Enabled PostgreSQL logical replication.
- Configured a Debezium connector using the `pgoutput` plugin.
- Published database change events to Kafka topics.

### 2. Kafka Processing and Storage

- Developed a Python consumer to read events from Kafka.
- Configured batch processing with a threshold of 50 records per topic.
- Converted extracted records into Parquet files.
- Stored the files in MinIO for downstream ingestion.

### 3. Data Loading and Orchestration

- Created Airflow DAGs for MinIO-to-Snowflake ingestion and dbt execution.
- Used Snowflake stages and `COPY INTO` for file loading.
- Configured scheduled ingestion and daily transformation workflows.

### 4. Data Transformation with dbt

Built staging and analytical models in dbt.

| Model | Description |
|---|---|
| `stg_customers` | Extracts and structures customer attributes |
| `stg_accounts` | Extracts and structures account attributes |
| `stg_transactions` | Structures transaction records |
| `customers_snapshot` | Tracks changes to customer attributes |
| `accounts_snapshot` | Tracks changes to account attributes |
| `dim_customers` | Builds the customer dimension with historical validity |
| `dim_accounts` | Builds the account dimension with historical validity |
| `fact_transactions` | Incrementally models transactions and associated customer information |

The snapshots use check-based change detection. The transaction fact model uses incremental materialization to avoid rebuilding the entire historical dataset on every run.

### 5. Workflow Automation

GitHub Actions workflows are defined for:

- Running linting and dbt compilation checks.
- Executing dbt models and tests against Snowflake.
- Managing warehouse credentials through GitHub Secrets.

## Technical Outcomes

- Established a CDC-based architecture connecting PostgreSQL, Debezium and Kafka.
- Defined a Kafka consumer for batch-based Parquet file generation.
- Used MinIO as an intermediate storage layer between event processing and Snowflake.
- Organized analytical transformations into staging, historical snapshots, dimensions and an incremental fact model.
- Defined Airflow workflows for scheduled ingestion and transformation.
- Added GitHub Actions workflows for dbt validation and execution.

## Repository Structure

```text
.
├── .github/
│   └── workflows/
├── initial_phase/
├── banking_dbt/
│   ├── models/
│   │   ├── staging/
│   │   └── marts/
│   └── snapshots/
├── consumer/
│   └── kafka_to_minio.py
├── data-generator/
│   └── faker_generator.py
├── docker/
│   └── dags/
├── kafka-debezium/
├── postgres/
├── docker-compose.yml
├── dockerfile-airflow.dockerfile
├── requirements.txt
├── .gitignore
└── LICENSE
```

## Setup and Execution

### Prerequisites

- Docker Desktop and Docker Compose
- Python 3.11+
- Snowflake account
- Required environment variables and credentials

### 1. Clone the Repository

```bash
git clone https://github.com/Subhajit-Chowdhury/Real-Time-End-to-End_Data_Pipeline-Using-Kafka-Airflow-DBT-Snowflake.git

cd Real-Time-End-to-End_Data_Pipeline-Using-Kafka-Airflow-DBT-Snowflake
```

### 2. Configure Environment Variables

Create the required local `.env` files for PostgreSQL, MinIO, Airflow, Kafka and Snowflake.

Configure the dbt Snowflake profile separately.

Do not commit passwords, access keys or other credentials.

### 3. Start Infrastructure

```bash
docker compose up -d --build
```

### 4. Install Dependencies

```bash
python -m venv .venv

# Windows
.venv\Scripts\activate

pip install -r requirements.txt
```

### 5. Initialize PostgreSQL

```bash
docker compose cp ./postgres/schema.sql postgres:/tmp/schema.sql

docker compose exec postgres sh -c \
'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /tmp/schema.sql'
```

### 6. Register the Debezium Connector

```bash
python kafka-debezium/generate_and_post_connector.py
```

### 7. Generate Synthetic Data

```bash
python data-generator/faker_generator.py --once
```

### 8. Start the Kafka Consumer

```bash
python consumer/kafka_to_minio.py
```

Configure Snowflake objects and the dbt profile before enabling the Airflow DAGs.

The data generator, connector registration script and Kafka consumer run as separate processes and are not started automatically by Docker Compose.

## Known Limitations

The following areas require additional implementation and testing:

- The consumer's expected Debezium event structure does not match the configured schema-less event format.
- The Parquet output schema does not match the VARIANT-based structure expected by the dbt staging models.
- The consumer does not flush partially filled batches on a timer.
- Delete events are not currently handled.
- MinIO object listing does not paginate results.
- Snapshot and mart tasks do not have an explicit execution dependency in the daily Airflow workflow.
- Snowflake raw-table definitions and the complete dbt connection configuration require setup.

These items need to be addressed before the complete pipeline can be executed successfully.

## Attribution and References

This project was inspired by **Jaya Chandra Kadiveti (Data with Jay)** and his banking data engineering tutorial.

**Original project:** [banking-modern-datastack](https://github.com/Jay61616/banking-modern-datastack)

**Reference tutorial:** [End-to-End Banking Data Engineering Project | Snowflake + DBT + Airflow](https://youtu.be/uHiyZitmIS0)

I used the original project and tutorial as a reference and developed my own implementation, configurations and modifications for this repository.

This repository is an independent learning and portfolio implementation. It is not an official version of the original project and is not affiliated with or endorsed by the original creator.

## License

This repository includes an MIT License. Any reused third-party code or materials remain subject to their original applicable copyright and license terms.
