# Real-Time Data Pipeline

End-to-end real-time data pipeline: Kafka → PySpark Structured Streaming → Delta Lake → AWS S3 (Gold layer) → PostgreSQL analytics.

## Architecture
```
Kafka Events → PySpark Streaming → Delta Lake (local) → AWS S3 Gold → PostgreSQL
                                                        ↓
                                              Prometheus Metrics
```

## Tech Stack
`Apache Kafka` `PySpark` `Delta Lake` `AWS S3` `AWS Glue` `Terraform` `PostgreSQL` `Prometheus` `Python` `Bash`

## Quick Start

```bash
# 1. Install Python dependencies
pip install -r requirements.txt

# 2. Deploy AWS infrastructure
cd terraform && terraform init && terraform apply

# 3. Set up PostgreSQL schema
psql -U postgres -f database/schema.sql

# 4. Run local demo (no Kafka needed)
python pipeline/local_pipeline.py

# 5. Full pipeline with Kafka
python producer/event_producer.py        # terminal 1
python pipeline/spark_streaming.py       # terminal 2

# 6. Export metrics
python monitoring/metrics_exporter.py
```

## Project Structure
```
realtime-data-pipeline/
├── terraform/               # AWS S3, Glue, IAM, CloudWatch
├── producer/                # Kafka event producer
├── pipeline/
│   ├── spark_streaming.py   # PySpark structured streaming
│   ├── delta_processor.py   # Delta Lake read/write/merge
│   └── local_pipeline.py    # full pipeline, runs locally
├── database/
│   └── schema.sql           # PostgreSQL analytics schema
├── monitoring/
│   ├── prometheus.yml       # Prometheus config
│   └── metrics_exporter.py  # custom metrics
└── scripts/
    ├── setup.sh             # environment bootstrap
    └── upload_to_s3.sh      # uploads gold layer to S3
```
