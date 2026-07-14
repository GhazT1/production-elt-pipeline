# Netflix Data Analytics Pipeline

A production-grade ELT pipeline processing Netflix content data using the modern data stack: **Apache Airflow → AWS S3 → Snowflake → dbt**.

## Architecture

```
S3 (credits.csv, titles.csv)
        │
        ▼  [S3KeySensor — reschedule mode, 6hr SLA]
  Apache Airflow
        │
        ▼  [PythonOperator — transactional load]
 Snowflake DBT_RAW
   ├── TITLES_RAW
   └── CREDITS_RAW
        │
        ▼  [BashOperator — dbt run tag:STAGE]
 Snowflake DBT_STAGE
   ├── SHOW_DETAILS_STAGE
   ├── CREDITS_STAGE
   └── SCORES_VOTES_STAGE
        │
        ▼  [BashOperator — dbt run tag:FACT]
 Snowflake DBT_TRANSFORM
   ├── POPULARITY_FACT          (incremental, merge)
   ├── ACTORS_DOMINATING_FACT   (incremental, merge)
   └── CONTENT_TYPE_SHARE_FACT  (incremental, merge)
        │
        ▼  [BashOperator — dbt test --store-failures]
   Test results → Snowflake audit table
        │
        ▼  [SlackWebhookOperator]
   Slack success notification

   ══ Any failure ══▶ Slack alert + SNS topic (parallel)
```

## Repository structure

```
├── airflow/
│   └── dags/
│       ├── netflix_analytics.py        # Main DAG
│       ├── source_load/
│       │   └── data_load.py            # S3 → Snowflake loader
│       └── alerting/
│           └── slack_alert.py          # Slack webhook helpers
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml.template           # Fill and copy to ~/.dbt/
│   └── models/netflix/
│       ├── stage/                      # Raw → clean staging tables
│       ├── fact/                       # Analytical fact tables
│       └── dimension/                  # (extend here)
├── .env.example                        # Copy to .env, fill in values
├── .gitignore
└── .github/workflows/ci.yml            # Lint + dbt compile on every PR
```

## Security model

| Secret | Where it lives |
|--------|---------------|
| Snowflake username/password | AWS SSM Parameter Store (SecureString) |
| Snowflake account identifier | AWS SSM Parameter Store |
| Slack webhook token | Airflow Connection (id: `slack_default`) |
| SNS topic ARN | Environment variable (injected by ECS/EC2 at runtime) |
| AWS credentials | EC2 instance role — no static keys |

**Nothing sensitive is hardcoded or committed.**

## Quick start

### Prerequisites
- Python 3.11+
- Apache Airflow 2.7+
- Snowflake account
- AWS account (S3 bucket + SSM parameters configured)

### 1. Clone and configure
```bash
git clone <repo>
cd netflix-pipeline
cp .env.example .env
# Edit .env — fill in non-secret values; put secrets in SSM
```

### 2. Install Airflow dependencies
```bash
pip install apache-airflow \
    apache-airflow-providers-amazon \
    apache-airflow-providers-slack \
    snowflake-connector-python \
    pandas boto3
```

### 3. Set up dbt
```bash
pip install dbt-snowflake
cp dbt/profiles.yml.template ~/.dbt/profiles.yml
# profiles.yml reads from env vars — set SF_ACCOUNT, SF_USER, SF_PASSWORD
```

### 4. Add Airflow connections
In the Airflow UI:
- **aws_default** — AWS credentials (or use IAM role on EC2)
- **slack_default** — HTTP connection; set the Slack webhook URL as password

### 5. Deploy DAG
```bash
cp airflow/dags/* $AIRFLOW_HOME/dags/
airflow dags test netflix_analytics $(date +%Y-%m-%d)
```

## dbt model lineage

```
TITLES_RAW  ─┬─▶ SHOW_DETAILS_STAGE ─┬─▶ POPULARITY_FACT
             │                        ├─▶ ACTORS_DOMINATING_FACT
             └─▶ SCORES_VOTES_STAGE ──┘   CONTENT_TYPE_SHARE_FACT
CREDITS_RAW ────▶ CREDITS_STAGE ──────────▶ ACTORS_DOMINATING_FACT
```

All stage→fact references use `{{ ref() }}`. All raw→stage references use `{{ source() }}`. dbt owns the complete lineage graph.

## dbt tests

Every model has schema tests. Run them:
```bash
cd dbt
dbt test --store-failures   # failures written to Snowflake audit tables
```

Tests cover: `unique`, `not_null`, `accepted_values` (TYPE ∈ {MOVIE, SHOW}, ROLE ∈ {ACTOR, DIRECTOR}), and `dbt_utils.accepted_range` on scores and years.

## CI/CD

Every pull request runs:
1. **ruff** — Python linting
2. **pytest** — unit tests
3. **dbt compile** — SQL syntax validation (no Snowflake connection needed)

Merges to `main` trigger deployment via your CI/CD platform (GitHub Actions, AWS CodePipeline, etc.).

## What this demonstrates

- **Modern data stack** proficiency (Airflow + Snowflake + dbt)
- **Security best practices** — zero hardcoded secrets, SSM + IAM roles
- **Transactional data loading** — truncate + load wrapped in one Snowflake transaction with rollback on failure
- **Incremental dbt models** — efficient merge strategy, not full rebuilds
- **Observability** — structured logging, Slack alerts, SNS for programmatic subscribers, dbt test failures stored for audit
- **Production DAG hygiene** — `catchup=False`, `mode='reschedule'` sensors, execution timeouts, exponential retry backoff
- **CI/CD pipeline** — linting + dbt compile gate on every PR
