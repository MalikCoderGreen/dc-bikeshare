# DC Bikeshare Analytics Pipeline

A production-grade data pipeline that ingests, transforms, and analyzes Washington DC bikeshare trip data using Apache Spark, Delta Lake, and Databricks Asset Bundles (DAB). The pipeline implements a medallion architecture (bronze/silver/gold) with full CI/CD automation and multi-environment deployment.

---

## 📊 Project Overview

This project processes one year of Washington DC bikeshare trip metadata, tracking:

- Start and end station information
- Trip duration and distance calculations
- Rider type classification (member vs casual)
- Temporal patterns (day of week, time of day)
- Geographic patterns (station popularity, route analysis)

Data flows from AWS S3 through Databricks using Spark Structured Streaming, transformed via medallion architecture, and stored as Delta tables for analytics.

---

## 🏗️ Architecture

### Multi-Environment Setup

| Environment | Region    | Workspace           | Catalog             | S3 Bucket                        | Purpose                   |
| ----------- | --------- | ------------------- | ------------------- | -------------------------------- | ------------------------- |
| **Dev**     | us-west-1 | `dbc-b4813f44-2c67` | `dev_bikeshare`     | `s3://dc-bikeshare-data-dev`     | Development and testing   |
| **Staging** | us-east-1 | `dbc-b9fdb8d4-ebaa` | `staging_bikeshare` | `s3://dc-bikeshare-data-staging` | Pre-production validation |
| **Prod**    | us-east-1 | `dbc-1886d4dd-39a5` | `prod_bikeshare`    | `s3://dc-bikeshare-data`         | Production analytics      |

### Medallion Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Raw S3 Data                            │
│              s3://{env}-bikeshare/raw/                      │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│  BRONZE LAYER (Raw Ingestion)                               │
│  - Auto Loader (Spark Structured Streaming)                 │
│  - Schema inference and evolution                           │
│  - CSV → Delta Lake conversion                              │
│  Table: {catalog}.bronze.dc_rideshare_bt                    │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│  SILVER LAYER (Cleansed & Enriched)                         │
│  - Data quality checks                                      │
│  - Trip distance calculation (Haversine formula)            │
│  - Duration computation                                     │
│  - Trip type classification (round trip vs one-way)         │
│  - Temporal features (day of week, hour)                    │
│  Table: {catalog}.silver.dc_rideshare_cleaned               │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│  GOLD LAYER (Analytics-Ready Aggregations)                  │
│  - Station popularity metrics                               │
│  - Temporal usage patterns                                  │
│  - Route analysis                                           │
│  - Member vs casual comparisons                             │
│  Tables: {catalog}.gold.fact_rides_summary                  │
│          {catalog}.gold.station_metrics                     │
└─────────────────────────────────────────────────────────────┘
```

### IAM & Security

**Storage Credentials (Unity Catalog):**

- Each environment has its own AWS IAM role
- Separate storage credentials per environment:
  - `dev-bikeshare-storage-cred` → IAM role for dev bucket access
  - `staging-bikeshare-storage-cred` → IAM role for staging bucket access
  - `prod-bikeshare-storage-cred` → IAM role for prod bucket access
- External locations defined per environment for governed data access

**S3 Access Pattern:**

```
Unity Catalog External Location
    ↓ (uses)
Storage Credential (IAM Role)
    ↓ (grants access to)
S3 Bucket (environment-specific)
```

---

## 🛠️ Technology Stack

### Core Technologies

- **Data Processing:** Apache Spark 3.5.0 (Databricks Runtime 14.3 LTS)
- **Storage Format:** Delta Lake 3.0+
- **Orchestration:** Databricks Asset Bundles (DAB) 0.277.0
- **Data Governance:** Unity Catalog
- **Cloud Storage:** AWS S3
- **Streaming:** Spark Structured Streaming with Auto Loader

### Development & CI/CD

- **Language:** Python 3.11
- **Testing:** pytest 7.4.3, PySpark 3.5.0
- **Version Control:** Git/GitHub
- **CI/CD:** GitHub Actions
- **CLI:** Databricks CLI 0.277.0

### Infrastructure as Code

- **Deployment:** Databricks Asset Bundles (YAML)
- **Parameterization:** dbutils.widgets + DAB base_parameters
- **Environment Management:** Separate targets (dev/staging/prod)

---

## 📁 Project Structure

```
dc-bikeshare/
├── .github/
│   └── workflows/
│       ├── test.yml                 # Unit tests on PR
│       ├── deploy-dev.yml           # Auto-deploy to dev
│       ├── deploy-staging.yml       # Auto-deploy to staging (after dev)
│       └── deploy-prod.yml          # Manual approval deploy to prod
├── src/
│   ├── bikeshare_bronze_nb.py       # Bronze layer ingestion
│   ├── bikeshare_silver_nb.py       # Silver layer transformations
│   └── bikeshare_gold_nb.py         # Gold layer aggregations
├── tests/
│   ├── conftest.py                  # Pytest fixtures (local Spark)
│   └── test_transformations.py      # Unit tests
├── databricks.yml                   # DAB configuration
├── requirements.txt                 # Python dependencies
└── README.md
```

---

## 🚀 CI/CD Pipeline

### Workflow Overview

```
┌─────────────────────────────────────────────────────────────┐
│  Developer Workflow                                          │
└─────────────────────────────────────────────────────────────┘

1. Create Feature Branch
   git checkout -b feature/new-transformation

2. Make Changes & Test Locally
   pytest tests/ -v

3. Commit & Push
   git push origin feature/new-transformation

4. Open Pull Request to 'dev'
   ↓
┌─────────────────────────────────────────────────────────────┐
│  GitHub Actions: Run Tests                                   │
│  - Lint code                                                 │
│  - Run pytest unit tests                                     │
│  - Validate DAB configuration                                │
└─────────────────────────────────────────────────────────────┘
   ↓ (tests pass)

5. Merge PR to 'dev' branch
   ↓
┌─────────────────────────────────────────────────────────────┐
│  GitHub Actions: Deploy to Dev                               │
│  - Install Databricks CLI                                    │
│  - Authenticate to dev workspace                             │
│  - Run: databricks bundle deploy --target dev                │
│  - Execute pipeline: databricks bundle run bikeshare_etl     │
└─────────────────────────────────────────────────────────────┘
   ↓ (dev succeeds)
┌─────────────────────────────────────────────────────────────┐
│  GitHub Actions: Deploy to Staging (Sequential)              │
│  - Triggered by dev workflow completion                      │
│  - Deploy to staging workspace                               │
│  - Run integration tests                                     │
│  - Validate data quality                                     │
└─────────────────────────────────────────────────────────────┘
   ↓ (staging succeeds)

6. Open Pull Request from 'dev' → 'main'
   - Code review required
   - Approval from team lead

7. Merge to 'main' branch
   ↓
┌─────────────────────────────────────────────────────────────┐
│  GitHub Actions: Deploy to Production                        │
│  - Workflow pauses for manual approval ⏸️                   │
│  - Requires approval from: green.leek47@gmail.com            │
│  - After approval: deploy to prod workspace                  │
│  - Execute production pipeline                               │
│  - Send success notifications                                │
└─────────────────────────────────────────────────────────────┘
```

### Deployment Gates

- **Dev:** Automatic on merge to dev branch
- **Staging:** Automatic after dev succeeds
- **Production:** Requires manual approval via GitHub Environment

---

## 🧪 Testing Strategy

### Unit Tests (Local)

```bash
# Run all tests
pytest tests/ -v

# Run specific test
pytest tests/test_transformations.py::test_trip_type_classification -v

# Run with coverage
pytest tests/ --cov=src --cov-report=html
```

**Test Coverage:**

- Trip type classification (round trip vs one-way)
- Data quality flag logic (suspicious durations)
- Haversine distance calculations
- Temporal feature extraction (weekend detection)

### Integration Tests (Staging)

- End-to-end pipeline execution
- Data quality assertions on silver/gold tables
- Cross-layer consistency checks
- Performance benchmarks

---

### Project dependencies

- Python 3.11+
- Databricks CLI 0.277.0+
- AWS credentials (for S3 access)
- Databricks workspace access

---

## 🔧 Configuration

### Environment-Specific Variables

All environment-specific configuration is defined in `databricks.yml`:

```yaml
targets:
  dev:
    mode: development
    workspace:
      host: https://dbc-b4813f44-2c67.cloud.databricks.com
    variables:
      catalog: dev_bikeshare
      source_bucket: s3://dc-bikeshare-data-dev

  staging:
    mode: production
    workspace:
      host: https://dbc-b9fdb8d4-ebaa.cloud.databricks.com
      root_path: /Workspace/Users/green.malik5@gmail.com/.bundle/${bundle.name}/${bundle.target}
    variables:
      catalog: staging_bikeshare
      source_bucket: s3://dc-bikeshare-data-staging

  prod:
    mode: production
    workspace:
      host: https://dbc-1886d4dd-39a5.cloud.databricks.com
      root_path: /Workspace/Users/green.malik5@gmail.com/.bundle/${bundle.name}/${bundle.target}
    variables:
      catalog: prod_bikeshare
      source_bucket: s3://dc-bikeshare-data
```

### Notebook Parameterization

All notebooks use `dbutils.widgets` for parameterization:

```python
# Parameters - overridden by DAB base_parameters
dbutils.widgets.text("catalog", "OVERRIDE_ME")
dbutils.widgets.text("bronze_schema", "bronze")
dbutils.widgets.text("source_bucket", "s3://OVERRIDE_ME")

# Retrieved at runtime
catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
source_bucket = dbutils.widgets.get("source_bucket")
```

DAB injects environment-specific values via `base_parameters` in the job definition.

---

## 🚨 Current Challenges & Known Issues

### 1. DAB Sync Cache Corruption (Critical - Unresolved)

**Symptom:**

- DAB persistently deploys stale notebook versions despite file changes
- Cache clearing procedures ineffective:
  - `databricks bundle destroy` doesn't fix it
  - Deleting local `.databricks/` cache doesn't fix it
  - Deleting remote `.bundle/` workspace directory doesn't fix it
  - Deleting `sync-snapshots/` metadata doesn't fix it

**Observed Behavior:**

- Local files are updated and committed
- GitHub repository shows updated code
- After deployment, old non-parameterized notebooks still appear in workspace
- DAB's sync mechanism incorrectly reports "no changes detected"

**Current Workaround:**

- Rename notebook files (e.g., `bikeshare_silver.py` → `bikeshare_silver_nb.py`)
- DAB treats renamed files as "new" and uploads them correctly
- **Limitation:** This is not viable if file naming conventions are strict

**Root Cause:**

- Unknown. Likely a bug in DAB's file sync mechanism or workspace-level caching
- Appears to persist across DAB versions and workspace configurations
- May be related to timestamp comparison vs content hashing

**Impact:**

- Risk of deploying stale code in production if cache corruption occurs
- No reliable fix exists for forcing re-upload without renaming files
- Requires vigilance during deployments to verify correct versions are deployed

**Potential Mitigations:**

1. Always verify deployed notebook content in workspace UI after deployment
2. Include content checksums in deployment logs for verification
3. Consider scripted cache clearing before critical deployments:
   ```bash
   rm -rf .databricks/
   databricks workspace delete /Workspace/Users/{user}/.bundle/{bundle} --recursive
   databricks bundle deploy --target prod
   ```
4. File a detailed bug report with Databricks support

**Status:** Open issue. Workaround functional but not ideal. Investigating further.

---

### 2. Production Mode Root Path Requirement

**Issue:**
When using `mode: production` in DAB targets, `workspace.root_path` must be explicitly set.

**Error Message:**

```
Error: target with 'mode: production' must set 'workspace.root_path' to make sure only one copy is deployed
```

**Resolution:**
Add `root_path` to all production targets:

```yaml
staging:
  mode: production
  workspace:
    root_path: /Workspace/Users/green.malik5@gmail.com/.bundle/${bundle.name}/${bundle.target}
```

**Design Rationale:**

- Prevents multiple users/CI systems from overwriting each other's deployments
- Enforces explicit path declaration for production safety
- Development mode is more permissive, production mode has guardrails

---

## 📈 Future Enhancements

- [ ] Add Great Expectations for comprehensive data quality checks
- [ ] Implement job scheduling (daily at 2 AM ET)
- [ ] Add Slack/Teams notifications for job failures
- [ ] Create data quality dashboard in Databricks SQL
- [ ] Implement incremental processing for cost optimization
- [ ] Add rollback capability with Git tags
- [ ] Expand gold layer with additional analytics tables
- [ ] Add performance benchmarking tests

---

## 📝 Contributing

1. Create a feature branch from `dev`
2. Make changes and add tests
3. Ensure all tests pass locally: `pytest tests/ -v`
4. Open a pull request to `dev` branch
5. Wait for automated tests to pass
6. Request code review
7. After approval, merge to `dev` (auto-deploys to dev & staging)
8. For production release, create PR from `dev` to `main`

---

## 📄 License

This is a portfolio project for demonstration purposes. Please contact the maintainer for usage permissions.

---

## 🙏 Acknowledgments

- Built with [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/)
- Uses [Unity Catalog](https://www.databricks.com/product/unity-catalog) for data governance
- CI/CD powered by [GitHub Actions](https://github.com/features/actions)
- Data source: [Capital Bikeshare System Data](https://capitalbikeshare.com/system-data)

---

## 📞 Contact

**Maintainer:** Malik Green  
**Email:** green.malik5@gmail.com  
**GitHub:** [@MalikCoderGreen](https://github.com/MalikCoderGreen)

---

**Last Updated:** February 2026  
**DAB Version:** 0.277.0  
**Python Version:** 3.11  
**Spark Version:** 3.5.0 (Databricks Runtime 14.3 LTS)
