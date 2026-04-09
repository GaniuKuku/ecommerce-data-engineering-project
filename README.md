# 🛒 Olist E-Commerce Data Engineering Project (v2.0 - Production Architecture)

**📌 Version Notice:** This is the `main` branch representing Version 2, focusing on serverless cloud deployment, automated CI/CD pipelines, hardened code standards, and infrastructure security. 

*To view the original local Proof-of-Concept, check out the [**VERSION 1**](https://github.com/GaniuKuku/ecommerce-data-engineering-project/tree/archive-v1) branch.*

**Serverless Modern Data Stack using GCP, Cloud Run, Terraform, Prefect Cloud, BigQuery, dbt, GitHub Actions & Looker Studio**

![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=for-the-badge&logo=python&logoColor=white)
![GCP](https://img.shields.io/badge/GCP-Cloud-4285F4?style=for-the-badge&logo=googlecloud&logoColor=white)
![Google Cloud Run](https://img.shields.io/badge/Cloud_Run-Serverless-4285F4?style=for-the-badge&logo=googlecloud&logoColor=white)
![Terraform](https://img.shields.io/badge/Terraform-IaC-7B42BC?style=for-the-badge&logo=terraform&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Containerization-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-CI/CD-2088FF?style=for-the-badge&logo=github-actions&logoColor=white)
![BigQuery](https://img.shields.io/badge/BigQuery-Data%20Warehouse-4285F4?style=for-the-badge&logo=googlebigquery&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-Transformations-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![Prefect Cloud](https://img.shields.io/badge/Prefect-Orchestration-070E10?style=for-the-badge&logo=prefect&logoColor=white)
![Looker](https://img.shields.io/badge/Looker%20Studio-Dashboard-4285F4?style=for-the-badge&logo=looker&logoColor=white)
![GitHub Codespaces](https://img.shields.io/badge/Codespaces-Environment-181717?style=for-the-badge&logo=github&logoColor=white)

![Architecture](assets/archi_flow_two.png)

### 🎯 Business Impact Driven by Engineering
By upgrading to a highly scalable, serverless architecture, the pipeline was able to process the complete 2019 dataset (a 3x volume increase). This scale unlocked a critical business reality that smaller samples missed: despite driving **$60.08M** in total revenue and maintaining a **98% delivery success rate**, the business is facing a severe retention crisis, with over **95% of customers making only a single purchase.**

## 🚀 The V2 Upgrade: From Local Script to Serverless Cloud
Version 1 successfully moved data, but Version 2 was engineered for production. Following a rigorous code review, the entire codebase was refactored to meet enterprise software engineering and DevOps standards to handle a 3x scale-up in data volume (processing 2019 data).

### 🏗️ Infrastructure as Code (Terraform) Hardening
* **Security First:** Implemented strict IAM policies and enforced public access prevention on all storage buckets.
* **State Management:** Migrated from local state to remote backend state management for team-ready deployment.
* **Reusability & Safety:** Replaced hardcoded variables with dynamic `tfvars` and removed dangerous `force_destroy = true` flags to prevent accidental production data loss.
* **Cost Tracking:** Implemented comprehensive resource tagging across all GCP assets.

### ⚙️ Serverless DevOps & Automated CI/CD
* **Continuous Delivery Pipeline (New):** Implemented a fully automated CI/CD pipeline using **GitHub Actions**. Every pull request and push to the `main` branch undergoes a strict two-stage validation:
  1. **Infrastructure Check:** Terraform plans are verified against GCP to ensure resource integrity.
  2. **Data Contract Testing:** A `dbt build` is executed to ensure new code doesn't break existing financial models or critical data quality tests.
* **Containerization:** Only upon passing all tests is the Python execution code and dbt core packaged into a Docker container and delivered securely to Google Artifact Registry.
* **Zero-Ops Compute:** Deployed to Google Cloud Run, moving from a static machine to a highly scalable, serverless environment that bills only per millisecond of execution.
* **Cloud Orchestration:** Shifted from a local SQLite Prefect database to Prefect Cloud for remote observability, scheduling, and UI-based pipeline tracking.

### 🧹 Code Quality & Data Governance
* **Python Resiliency:** Replaced basic print statements with a production logger, added comprehensive function docstrings, and implemented strict `try/except` error handling.
* **Historical State Tracking (dbt Snapshots):** Implemented Slowly Changing Dimensions (SCD Type 2) via dbt snapshots to track the historical state of the `order_status` column over time, allowing the business to analyze order lifecycle bottlenecks.
* **Custom Data Contracts (Singular Tests):** Engineered advanced SQL tests to audit business logic (e.g., ensuring delivery dates always occur after purchase dates). Strategically configured financial discrepancy checks with `severity = 'warn'`. This ensures minor data anomalies (like sub-$2 float rounding differences) are flagged for the analytics team without hard-failing the automated CI/CD deployment.
* **dbt Best Practices:** Eliminated lazy `SELECT *` queries. Introduced config blocks inside models for precise materialization control, and expanded the `sources.yml` definitions for robust lineage tracking.

## 🧩 System Design Decisions & Trade-offs
When upgrading to Version 2, several architectural choices were made to optimize for scalability, cost, and maintainability:

* **Cloud Run vs. Airflow/Compute Engine:** Traditional orchestration engines like Airflow require "always-on" infrastructure, incurring passive costs. By decoupling orchestration (Prefect) from execution (Cloud Run), the pipeline achieves a true scale-to-zero serverless footprint, billing only for exact milliseconds of execution time.
* **Prefect vs. Cron Jobs:** While a simple cron job could trigger the script, Prefect Cloud was chosen to provide critical observability. It offers built-in state tracking, automated retries for transient network failures, and UI-based logging without managing backend databases.
* **dbt vs. Pure SQL Views:** dbt was implemented to bring software engineering principles to the data warehouse. It provides automatic lineage graphs, DRY (Don't Repeat Yourself) modularity via macros, and automated data quality testing that pure SQL lacks.
* **CI/CD enforcing Data Contracts:** The GitHub Actions pipeline isn't just for deployment; it is a defensive gatekeeper. If a financial discrepancy test fails during the build step, the deployment is automatically blocked, preventing bad code from reaching the production BigQuery environment.

## 🛡️ Reliability & Engineering Depth
To ensure enterprise-grade resilience, the pipeline incorporates the following data engineering patterns:

* **Idempotency:** The pipeline is fully idempotent. Executing the Prefect workflow multiple times for the same time window will not duplicate records. The dbt Silver/Gold layers utilize incremental models and merge strategies to safely upsert data.
* **Partitioning & Cost Optimization:** To optimize Looker Studio query costs, the primary BigQuery fact tables (`fct_orders`) are partitioned by `order_purchase_timestamp`. This drastically reduces the data scanned during dashboard date-range filtering.
* **Failure Handling Strategy:**
  * **Transient API Failures:** Prefect tasks interacting with external APIs are configured with retries to survive temporary network hiccups.
  * **Data Quality Breaches:** Custom singular tests use dynamic severities (`warn` for analytics visibility vs. `error` for hard fails).

---

## 📊 Dashboard & Business Insights (Scaled Dataset)

![Dashboard](assets/v_two.png)

🔗 **Live Dashboard:** [View on Looker Studio](https://lookerstudio.google.com/reporting/a8f06a74-485c-45e9-9554-3c5b36d7746e)

**1. Explosive Growth, But the Retention Crisis Remains 🚨**
* **Insight:** Out of 386,051 total orders, an overwhelming 367,558 belong to customers who made only a single purchase.
* **Impact:** The business can acquire customers at an incredible scale, but Lifetime Value (LTV) is bleeding out.
* **Recommendation:** Implement automated 30-day post-purchase retargeting campaigns to convert the massive pool of one-time buyers.

**2. Logistics is a True Superpower 🚚**
* **Insight:** As volume scaled 4x, on-time and early deliveries actually improved from 91.9% to a staggering 98%.
* **Impact:** The operational foundation is incredibly resilient.
* **Recommendation:** Make this 98% on-time delivery rate a core pillar of customer acquisition marketing.

**3. A Shift in Lifestyle Categories 🛋️**
* **Insight:** `sports_leisure` surged past tech products to become the #3 highest-grossing category, sitting right behind `bed_bath_table` and `health_beauty`.
* **Impact:** The customer base is leaning heavily into personal wellness and home goods.
* **Recommendation:** Shift ad spend and inventory forecasting to capitalize on the momentum of the sports and leisure category.

**4. Basket Size is Shrinking 🛒**
* **Insight:** Despite the revenue surge, Average Order Value (AOV) dipped to $155.63, and the average items per order dropped to an almost flat 1.04.
* **Impact:** Customers are buying strictly what they came for with virtually no cross-selling happening at checkout.
* **Recommendation:** Deploy "Frequently Bought Together" UI modules and introduce free shipping thresholds at $200.
---

## 🛠️ Tech Stack

- **Cloud Platform:** Google Cloud Platform (GCP)
- **Serverless Compute (New):** Google Cloud Run
- **Container Registry (New):** Google Artifact Registry
- **CI/CD Pipeline (New):** GitHub Actions
- **Containerization (New):** Docker
- **Infrastructure as Code (IaC):** Terraform
- **Data Lake:** Google Cloud Storage (GCS)
- **Data Warehouse:** BigQuery
- **Transformation Layer:** dbt (data build tool)
- **Workflow Orchestration (Upgraded):** Prefect Cloud
- **Programming Language:** Python 3.11+
- **BI Tool:** Looker Studio
- **Development Environment:** GitHub Codespaces

---

## 📋 Prerequisites
- **Google Cloud Platform** account (with billing enabled)
- **GitHub Account** (to fork the repo and run GitHub Actions/Codespaces)
- **Prefect Cloud** workspace (Free tier works perfectly)
- **Kaggle Account + API Key** (to download the raw Olist dataset)
- **Docker Desktop** installed (for any local container testing)
- **Terraform** installed
- **Python 3.11+** ```

---

## 🚀 How to Run this Project

**Step 1. Clone the repository**

```bash
git clone [https://github.com/GaniuKuku/ecommerce-data-engineering-project.git]
```

**Step 2: Set up GitHub Secrets & Variables**

To allow GitHub Actions to securely build your infrastructure and containerize your code, go to your forked repository's Settings > Secrets and variables > Actions and add the following:

Repository Secrets:

GCP_CREDENTIALS: Your Google Cloud Service Account JSON key.

Repository Variables:

GCP_PROJECT_ID: Your exact GCP Project ID.

GCP_REGION: e.g., us-central1.

GCP_BUCKET_NAME: Your chosen globally unique Cloud Storage bucket name.

GCP_DATASET_ID: Your BigQuery dataset name.

**Step 3: Trigger the CI/CD Pipeline**

Make any change to the codebase (or simply trigger a manual commit) and push to the main branch.

```bash
git commit --allow-empty -m "trigger: initial pipeline delivery"
git push origin main
```

**Step 4: Verify the Delivery**

Open the Actions tab in GitHub to watch the runner automatically test your Terraform plans, validate your dbt models, and build the Docker container.

Once the pipeline turns green, log into Google Cloud Console.

Navigate to Artifact Registry. You will see your production-ready Docker image (olist-pipeline:latest) safely stored and ready to be deployed to Cloud Run or triggered via an orchestrator (like Prefect) whenever a data run is required.






















