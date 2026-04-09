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
![Prefect](https://img.shields.io/badge/Prefect-Orchestration-070E10?style=for-the-badge&logo=prefect&logoColor=white)
![Looker](https://img.shields.io/badge/Looker%20Studio-Dashboard-4285F4?style=for-the-badge&logo=looker&logoColor=white)
![GitHub Codespaces](https://img.shields.io/badge/Codespaces-Environment-181717?style=for-the-badge&logo=github&logoColor=white)

![Architecture](assets/archi_flow_two.png)

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
---

## 🛠️ Tech Stack

- **Cloud Platform:** Google Cloud Platform (GCP)
- **Infrastructure as Code (IaC):** Terraform
- **Data Lake:** Google Cloud Storage (GCS)
- **Data Warehouse:** BigQuery
- **Transformation Layer:** dbt (data build tool)
- **Workflow Orchestration:** Prefect
- **Programming Language:** Python
- **BI Tool:** Looker Studio
- **Environment:** GitHub Codespaces

---

---

## 🙏 Acknowledgements
This project was submitted as capstone project to 
[DataTalks.Club Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) 
— a free, project-based bootcamp for aspiring data engineers.

---

## 📋 Prerequisites
- Google Cloud Platform account with billing enabled
- Kaggle account + API key (to download the dataset)
- Python 3.11+
- Terraform installed
- Prefect installed

---

## 🚀 How to Run this Project

**1. Clone the repository**

```bash
git clone https://github.com/GaniuKuku/ecommerce-data-engineering-project.git
cd ecommerce-data-engineering-project
```

**2. Provision Cloud Infrastructure**

Ensure you have GCP credentials configured in your environment, then run:
```bash
cd terraform
terraform init
terraform apply
```

**3. Run the Data Pipeline**

Install dependencies and execute the Prefect orchestrator to move data from local CSVs → GCS → BigQuery → dbt:
```bash
pip install -r requirements.txt
python orchestrate.py
```

**4. View the Pipeline DAG**

Spin up the Prefect UI to view the task execution graph:
```bash
prefect server start
```






















