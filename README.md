# 🛒 Olist E-Commerce Data Lakehouse

![Python](https://img.shields.io/badge/Python-3.14-blue)
![PySpark](https://img.shields.io/badge/PySpark-3.5-orange)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.0-cyan)
![Docker](https://img.shields.io/badge/Containerization-Docker-2496ED)

An end-to-end Data Lakehouse pipeline built with **PySpark** and **Docker**, transforming raw Olist E-Commerce data into a Star Schema (Gold Layer) via a Medallion Architecture.

This project demonstrates a production-grade ETL pipeline that handles data ingestion, cleaning, deduplication with **Delta Lake**, and dimensional modeling for analytics.


## 🏗️ Architecture

The pipeline follows the **Medallion Architecture** (Bronze $\to$ Silver $\to$ Gold):

| Layer | Type | Format | Description |
| :--- | :--- | :--- | :--- |
| **Bronze** | Raw Ingestion | `Delta` | Raw data ingested directly from Kaggle using `kagglehub`. |
| **Silver** | Cleaned | `Delta` | Data cleaning, schema enforcement, and deduplication (Upserts). |
| **Gold** | Business | `Delta` | **Star Schema** (Fact/Dimensions) optimized for BI tools like PowerBI/Tableau. |


## 🛠️ Tech Stack

* **Language:** Python 3.14
* **Processing Engine:** Apache Spark (PySpark)
* **Storage Format:** Delta Lake (ACID Transactions, Time Travel)
* **Orchestration:** Custom Python Pipeline (Linear DAG)
* **Containerization:** Docker & Docker Compose
* **Data Source:** [Olist Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)


## 📂 Project Structure

```
├── data/                   # Local storage for data (mounted in Docker)
├── src/
│   ├── download_data.py    # Download data from Kaggle
│   ├── ingest_bronze.py    # Converts CSV to Delta table
│   ├── clean_silver.py     # Cleans & Dedupes (Delta Upsert)
│   ├── model_gold.py       # Models Star Schema
├── utils/
│   ├── config.py           # Paths & Global variables
│   ├── sparksession.py     # Spark Config with Delta Extensions
│   └── logger.py           # Logging function
├── main.py                 # Main orchestrator
├── Dockerfile              # Container definition
├── docker-compose.yml      # Service orchestration
├── requirements.txt        # Python dependencies
└── README.md               # Project Documentation
```

## 🚀 How to Run
**1. Clone the repository**

```bash
git clone https://github.com/PalmChalan/E-Commerce_Lakehouse.git
```

**2. Run the pipeline**

This command builds the Docker container and executes the full ETL pipeline (Download $\to$ Bronze $\to$ Silver $\to$ Gold).

```bash
docker compose run etl python main.py
```

**3. Check the results**

After the pipeline finishes, check the data/gold/ folder. You will see the final Delta tables:
- fact_sales
- fact_payments
- fact_reviews
- dim_products
- dim_customers
- dim_sellers

## 📊 Data Modeling (Gold Layer)
The Gold layer is modeled as a **Star Schema** to enable high-performance reporting.

- Fact Tables:

    - fact_sales: Transactional data (order_id, price, freight_value) linked to dimensions.
    - fact_payments: Payment details (payment_type, installments, payment_value).
    - fact_reviews: Customer feedback (review_score, comment_message, review_creation_date).

- Dimension Tables:

    - dim_products: Product categories and details.
    - dim_customers: Customer demographics and location.
    - dim_sellers: Seller information.
    - dim_dates: Date dimension for temporal analysis.

## 📈 Future Improvements
- [ ] Add Apache Airflow for complex scheduling.
- [ ] Implement Data Quality Tests.
- [ ] Build a Streamlit Dashboard to visualize the Gold data.

## Author
### [Chalantorn Chuamkaew](https://www.linkedin.com/in/palmchalan/)