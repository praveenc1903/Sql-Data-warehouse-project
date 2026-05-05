# SQL Data Warehouse Project

A modern data warehouse built with SQL Server, implementing **Medallion Architecture** (Bronze → Silver → Gold) to consolidate CRM and ERP sales data into a star schema for business analytics and reporting.

---

## Business Problem

Companies relying on manual reporting from disconnected systems face slow turnaround, inconsistent data, and poor decision-making. This project solves that by building a centralised data warehouse that integrates two source systems (CRM and ERP), cleans and standardises the data, and delivers a single source of truth for sales, product, and customer analytics.

**What this enables:**
- Revenue analysis by product line, category, and region
- Customer segmentation by demographics and purchasing behaviour
- Sales performance tracking across orders, quantities, and pricing trends
- Data-driven decision-making with accurate, consistent, and auditable reporting

---

## Architecture

The project follows a three-layer **Medallion Architecture**:

```
  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
  │   BRONZE    │───▶│   SILVER    │────▶│    GOLD     │
  │  Raw Data   │     │  Cleansed   │     │  Star Schema│
  │  (Tables)   │     │  (Tables)   │     │  (Views)    │
  └─────────────┘     └─────────────┘     └─────────────┘
        ▲                                        │
   CSV Files                              BI / Reporting
  (CRM + ERP)                           (Power BI, Excel)
```

| Layer | Purpose | Objects | Load Method |
|---|---|---|---|
| **Bronze** | Store raw data exactly as received from source systems | Tables | Full load (truncate & insert) |
| **Silver** | Cleanse, standardise, and normalise data for consistency | Tables | Stored procedures with transformations |
| **Gold** | Business-ready star schema optimised for analytics | Views | Joins across silver tables with surrogate keys |

---

## Data Model (Star Schema)

The Gold layer implements a **star schema** with two dimension tables and one fact table:

```
                    ┌──────────────────┐
                    │  dim_customers   │
                    ├──────────────────┤
                    │ customer_key (PK)│
                    │ customer_id      │
                    │ first_name       │
                    │ last_name        │
                    │ country          │
                    │ gender           │
                    │ birthdate        │
                    └────────┬─────────┘
                             │
┌──────────────────┐         │         ┌──────────────────┐
│  dim_products    │         │         │   fact_sales     │
├──────────────────┤         │         ├──────────────────┤
│ product_key (PK) │◄────────┼───────▶│ order_number     │
│ product_name     │         └───────▶│ product_key (FK) │
│ category         │                   │ customer_key (FK)│
│ subcategory      │                   │ order_date       │
│ product_line     │                   │ sales_amount     │
│ cost             │                   │ quantity         │
└──────────────────┘                   │ price            │
                                       └──────────────────┘
```

---

## Source Systems

Data is ingested from two operational systems provided as CSV files:

**CRM (Customer Relationship Management)**
- `crm_cust_info` — Customer master data (names, gender, marital status)
- `crm_prd_info` — Product details (names, costs, product lines)
- `crm_sales_details` — Transactional sales records (orders, quantities, prices)

**ERP (Enterprise Resource Planning)**
- `erp_cust_az12` — Additional customer demographics (birthdate, gender fallback)
- `erp_loc_a101` — Customer location/geography data
- `erp_px_cat_g1v2` — Product category hierarchy (category, subcategory, maintenance)

---

## Key Data Engineering Decisions

**Surrogate Keys**
Generated via `ROW_NUMBER()` in gold dimension views to decouple the warehouse from source system IDs and ensure referential integrity.

**Gender Resolution Logic**
CRM is the master source for customer gender. When CRM returns `'n/a'`, the pipeline falls back to the ERP gender field using `CASE` + `COALESCE`.

**Active Products Only**
The product dimension filters on `prd_end_dt IS NULL` to exclude historical/discontinued products, keeping the reporting layer focused on current inventory.

**Left Joins for Completeness**
All dimension-to-ERP joins use `LEFT JOIN` to preserve every CRM record even when ERP data is missing — preventing silent data loss.

**Data Quality Checks**
Validation scripts in `tests/` verify data integrity across layers: null checks, duplicate detection, referential integrity between fact and dimension tables, and business rule validation.

---

## Project Structure

```
Sql-Data-warehouse-project/
│
├── datasets/                    # Raw CSV source files (CRM + ERP)
│
├── diagrams/                    # Architecture and data model visuals
│   ├── Data_flow_diagram.drawio.png
│   ├── Entity Relationship Diagram.drawio.png
│   └── data_model for gold layer.drawio.png
│
├── docs/                        # Documentation
│   └── data_catalog_gold.md     # Column-level catalog for gold layer
│
├── scripts/
│   ├── init_database.sql        # Database and schema initialisation
│   ├── bronze/                  # Raw data ingestion scripts
│   ├── silver/                  # Cleansing and transformation scripts
│   └── gold/                    # Star schema views (dimensions + fact)
│
├── tests/
│   ├── data cleaning silver.sql # Silver layer validation checks
│   └── data quality check gold.sql  # Gold layer quality assurance
│
└── README.md
```

---

## Tech Stack

| Tool | Purpose |
|---|---|
| **SQL Server** | Data warehouse platform |
| **T-SQL** | ETL transformations, stored procedures, views |
| **Draw.io** | Data architecture and ERD diagrams |
| **Git / GitHub** | Version control and project documentation |

---

## How to Run

1. **Clone the repository**
   ```bash
   git clone https://github.com/praveenc1903/Sql-Data-warehouse-project.git
   ```

2. **Set up the database**
   - Install SQL Server Express and SSMS
   - Run `scripts/init_database.sql` to create the database and schemas

3. **Load data layer by layer**
   - Execute scripts in `scripts/bronze/` to ingest raw CSVs
   - Execute scripts in `scripts/silver/` to cleanse and transform
   - Execute scripts in `scripts/gold/` to create star schema views

4. **Validate**
   - Run `tests/data cleaning silver.sql` and `tests/data quality check gold.sql` to verify data integrity

---

## Author

**Praveen C**
MSc Data Science & AI — Sheffield Hallam University
[LinkedIn](https://linkedin.com/in/praveenc1932) · 
