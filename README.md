# **Modern SQL Data Warehouse Project: CRM & ERP Integration | End-to-end Data Project with EDA (Exploratory Data Analysis) and ADA (Advanced Data Analysis)**

This project demonstrates the end-to-end process of building a **modern SQL Data Warehouse** from scratch using real-world data engineering practices. It follows the **Medallion Architecture** to transform raw CSV data from two source systems (CRM and ERP) into a business-ready **Star Schema** for analytics and reporting.

![DataWarehouse](/docs/DataWarehouse_Project.png)

---

## **Tools & Technologies**
*   **Database Engine:** SQL Server (Express Edition).
*   **SQL Client:** SQL Server Management Studio (SSMS).
*   **Project Management:** Notion (for epics, tasks, and progress tracking).
  [DWH Project Management](https://quick-spoonbill-3c9.notion.site/Data-Warehouse-Project-33facaabdda280beae6ef725f5ad5714?pvs=143)
*   **Design & Diagramming:** Draw.io (for architecture, data flow, and modeling diagrams).
*   **Version Control:** Git & GitHub (for code repository and portfolio showcase).

---

## **Data Warehouse Architecture**
The project implements a **Medallion Architecture** consisting of three distinct layers:
1.  **Bronze Layer:** Stores **raw, unprocessed data** exactly as it is received from the sources for traceability and debugging.
2.  **Silver Layer:** Contains **cleaned and standardized data** where basic transformations and quality fixes are applied.
3.  **Gold Layer:** Contains **business-ready data** modeled into a **Star Schema** (Facts and Dimensions) for end-user consumption.

---

## **Step-by-Step Project Implementation**

### **Step 1: Requirements Analysis & Planning**
*   **Requirement Gathering:** Identify data sources (CRM and ERP CSV files) and define the goal: a single "point of truth" for sales reporting.
*   **Project Planning:** Use **Notion** to break the project into "Epics" (e.g., Build Bronze Layer) and sub-tasks with progress bars.
*   **Naming Conventions:** Establish a **Snake Case** rule (e.g., `customer_info`) and define prefixes for technical columns (`dw_load_date`) and objects (`load_bronze`).

### **Step 2: Environment Initialization**
*   **Git Setup:** Create a GitHub repository with a standardized folder structure: `data_sets`, `docs`, `scripts`, and `tests`.
*   **Database Creation:** Execute SQL scripts to create the `Data_Warehouse` database and initialize the three schemas: `bronze`, `silver`, and `gold`.

### **Step 3: Building the Bronze Layer (Ingestion)**
*   **Source Analysis:** Interview source experts to understand data types, volume, and extraction methods.
*   **DDL Creation:** Define tables in the `bronze` schema that match the source CSV structure exactly.
*   **Data Ingestion:** Develop a stored procedure (`load_bronze`) that uses the **Bulk Insert** technique to load data from CSVs into SQL tables.
*   **Technique:** Implemented **Truncate and Insert** (Full Load) for refreshing data daily.

### **Step 4: Building the Silver Layer (Cleansing)**
*   **Data Profiling:** Detect quality issues such as duplicates, nulls, unwanted spaces, and invalid date ranges.
*   **Data Cleansing & Transformation:**
    *   **Trimming:** Removing leading/trailing spaces from strings.
    *   **Handling Missing Values:** Replacing nulls with "Not Available" or default values like `0`.
    *   **Normalization:** Mapping cryptic codes (e.g., 'M', 'F') to user-friendly names ('Male', 'Female').
    *   **Deduplication:** Using **Window Functions** (`ROW_NUMBER`) to identify and keep only the latest record based on a timestamp.
    *   **Invalid Date Handling:** Using `LEAD` functions to fix overlapping history dates.
*   **Metadata:** Add technical columns like `dw_create_date` to every table for auditing.

### **Step 5: Building the Gold Layer (Modeling)**
*   **Data Integration:** Merge related tables from CRM and ERP into unified business objects (e.g., combining two customer sources into one Dimension).
*   **Star Schema Design:**
    *   **Dimensions:** Created `dim_customers` and `dim_products` to store descriptive attributes.
    *   **Facts:** Created `fact_sales` to store quantitative measures and keys connecting to dimensions.
*   **Surrogate Keys:** Generate unique, system-produced identifiers (`customer_key`) using `ROW_NUMBER` to maintain control over the model independently of source systems.
*   **Data Lookup:** Replace source business keys in the Fact table with Surrogate Keys from the Dimensions.
*   **Object Type:** Use **SQL Views** for the Gold layer to keep the model virtual and dynamic.

--------------------------------------------------------------------------------------------------------------------------------------------------

### **1. Medallion Architecture Overview**

![DataWarehouse](/docs/DataWarehouse_Project.png)

Incorporate a section describing the functional design of the Data Warehouse.
*   **Layered Processing:** The project follows a three-tier architecture: **Bronze (Raw Data)**, **Silver (Cleaned/Standardized)**, and **Gold (Business Ready)**.
*   **Object Implementation:** 
    *   **Bronze & Silver:** Implemented as physical **Tables** refreshed via batch processing and "Truncate & Insert" full loads.
    *   **Gold:** Implemented as **SQL Views**, keeping the business layer virtual and dynamic without requiring separate storage for final transformations.
*   **Transformation Scope:** 
    *   The Silver layer handles **data cleansing, enrichment, and normalization** (e.g., mapping codes to friendly names).
    *   The Gold layer focuses on **integrations, aggregations, and business logic** to create final analytic products.

### **2. Data Lineage and Flow**

![Data Flow](/docs/Data_Flow_Diagram.png)

Explain how the source files move through the system as shown in the **Data Flow Diagram**.
*   **Source-to-Target Mapping:** Data originates from two sources (**CRM** and **ERP**) and maintains a one-to-one relationship through the Bronze and Silver layers before converging in the Gold layer.
*   **Multi-Source Consolidation:** The final **Gold dimensions** are formed by merging disparate source tables. For example:
    *   `dim_customers` integrates data from `crm_cust_info`, `erp_cust_az12`, and `erp_loc_a101`.
    *   `dim_products` integrates `crm_prd_info` and `erp_px_cat_g1v2`.

### **3. Data Integration Logic**

![Data Integration](/docs/Data_Integration_Diagram.png)

Describe the specific business relationships defined in the **Integration Diagram**.
*   **Relational Joins:**
    *   Sales transactions (`crm_sales_details`) are linked to customers via `cust_id` and products via `prd_key`.
*   **Supplemental ERP Attributes:** The ERP system is used to enrich the primary CRM data with:
    *   **Customer Details:** Adding birthdates and country locations.
    *   **Product Details:** Providing hierarchical categories and subcategories.

### **4. Gold Layer Star Schema Model**

![Data Mart Star Schema](/docs/Data_Mart_Star_Schema_Model.png)

Detail the final analytical model provided for business consumption.
*   **Central Fact Table:** `fact_sales` serves as the core, containing quantitative measures like **Sales Amount**, **Quantity**, and **Price**.
*   **Dimensions:**
    *   `dim_customers`: Provides descriptive attributes like name, country, gender, and marital status.
    *   `dim_products`: Contains product names, costs, lines, and category hierarchies.
*   **Surrogate Key Management:** The model uses system-generated **Surrogate Keys** (e.g., `customer_key`, `product_key`) to link the fact table to dimensions, ensuring independent control over the data model regardless of changes in source system business keys.
*   **Relationships:** The schema utilizes **One-to-Many** relationships where each dimension record corresponds to multiple transaction records in the fact table.

---

## **Quality Assurance & Documentation**
*   **Data Lineage:** Created visual diagrams in Draw.io to track data flow from the raw source files through each layer of the warehouse.
*   **Data Catalog:** Developed a detailed dictionary for the Gold layer, describing every column, data type, and providing example values for business users.
*   **Quality Checks:** Created a `tests` script to validate the integrity of the final model (e.g., checking for orphans where a Fact key doesn't exist in a Dimension).
*   **Naming Convention:** For Professional Engineering best practices I created file for controlling naming of each object within project.

***

# Data Analytics Part: From Exploration to Advanced Insights

## Project Overview
This project demonstrates a comprehensive SQL-based analytical workflow, transitioning from initial **Exploratory Data Analysis (EDA)** to **Advanced Data Analytics (ADA)**. The goal is to transform raw transactional data into actionable business intelligence by uncovering trends, segmenting customers, and generating complex reports for stakeholders.

## Technical Stack
*   **Language:** SQL (T-SQL/SQL Server)
*   **Tool:** SQL Server Management Studio (SSMS)
*   **Concepts:** Data Warehousing, Data Profiling, Window Functions, CTEs, and Data Modeling.

## Phase 1: Exploratory Data Analysis (EDA)
The first phase focuses on understanding the dataset's structure and uncovering basic insights through **Data Profiling**.

### 1. Database & Metadata Exploration
*   **Technique:** Querying system tables like `information_schema.tables` and `information_schema.columns` to map the database structure and identify table relationships.

### 2. Dimensions and Measures Framework
*   **Practice:** Categorizing data into **Dimensions** (descriptive attributes like country or category) and **Measures** (numeric values suitable for aggregation like sales or quantity).
*   **Technique:** Using `DISTINCT` to identify unique values and understand the cardinality of dimensions.

### 3. Date Boundaries & Time Span
*   **Technique:** Utilizing `MIN` and `MAX` on date columns to determine the business's time horizon (e.g., finding a 4-year range of sales).

### 4. Magnitude and Ranking Analysis
*   **Magnitude:** Comparing measures across dimensions (e.g., Total Revenue by Country) to identify key business drivers.
*   **Ranking:** Identifying top and bottom performers using the `TOP` keyword or **Ranking Window Functions** like `ROW_NUMBER()`.

---

## Phase 2: Advanced Data Analytics (ADA)
The second phase applies advanced SQL techniques to solve complex business questions and track performance over time.

### 1. Change Over Time & Seasonality
*   **Technique:** Using date functions (`YEAR`, `MONTH`, `DATETRUNC`) to track how metrics like total sales evolve, helping to identify seasonal patterns (e.g., peak sales in December).

### 2. Cumulative Analysis
*   **Technique:** Implementing **Aggregate Window Functions** to calculate "Running Totals" and "Moving Averages" to visualize business growth and progression.

### 3. Performance Analysis (Year-over-Year)
*   **Technique:** Using the `LAG()` value window function to compare current year sales against the previous year (YoY) or the average.
*   **Logic:** Applying `CASE WHEN` to flag performance as "Increase," "Decrease," or "Above/Below Average".

### 4. Part-to-Whole Analysis
*   **Technique:** Calculating the percentage contribution of individual categories to the overall total (e.g., finding that Bikes contribute ~69% of revenue).

### 5. Data Segmentation
*   **Practice:** Converting measures into new dimensions via `CASE WHEN` to group data into meaningful ranges.
*   **Examples:** Segmenting products into cost ranges (Low, Medium, High) and categorizing customers into "VIP," "Regular," or "New" based on their spending and lifespan.

---

## Phase 3: Strategic Reporting & KPIs
The final stage involves consolidating all metrics into a "Gold Layer" of views for stakeholders and visualization tools.

### Key Performance Indicators (KPIs) Developed:
*   **Recency:** Months since the customer's last order.
*   **Average Order Value (AOV):** Total Revenue divided by Total Orders.
*   **Average Monthly Spend:** Total Revenue divided by the customer's lifespan in months.

### Final Deliverable:
Created consolidated **SQL Views** (e.g., `gold.report_customers`, `gold.report_products`) to provide a 360-degree view of business entities, ready for immediate use in Tableau or PowerBI dashboards.

---

## How to Use This Project
1.  **Database Setup:** Execute the provided `init_database.sql` script to create the schema and load the data.
2.  **Scripts:** Navigate through the folders categorized by analysis type (EDA, Change Over Time, Segmentation, etc.).
3.  **Reports:** Run the "Report" scripts to generate the final analytical views.

***
