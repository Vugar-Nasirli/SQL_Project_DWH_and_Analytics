USE DataWarehouse;
GO

-- 6.1 Final Report (Customer Report)
-- Purpose:
--		To Combine All Relalevant Data and Create Final Report.
--		Make Some Transformations and Relevant Aggregation to Data.
--		Bring Together all Essential Analysis.

-- Requirements:
--		This report consolidates key customer metrices and behaviours
-- Highlights:
--		1. Gather essential data about customers, such as name, age, customer behaviour according to transactions
--		2. Segment customers into categories according to different aspects: age groups, customer segments
--		3. Aggregate customer level metrices:
--			- total sales(ltv)
--			- total orders
--			- total quantity purchased
--			- total products
--			- lifespan
--			- cohort year
--		4. Essential KPIs
--			- recency (last order till today)
--			- average order value (AOV)
--			- average monthly spend


-- 4) Create view
IF OBJECT_ID('gold.report_customers', 'V') IS NOT NULL
	DROP VIEW gold.report_customers;
GO
CREATE VIEW gold.report_customers
AS
-- 1) Retriving core columns from tables
WITH base_query
AS
(
	SELECT
		s.order_number,
		s.product_key,
		s.order_date,
		s.sales,
		s.quantity,
		s.customer_key,
		c.customer_number,
		CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
		DATEDIFF(YEAR, birthdate, GETDATE()) AS age,
		c.country
	FROM
		gold.fact_sales AS s
		LEFT JOIN gold.dim_customers AS c ON c.customer_key = s.customer_key
),

-- 2) Aggregating Customer Specific Details
customer_aggregations
AS
(
	SELECT
		customer_key,
		customer_number,
		customer_name,
		country,
		age,
		SUM(sales) AS customer_ltv,
		COUNT(DISTINCT order_number) AS total_orders,
		COUNT(DISTINCT product_key) AS total_products,
		SUM(quantity) AS total_quantity,
		MIN(order_date) AS first_order_date,
		MAX(order_date) AS last_order_date,
		DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan
	FROM
		base_query
	GROUP BY
		customer_key,
		customer_number,
		customer_name,
		country,
		age
)

-- 3) Prepare last Report Form and Essential Data
SELECT
	customer_key,
	customer_number,
	customer_name,
	country,
	age,
	CASE
		WHEN age >= 50 THEN '50s and above'
		WHEN age >= 40 THEN '40s'
		WHEN age >= 30 THEN '30s'
		WHEN age >= 20 THEN '20s'
		ELSE 'Under 20'
	END AS age_group,
	first_order_date,
	YEAR(first_order_date) AS cohort_year,
	lifespan,
	customer_ltv,
	CASE
		WHEN lifespan >= 12 AND customer_ltv > 5000
		THEN 'VIP'
		WHEN lifespan >= 12 AND customer_ltv <= 5000
		THEN 'Regular'
		ELSE 'New'
	END AS customer_segment,
	total_orders,
	total_products,
	total_quantity,
	last_order_date,
	DATEDIFF(MONTH, last_order_date, GETDATE()) AS customer_recency,
	ROUND(CAST(customer_ltv AS FLOAT) / total_orders, 0) AS avg_order_value,
	CASE
		WHEN lifespan = 0 THEN customer_ltv
		ELSE ROUND(CAST(customer_ltv AS FLOAT) / lifespan, 0)
	END AS avg_monthly_spend
FROM
	customer_aggregations;