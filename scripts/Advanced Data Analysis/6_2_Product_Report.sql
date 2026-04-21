USE DataWarehouse;
GO

-- 6.2 Final Report (Product Report)
-- Purpose:
--		To Combine All Relalevant Data and Create Final Report.
--		Make Some Transformations and Relevant Aggregation to Data.
--		Bring Together all Essential Analysis.

-- Requirements:
--		This report consolidates key product metrices and behaviours
-- Highlights:
--		1. Gather essential data about customers, such as name, category, subcategory, cost, transactions
--		2. Segment customers into categories according to segment based on performancce
--		3. Aggregate customer level metrices:
--			- total profit
--			- total sales
--			- total orders
--			- total quantity sold
--			- total customers
--			- lifespan
--			- start year
--		4. Essential KPIs
--			- recency (last order till today)
--			- average order sales (AOV)
--			- average order profit
--			- average monthly sales
--			- average customer profit

-- 4) Create view
IF OBJECT_ID('gold.report_products', 'V') IS NOT NULL
	DROP VIEW gold.report_products;
GO
CREATE VIEW gold.report_products
AS
-- 1) Retriving core columns from tables
WITH base_query
AS
(
SELECT
	s.order_number,
	s.customer_key,
	s.order_date,
	s.sales,
	s.price,
	s.quantity,
	s.product_key,
	p.product_name,
	p.product_number,
	p.category,
	p.subcategory,
	YEAR(product_start_date) AS start_year,
	p.cost,
	(s.price - p.cost) * s.quantity AS profit
FROM
	gold.fact_sales AS s
	LEFT JOIN gold.dim_products AS p ON p.product_key = s.product_key
),

-- 2) Aggregating Customer Specific Details
product_aggregations
AS
(
SELECT
	product_key,
	product_number,
	product_name,
	category,
	subcategory,
	start_year,
	MAX(order_date) AS last_order_date,
	DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan,
	AVG(cost) AS avg_cost,
	AVG(price) AS avg_price,
	SUM(quantity) AS total_quantity,
	SUM(sales) AS total_sales,
	SUM(profit) AS total_profit,
	COUNT(DISTINCT order_number) AS total_orders,
	COUNT(DISTINCT customer_key) AS total_customers
FROM
	base_query
GROUP BY
	product_key,
	product_number,
	product_name,
	category,
	subcategory,
	start_year
)

-- 3) Prepare last Report Form and Essential Data
SELECT
	product_key,
	product_number,
	product_name,
	category,
	subcategory,
	start_year,
	last_order_date,
	lifespan,
	avg_cost,
	avg_price,
	total_quantity,
	CASE
		WHEN total_customers >= 1000 THEN 'Top-performer'
		WHEN total_customers >= 500 THEN 'Mid-performer'
		ELSE 'Low-performer'
	END AS performance_segment,
	CASE
		WHEN avg_cost >= 1000 THEN 'Expensive'
		WHEN avg_cost >= 400 THEN 'Moderate'
		ELSE 'Cheap'
	END AS cost_segment,
	total_customers,
	total_sales,
	total_profit,
	total_orders,
	ROUND(CAST(total_profit AS FLOAT) / total_customers, 0) AS avg_customer_profit,
	ROUND(CAST(total_profit AS FLOAT) / total_orders, 0) AS avg_order_profit,
	ROUND(CAST(total_sales AS FLOAT) / total_orders, 0) AS avg_order_sales,
	ROUND(CAST(total_sales AS FLOAT) / lifespan, 0) AS avg_monthly_sales,
	DATEDIFF(MONTH, last_order_date, GETDATE()) AS product_recency
FROM
	product_aggregations;


