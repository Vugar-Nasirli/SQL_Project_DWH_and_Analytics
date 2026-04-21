USE DataWarehouse;

-- Advanced Data Analytics

-- 1. Changes Over Time Analysis
-- Purpose: 
--		How Measures evolves over Time.
--		Helps Track Trends and Seasonality of Data.

-- YoY (Year over Year) Anaylsis:
SELECT
	YEAR(order_date) AS order_year,
	COUNT(DISTINCT customer_key) AS total_customers,
	COUNT(DISTINCT order_number) AS total_orders,
	COUNT(DISTINCT s.product_key) AS total_product,
	SUM(sales) AS total_sales,
	SUM((price - cost) * quantity) AS total_profit
FROM
	gold.fact_sales AS s
	LEFT JOIN gold.dim_products AS p ON p.product_key = s.product_key
GROUP BY
	YEAR(order_date)
ORDER BY
	YEAR(order_date);

-- MoM (Month over Month) Analysis:
SELECT
	MONTH(order_date) AS [#],
	FORMAT(order_date, 'MMMM') AS order_month,
	COUNT(DISTINCT customer_key) AS total_customers,
	COUNT(DISTINCT order_number) AS total_orders,
	COUNT(DISTINCT s.product_key) AS total_product,
	SUM(sales) AS total_sales,
	SUM((price - cost) * quantity) AS total_profit
FROM
	gold.fact_sales AS s
	LEFT JOIN gold.dim_products AS p ON p.product_key = s.product_key
GROUP BY
	MONTH(order_date),
	FORMAT(order_date, 'MMMM')
ORDER BY
	MONTH(order_date);

-- Cohort Analysis (Yearly)
-- Retention Analysis (Yearly)
-- Churn Analysis (Yearly)
WITH cohorts
AS
(
	SELECT DISTINCT
		s.customer_key,
		c.first_name,
		c.last_name,
		YEAR(order_date) AS order_year,
		YEAR(MIN(order_date) OVER(PARTITION BY s.customer_key)) AS cohort_year
	FROM
		gold.fact_sales AS s
		LEFT JOIN gold.dim_customers AS c ON c.customer_key = s.customer_key
),
cohort_analysis
AS
(
	SELECT DISTINCT
		order_year,
		cohort_year,
		COUNT(customer_key) OVER(
				PARTITION BY
					order_year,
					cohort_year
			) AS cohort_customers,
		COUNT(customer_key) OVER(
				PARTITION BY
					order_year
			) AS total_customers,
		CAST(CAST(COUNT(customer_key) OVER(PARTITION BY order_year, cohort_year) AS DECIMAL) / COUNT(customer_key) OVER(PARTITION BY order_year) * 100 AS DECIMAL(10,2))  
		AS cohort_rate
	FROM
		cohorts
)

SELECT
	*,
	CAST((CAST(cohort_customers AS DECIMAL) / FIRST_VALUE(cohort_customers) OVER(PARTITION BY cohort_year ORDER BY order_year)) * 100 AS DECIMAL(10,2))
	AS retention_rate,
	CAST(100 - ((CAST(cohort_customers AS DECIMAL) / FIRST_VALUE(cohort_customers) OVER(PARTITION BY cohort_year ORDER BY order_year)) * 100) AS DECIMAL(10,2))
	AS churn_rate
FROM
	cohort_analysis
ORDER BY
	order_year,
	cohort_year;


