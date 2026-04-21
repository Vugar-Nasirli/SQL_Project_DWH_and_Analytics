USE DataWarehouse;

-- 5. Data Segmentation Analysis
-- Purpose:
--		Group up the data based on specific range.
--		Used to see correlation between two measures.

SELECT
	cost_category,
	COUNT(*) AS product_count
FROM
(
SELECT
	*,
	CASE
		WHEN cost > PERCENTILE_DISC(0.75) WITHIN GROUP(ORDER BY cost ASC) OVER() THEN 'Expensive'
		WHEN cost < PERCENTILE_DISC(0.25) WITHIN GROUP(ORDER BY cost ASC) OVER() THEN 'Cheap'
		ELSE 'Moderate'
	END AS cost_category
FROM
	gold.dim_products
) AS T
GROUP BY
	cost_category;


SELECT DISTINCT
	customer_segment,
	COUNT(*) OVER(PARTITION BY customer_segment) AS customer_count,
	COUNT(*) OVER() AS total_customers,
	ROUND((CAST(COUNT(*) OVER(PARTITION BY customer_segment) AS FLOAT) / COUNT(*) OVER()) * 100, 2) AS part_to_whole
FROM
(
SELECT
	s.customer_key,
	c.first_name,
	c.last_name,
	COUNT(*) AS total_customers,
	SUM(sales) AS total_sales,
	MIN(order_date) AS first_order,
	MAX(order_date) AS last_order,
	DATEDIFF(MONTH, MIN(order_date),MAX(order_date)) AS customer_history,
	CASE
		WHEN DATEDIFF(MONTH, MIN(order_date),MAX(order_date)) >= 12 AND SUM(sales) > 5000
		THEN 'VIP'
		WHEN DATEDIFF(MONTH, MIN(order_date),MAX(order_date)) >= 12 AND SUM(sales) <= 5000
		THEN 'Regular'
		ELSE 'New'
	END AS customer_segment
FROM
	gold.fact_sales AS s
	LEFT JOIN gold.dim_customers AS c ON c.customer_key = s.customer_key
GROUP BY
	s.customer_key,
	c.first_name,
	c.last_name
) AS T;