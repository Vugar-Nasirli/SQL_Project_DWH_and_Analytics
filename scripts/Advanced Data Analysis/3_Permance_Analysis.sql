USE DataWarehouse;

-- 3. Permormance Analysis
-- Purpose:
--		Helps measure succes and compare performances
--		Compare current value to target value

-- YoY (Year over Year) Performance Analysis:
SELECT
	product_name,
	order_year,
	total_sales,
	ISNULL(CAST(CAST((total_sales - LAG(total_sales) OVER(PARTITION BY product_name ORDER BY order_year)) AS DECIMAL) / LAG(total_sales) OVER(PARTITION BY product_name ORDER BY order_year) * 100 AS DECIMAL(10,2)), 0)
	AS [total | progress(%)],
	CASE
		WHEN ISNULL(CAST(CAST((total_sales - LAG(total_sales) OVER(PARTITION BY product_name ORDER BY order_year)) AS DECIMAL) / LAG(total_sales) OVER(PARTITION BY product_name ORDER BY order_year) * 100 AS DECIMAL(10,2)), 0) < 0
		THEN 'Decreasing'
		WHEN ISNULL(CAST(CAST((total_sales - LAG(total_sales) OVER(PARTITION BY product_name ORDER BY order_year)) AS DECIMAL) / LAG(total_sales) OVER(PARTITION BY product_name ORDER BY order_year) * 100 AS DECIMAL(10,2)), 0) > 0
		THEN 'Increasing'
		ELSE 'No Change'
	END AS py_change_indicator,
	AVG(total_sales) OVER(PARTITION BY product_name) AS avg_sales,
	ISNULL(CAST(CAST((total_sales - AVG(total_sales) OVER(PARTITION BY product_name)) AS DECIMAL) / AVG(total_sales) OVER(PARTITION BY product_name) * 100 AS DECIMAL(10,2)), 0)
	AS [avg | progress(%)],
	CASE
		WHEN ISNULL(CAST(CAST((total_sales - AVG(total_sales) OVER(PARTITION BY product_name)) AS DECIMAL) / AVG(total_sales) OVER(PARTITION BY product_name) * 100 AS DECIMAL(10,2)), 0) < 0
		THEN 'Below Average'
		WHEN ISNULL(CAST(CAST((total_sales - AVG(total_sales) OVER(PARTITION BY product_name)) AS DECIMAL) / AVG(total_sales) OVER(PARTITION BY product_name) * 100 AS DECIMAL(10,2)), 0) > 0
		THEN 'Above Average'
		ELSE 'Average'
	END AS avg_change_indicator
FROM
(
	SELECT
		YEAR(s.order_date) AS order_year,
		p.product_name,
		SUM(sales) AS total_sales
	FROM
		gold.fact_sales AS s
		LEFT JOIN gold.dim_products AS p ON p.product_key = s.product_key
	GROUP BY
		YEAR(s.order_date),
		p.product_name
) AS T
ORDER BY
	COUNT(*) OVER(PARTITION BY product_name) DESC,
	product_name,
	order_year;