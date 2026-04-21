USE DataWarehouse;

-- 4. Part-to-Whole Analysis
-- Purpose:
--		How individual part impact overall business.
--		Show in category how impacct each part to whole.

-- Part-to-Whole (By Category)
-- with Subquery
SELECT
	category,
	category_sales,
	SUM(category_sales) OVER() AS total_sales,
	CAST((CAST(category_sales AS DECIMAL) / SUM(category_sales) OVER()) * 100 AS DECIMAL(10,2)) AS [part_to_whole(%)]
FROM
(
SELECT
	category,
	SUM(sales) AS category_sales
FROM
	gold.fact_sales AS s
	LEFT JOIN gold.dim_products AS p ON p.product_key = s.product_key
GROUP BY
	category
) AS T


-- Part-to-Whole (By Country)
-- with Distinct
SELECT DISTINCT
	country,
	SUM(sales) OVER(PARTITION BY country) AS country_sales,
	SUM(sales) OVER() AS total_sales,
	CAST((CAST(SUM(sales) OVER(PARTITION BY country) AS DECIMAL) / SUM(sales) OVER()) * 100 AS DECIMAL(10,2)) AS [part_to_whole(%)]
FROM
	gold.fact_sales AS s
	LEFT JOIN gold.dim_customers AS c ON c.customer_key = s.customer_key;