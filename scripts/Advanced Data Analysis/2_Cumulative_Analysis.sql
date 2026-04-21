USE DataWarehouse;

-- Advanced Data Analytics

-- 2. Cumulative Analysis
-- Purpose:
--		Aggregate measures over time
--      Explain How progress or decline our business over time
--		Runnning Total or Moving Average

-- Runnning Total Sales and Moving Average Sales by Month Analysis:
SELECT
	order_month,
	monthly_sales,
	ISNULL(CAST((CAST((monthly_sales - LAG(monthly_sales) OVER(ORDER BY order_month)) AS DECIMAL) / LAG(monthly_sales) OVER(ORDER BY order_month) * 100) AS DECIMAL(10,2)), 0)
	AS [total|progress(%)],
	SUM(monthly_sales) OVER(
			ORDER BY order_month
		) AS runnning_total_by_month,
	monthly_avg_sales,
	ISNULL(CAST((CAST((monthly_avg_sales - LAG(monthly_avg_sales) OVER(ORDER BY order_month)) AS DECIMAL) / LAG(monthly_avg_sales) OVER(ORDER BY order_month) * 100) AS DECIMAL(10,2)), 0)
	AS [avg|progress(%)],
	AVG(monthly_avg_sales) OVER(
			ORDER BY order_month
		) AS moving_average_by_month
FROM
(
	SELECT
		DATETRUNC(MONTH, order_date) AS order_month,
		SUM(sales) AS monthly_sales,
		AVG(sales) AS monthly_avg_sales
	FROM
		gold.fact_sales
	GROUP BY
		DATETRUNC(MONTH, order_date)
) AS T;