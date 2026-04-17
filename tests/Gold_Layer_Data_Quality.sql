-- Quality Checks


-- Customers (Dimension)

-- 1. Full Data Check
SELECT * FROM gold.dim_customers;

-- 2. Surrogate Key Check
SELECT
	customer_key,
	COUNT(*)
FROM
	gold.dim_customers
GROUP BY
	customer_key
HAVING
	COUNT(*) > 1;  -- Result: none (Expected)

-- 3. Integrated Data Check
-- After Integration:
SELECT DISTINCT
	gender
FROM
	gold.dim_customers;

-- Before Integration and Integration Process

-- Data Integration When the same data available in two different source
SELECT DISTINCT
	ci.cst_gndr,
	cb.gen,
	CASE
		WHEN ci.cst_gndr = 'n/a' AND cb.gen IS NOT NULL THEN cb.gen
		ELSE ci.cst_gndr
	END AS gender
FROM
	silver.crm_cust_info AS ci
	LEFT JOIN silver.erp_cust_az12 AS cb ON cb.cid = ci.cst_key
	LEFT JOIN silver.erp_loc_a101 AS cl ON cl.cid = ci.cst_key;

-------------------------------------

-- Products (Dimension)

-- 1. Full Data Check
SELECT * FROM gold.dim_products;

-- 2. Surrogate Key Check
SELECT
	product_key,
	COUNT(*)
FROM
	gold.dim_products
GROUP BY
	product_key
HAVING
	COUNT(*) > 1; -- None

-- 3. Essential Key Check
SELECT
	product_number,
	COUNT(*)
FROM
	gold.dim_products
GROUP BY
	product_number
HAVING
	COUNT(*) > 1; -- None

---------------------------------

-- Sales (Fact)

-- 1. Full Data Check
SELECT * FROM gold.fact_sales;

-- 2. Essential Key Columns Check
SELECT
	order_number,
	product_key,
	customer_key,
	COUNT(*)
FROM
	gold.fact_sales
GROUP BY
	order_number,
	product_key
	customer_key
HAVING
	COUNT(*) > 1; -- Result: none


-- 3. Check for Table Integrity via Foreign Keys
SELECT
	*
FROM
	gold.fact_sales AS s
	LEFT JOIN gold.dim_customers AS c ON c.customer_key = s.customer_key
WHERE
	s.customer_key IS NULL;  -- Result: none


SELECT
	*
FROM
	gold.fact_sales AS s
	LEFT JOIN gold.dim_products AS p ON p.product_key = s.product_key
WHERE
	s.product_key IS NULL;  -- Result: none

