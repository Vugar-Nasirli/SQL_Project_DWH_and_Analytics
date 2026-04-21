USE DataWarehouse;

-- EDA (Explatory Data Analysis)

-- 1. Exploring Database Structure
-- Purpose: To See Wide View of Database
-- 1.1 Explore Database Objects
SELECT * FROM INFORMATION_SCHEMA.TABLES;
SELECT * FROM INFORMATION_SCHEMA.VIEWS;
SELECT * FROM INFORMATION_SCHEMA.VIEW_TABLE_USAGE;

-- 1.2 Explore Database Columns
SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'gold';
SELECT * FROM INFORMATION_SCHEMA.VIEW_COLUMN_USAGE;
----------------------------------------------------------------------

-- 2. Exploring Dimensions
-- Purpose: Identirfying Unique values in each column. Find Granularity of Dimension
SELECT * FROM gold.dim_customers;
SELECT * FROM gold.dim_products;

-- dim_customers.country
SELECT DISTINCT country FROM gold.dim_customers;

-- dim_products.category
SELECT DISTINCT category FROM gold.dim_products;

-- dim_products.subcategory
SELECT DISTINCT category, subcategory, product_name FROM gold.dim_products;

-- dim_products.product_line
SELECT DISTINCT product_line FROM gold.dim_products;


-- 3. Date Exploration
-- Purpose: Understanding timespan and scope of the data with boundries

-- dim_customers.birthdate, create_date
SELECT
	MIN(birthdate) AS min_date,
	DATEDIFF(YEAR, MIN(birthdate), GETDATE()) AS oldest_customer,
	MAX(birthdate) AS max_date,
	DATEDIFF(YEAR, MAX(birthdate), GETDATE()) AS youngest_customer
FROM
	gold.dim_customers;

SELECT
	MIN(create_date) AS min_date,
	MAX(create_date) AS max_date
FROM
	gold.dim_customers;

-- dim_product.product_start_date
SELECT
	MIN(product_start_date) AS min_date,
	MAX(product_start_date) AS max_date,
	DATEDIFF(YEAR, MIN(product_start_date), MAX(product_start_date)) AS activity_years
FROM
	gold.dim_products;

-- fact_sales.order_date, shipping_date, due_date
SELECT
	MIN(order_date) AS min_date,
	MAX(order_date) AS max_date,
	DATEDIFF(YEAR, MIN(order_date), MAX(order_date)) AS activity_years
FROM
	gold.fact_sales;

SELECT
	MIN(shipping_date) AS min_date,
	MAX(shipping_date) AS max_date
FROM
	gold.fact_sales;

SELECT
	MIN(due_date) AS min_date,
	MAX(due_date) AS max_date
FROM
	gold.fact_sales;


-- 4. Measure Exploration
-- Purpose: Exploring Big Numbers and Key Metric for Business
-- Highest Level of Aggregations | Lowest Level of Details

-- Total Counts
SELECT COUNT(*) FROM gold.dim_customers;  -- Total Customers
SELECT COUNT(*) FROM gold.dim_products;   -- Total Products
SELECT COUNT(*) FROM gold.fact_sales;    -- Total Order Entries

-- Total Products that Ordered
SELECT
	COUNT(DISTINCT product_key)
FROM
	gold.fact_sales;

-- Total Orders
SELECT
	COUNT(DISTINCT order_number)
FROM
	gold.fact_sales;

-- Total Customers who Placed order
SELECT
	COUNT(DISTINCT customer_key)
FROM
	gold.fact_sales;


SELECT
	SUM(sales),  -- Total Sales
	SUM(profit),  -- Total Profit
	SUM(quantity),  -- Total Qunatity
	AVG(price),      -- Average Price
	AVG(cost)         -- Average Cost
FROM
(
	SELECT
		s.*,
		p.cost,
		(s.price - p.cost) * s.quantity AS profit
	FROM
		gold.fact_sales AS s
		LEFT JOIN gold.dim_products AS p ON p.product_key = s.product_key
) AS T;


-- Report
SELECT 'Total Customers' AS measure_name, COUNT(*) AS measure_value FROM gold.dim_customers
UNION
SELECT 'Total Products', COUNT(*) FROM gold.dim_products
UNION
SELECT 'Total Order Entries', COUNT(*) FROM gold.fact_sales
UNION
SELECT 'Total Orders', COUNT(DISTINCT order_number) FROM gold.fact_sales
UNION
SELECT 'Total Products Ordered', COUNT(DISTINCT product_key) FROM gold.fact_sales
UNION
SELECT 'Total Customers Place Order', COUNT(DISTINCT customer_key) FROM gold.fact_sales
UNION
SELECT 'Total Sales', SUM(sales) FROM gold.fact_sales
UNION
SELECT 'Average Price', AVG(price) FROM gold.fact_sales
UNION
SELECT 'Total Quantity', SUM(quantity) FROM gold.fact_sales
UNION
SELECT 'Average Cost', AVG(cost) FROM gold.fact_sales AS s LEFT JOIN gold.dim_products AS p ON p.product_key = s.product_key
UNION
SELECT
	'Total Profit',
	SUM(profit)
FROM
(
	SELECT
		(s.price - p.cost) * s.quantity AS profit
	FROM
		gold.fact_sales AS s
		LEFT JOIN gold.dim_products AS p ON p.product_key = s.product_key
) AS T;


-- 5. Magnitude Analysis
-- Purpose: Measure values According different groups and categories

-- Customer Count by Country
SELECT
	country,
	COUNT(*) AS customer_count
FROM
	gold.dim_customers
GROUP BY
	country;

-- Customer Count by Gender
SELECT
	gender,
	COUNT(*) AS customer_count
FROM
	gold.dim_customers
GROUP BY
	gender;

-- Customer Count by Age-Bins
SELECT
	CASE
		WHEN DATEDIFF(YEAR, birthdate, GETDATE()) > 50 THEN 'old'
		WHEN DATEDIFF(YEAR, birthdate, GETDATE()) > 30 THEN 'middle'
		ELSE 'young'
	END AS age_bins,
	COUNT(customer_key)
FROM
	gold.dim_customers
GROUP BY
	CASE
		WHEN DATEDIFF(YEAR, birthdate, GETDATE()) > 50 THEN 'old'
		WHEN DATEDIFF(YEAR, birthdate, GETDATE()) > 30 THEN 'middle'
		ELSE 'young'
	END;

-- Product Count by Category
SELECT
	category,
	COUNT(*) AS product_count
FROM
	gold.dim_products
GROUP BY
	category;

-- Product Count by Subcategory
SELECT
	category,
	subcategory,
	COUNT(*) AS product_count
FROM
	gold.dim_products
GROUP BY
	category,
	subcategory;

-- Product Count by Maintenance
SELECT
	maintenance,
	COUNT(*) AS product_count
FROM
	gold.dim_products
GROUP BY
	maintenance;

-- Product Count by Product Line
SELECT
	product_line,
	COUNT(*) AS product_count
FROM
	gold.dim_products
GROUP BY
	product_line;

-- Product Count by Starting Date
SELECT
	YEAR(product_start_date) AS start_year,
	COUNT(product_key)
FROM
	gold.dim_products
GROUP BY
	YEAR(product_start_date);

-- Cost by Category
SELECT
	category,
	AVG(cost)
FROM
	gold.dim_products
GROUP BY
	category;

-- Orders by Order Year
SELECT
	YEAR(order_date) AS order_year,
	COUNT(*)
FROM
	gold.fact_sales
GROUP BY
	YEAR(order_date);

-- Sales by ORder Year
SELECT
	YEAR(order_date) AS order_year,
	SUM(sales)
FROM
	gold.fact_sales
GROUP BY
	YEAR(order_date);

-- Orders by Country
SELECT
	c.country,
	COUNT(*) AS total_orders
FROM
	gold.fact_sales AS s
	LEFT JOIN gold.dim_customers AS c ON c.customer_key = s.customer_key
GROUP BY
	c.country;

-- Sales by Country
SELECT
	c.country,
	SUM(sales) AS total_sales
FROM
	gold.fact_sales AS s
	LEFT JOIN gold.dim_customers AS c ON c.customer_key = s.customer_key
GROUP BY
	c.country;

-- Sales by Gender
SELECT
	c.gender,
	SUM(sales) AS total_sales
FROM
	gold.fact_sales AS s
	LEFT JOIN gold.dim_customers AS c ON c.customer_key = s.customer_key
GROUP BY
	c.gender;

-- Orders by Gender
SELECT
	c.gender,
	COUNT(*) AS total_orders
FROM
	gold.fact_sales AS s
	LEFT JOIN gold.dim_customers AS c ON c.customer_key = s.customer_key
GROUP BY
	c.gender;

-- Sales by Marital Status
SELECT
	c.marital_status,
	SUM(s.sales) AS total_sales
FROM
	gold.fact_sales AS s
	LEFT JOIN gold.dim_customers AS c ON c.customer_key = s.customer_key
GROUP BY
	c.marital_status;

-- Orders by Marital Status
SELECT
	c.marital_status,
	COUNT(*) AS total_orders
FROM
	gold.fact_sales AS s
	LEFT JOIN gold.dim_customers AS c ON c.customer_key = s.customer_key
GROUP BY
	c.marital_status;

-- Sales by Age-Bins
SELECT
	CASE
		WHEN DATEDIFF(YEAR, birthdate, GETDATE()) > 50 THEN 'old'
		WHEN DATEDIFF(YEAR, birthdate, GETDATE()) > 30 THEN 'middle'
		ELSE 'young'
	END AS age_bins,
	SUM(sales) AS total_sales
FROM
	gold.fact_sales AS s
	LEFT JOIN gold.dim_customers AS c ON c.customer_key = s.customer_key
GROUP BY
	CASE
		WHEN DATEDIFF(YEAR, birthdate, GETDATE()) > 50 THEN 'old'
		WHEN DATEDIFF(YEAR, birthdate, GETDATE()) > 30 THEN 'middle'
		ELSE 'young'
	END;

-- Orders by Age-Bins
SELECT
	CASE
		WHEN DATEDIFF(YEAR, birthdate, GETDATE()) > 50 THEN 'old'
		WHEN DATEDIFF(YEAR, birthdate, GETDATE()) > 30 THEN 'middle'
		ELSE 'young'
	END AS age_bins,
	COUNT(*) AS total_orders
FROM
	gold.fact_sales AS s
	LEFT JOIN gold.dim_customers AS c ON c.customer_key = s.customer_key
GROUP BY
	CASE
		WHEN DATEDIFF(YEAR, birthdate, GETDATE()) > 50 THEN 'old'
		WHEN DATEDIFF(YEAR, birthdate, GETDATE()) > 30 THEN 'middle'
		ELSE 'young'
	END;

-- Order by Category
SELECT
	category,
	COUNT(*)
FROM
	gold.fact_sales AS s
	LEFT JOIN gold.dim_products AS p ON p.product_key = s.product_key
GROUP BY
	category;

-- Sales by Category
SELECT
	category,
	SUM(sales)
FROM
	gold.fact_sales AS s
	LEFT JOIN gold.dim_products AS p ON p.product_key = s.product_key
GROUP BY
	category;


-- Order by Subcategory
SELECT
	subcategory,
	COUNT(*)
FROM
	gold.fact_sales AS s
	LEFT JOIN gold.dim_products AS p ON p.product_key = s.product_key
GROUP BY
	subcategory;

-- Sales by Subcategory
SELECT
	subcategory,
	SUM(sales)
FROM
	gold.fact_sales AS s
	LEFT JOIN gold.dim_products AS p ON p.product_key = s.product_key
GROUP BY
	subcategory;

-- Orders by Product Line
SELECT
	product_line,
	COUNT(*)
FROM
	gold.fact_sales AS s
	LEFT JOIN gold.dim_products AS p ON p.product_key = s.product_key
GROUP BY
	product_line;

-- Sales by Product Line
SELECT
	product_line,
	SUM(sales)
FROM
	gold.fact_sales AS s
	LEFT JOIN gold.dim_products AS p ON p.product_key = s.product_key
GROUP BY
	product_line;

-- Average Price vs Cost by Category 
SELECT
	category,
	AVG(price),
	AVG(cost)
FROM
	gold.fact_sales AS s
	LEFT JOIN gold.dim_products AS p ON p.product_key = s.product_key
GROUP BY
	category;

-- Total Sales by Customers
SELECT
	s.customer_key,
	c.first_name,
	c.last_name,
	SUM(sales)
FROM
	gold.fact_sales AS s
	LEFT JOIN gold.dim_customers AS c ON c.customer_key = s.customer_key
GROUP BY
	s.customer_key,
	c.first_name,
	c.last_name
ORDER BY
	SUM(sales) DESC;


-- 6. Ranking Analysis
-- Purpose: Order the Measure values by dimensions

-- TOP 10 Best Products by Profit
SELECT TOP 10
	s.product_key,
	p.product_name,
	SUM((s.price - p.cost) * s.quantity) AS total_profit
FROM
	gold.fact_sales AS s
	LEFT JOIN gold.dim_products AS p ON p.product_key = s.product_key
GROUP BY
	s.product_key,
	p.product_name
ORDER BY
	total_profit DESC;

-- TOP 5 Best Subcategory by Profit
SELECT TOP 5
	p.category,
	p.subcategory,
	SUM((s.price - p.cost) * s.quantity) AS total_profit
FROM
	gold.fact_sales AS s
	LEFT JOIN gold.dim_products AS p ON p.product_key = s.product_key
GROUP BY
	p.category,
	p.subcategory
ORDER BY
	total_profit DESC;

-- TOP 10 Best Customers by Profit
SELECT TOP 10
	s.customer_key,
	c.first_name,
	c.last_name,
	SUM((s.price - p.cost) * s.quantity) AS total_profit
FROM
	gold.fact_sales AS s
	LEFT JOIN gold.dim_products AS p ON p.product_key = s.product_key
	LEFT JOIN gold.dim_customers AS c ON c.customer_key = s.customer_key
GROUP BY
	s.customer_key,
	c.first_name,
	c.last_name
ORDER BY
	total_profit DESC;

-- WORST 3 Customers by Order Count
SELECT TOP 3
	s.customer_key,
	c.first_name,
	c.last_name,
	COUNT(*) AS total_orders
FROM
	gold.fact_sales AS s
	LEFT JOIN gold.dim_customers AS c ON c.customer_key = s.customer_key
GROUP BY
	s.customer_key,
	c.first_name,
	c.last_name
ORDER BY
	total_orders ASC;
