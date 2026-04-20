USE DataWarehouse;
GO

--	Customer Information Tables (Dimension Table): 
--	crm_cust_info(main), erp_cust_az12m(bdate, gen), erp_loc_a101(cntry)
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
	DROP VIEW gold.dim_customers;
GO
CREATE VIEW gold.dim_customers
AS
SELECT
	ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	cl.cntry AS country,
	cb.bdate AS birthdate,
	CASE
		WHEN ci.cst_gndr = 'n/a' AND cb.gen IS NOT NULL THEN cb.gen
		ELSE ci.cst_gndr
	END AS gender,
	ci.cst_marital_status AS marital_status,
	ci.cst_create_date AS create_date
FROM
	silver.crm_cust_info AS ci
	LEFT JOIN silver.erp_cust_az12 AS cb ON cb.cid = ci.cst_key
	LEFT JOIN silver.erp_loc_a101 AS cl ON cl.cid = ci.cst_key;

-------------------------------------------------------------------

--	Product Information Tables (Dimension Table):
--	crm_prd_info(main), erp_px_cat_g1v2(cat, subcat)
GO

IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
	DROP VIEW gold.dim_products;
GO
CREATE VIEW gold.dim_products
AS
SELECT
	ROW_NUMBER() OVER(ORDER BY pn.prd_id) AS product_key,
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	CASE
		WHEN pn.cat_id = 'CO_PE' THEN 'Components'
		ELSE pc.cat
	END AS category,
	CASE
		WHEN pn.cat_id = 'CO_PE' THEN 'Pedal'
		ELSE pc.subcat
	END AS subcategory,
	ISNULL(pc.maintenance, 'No') AS maintenance,
	pn.prd_cost AS cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS product_start_date
FROM
	silver.crm_prd_info AS pn
	LEFT JOIN silver.erp_px_cat_g1v2 AS pc ON pc.id = pn.cat_id
WHERE
	prd_end_dt IS NULL;  -- Filter out historical data

-----------------------------------------------------------------

--	Sales Information Table (Fact Table):
--	crm_sales_details
GO

IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
	DROP VIEW gold.fact_sales;
GO
CREATE VIEW gold.fact_sales
AS
SELECT
	sd.sls_ord_num AS order_number,
	p.product_key,
	c.customer_key,
	CASE
		WHEN sd.sls_order_dt IS NULL THEN DATEADD(DAY, -7, sd.sls_ship_dt)
		ELSE sd.sls_order_dt
	END AS order_date,
	CASE
		WHEN sd.sls_ship_dt IS NULL THEN DATEADD(DAY, 7, sd.sls_order_dt)
		ELSE sd.sls_ship_dt
	END AS shipping_date,
	CASE
		WHEN sd.sls_due_dt IS NULL THEN DATEADD(DAY, 7, sd.sls_ship_dt)
		ELSE sd.sls_due_dt
	END AS due_date,
	sd.sls_sales AS sales,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price
FROM
	silver.crm_sales_details AS sd
	LEFT JOIN gold.dim_customers AS c ON c.customer_id = sd.sls_cust_id
	LEFT JOIN gold.dim_products AS p ON p.product_number = sd.sls_prd_key;
