-- Quality Control in Bronze Layer

-- Table: crm_cust_info
-- Before Transformations - Bronze
SELECT
	*
FROM
	bronze.crm_cust_info;
-- After Transformations - Silver
SELECT
	*
FROM
	silver.crm_cust_info;

-- Checking and Transformation Steps:

-- 1. Check for NULLs and Duplicates in Primary Key
-- Expected Result: None

-- Find NULLs and Remove because of Primary key
SELECT
	cst_id,
	cst_key
FROM
	bronze.crm_cust_info
WHERE
	cst_id IS NULL; -- 3 results

-- After Transformations On Silver Layer
SELECT
	cst_id,
	cst_key
FROM
	silver.crm_cust_info
WHERE
	cst_id IS NULL; -- No result

-- Find Duplicates and Remove
SELECT
	cst_id,
	COUNT(*)
FROM
	bronze.crm_cust_info
GROUP BY
	cst_id
HAVING
	COUNT(*) > 1 AND cst_id IS NOT NULL; -- 5 results

-- After Transformations On Silver Layer
SELECT
	cst_id,
	COUNT(*)
FROM
	silver.crm_cust_info
GROUP BY
	cst_id
HAVING
	COUNT(*) > 1 AND cst_id IS NOT NULL; -- No result

-- Analyze Duplicates
SELECT
	ROW_NUMBER() OVER(
		PARTITION BY cst_id
		ORDER BY cst_create_date DESC
	) AS flag, -- Flag: 1
	*
FROM
	bronze.crm_cust_info
WHERE
	cst_id IN 
	(
		SELECT
			cst_id
		FROM
			bronze.crm_cust_info
		GROUP BY
			cst_id
		HAVING
			COUNT(*) > 1 AND cst_id IS NOT NULL		-- 11 Results
	);

-- After Transformations On Silver Layer
SELECT
	ROW_NUMBER() OVER(
		PARTITION BY cst_id
		ORDER BY cst_create_date DESC
	) AS flag, -- Flag: 1
	*
FROM
	silver.crm_cust_info
WHERE
	cst_id IN 
	(
		SELECT
			cst_id
		FROM
			silver.crm_cust_info
		GROUP BY
			cst_id
		HAVING
			COUNT(*) > 1 AND cst_id IS NOT NULL		 -- No Results
	);

-- 2. Check for unwanted spaces
-- Expected Result: original value = trimmed value
SELECT
	*
FROM
	bronze.crm_cust_info
WHERE
	cst_firstname != TRIM(cst_firstname); -- 15 results

SELECT
	*
FROM
	bronze.crm_cust_info
WHERE
	cst_lastname != TRIM(cst_lastname); -- 17 results


-- After Transformations On Silver Layer
SELECT
	*
FROM
	silver.crm_cust_info
WHERE
	cst_firstname != TRIM(cst_firstname); -- No result

SELECT
	*
FROM
	silver.crm_cust_info
WHERE
	cst_lastname != TRIM(cst_lastname); -- No result

-- 3. Data Consistency when Low Cardinality Columns (ex: gender)
-- 4. Data Normalization and Standardization
-- 5. Handling Missing Data
-- Expected Result: Detailed name
SELECT DISTINCT
	cst_gndr
FROM
	bronze.crm_cust_info; -- Result: NULL, F, M

SELECT DISTINCT
	cst_marital_status
FROM
	bronze.crm_cust_info; -- Result: NULL, S, M

-- After Transformations On Silver Layer
SELECT DISTINCT
	cst_gndr
FROM
	silver.crm_cust_info; -- Result: n/a, Female, Male

SELECT DISTINCT
	cst_marital_status
FROM
	silver.crm_cust_info; -- Result: n/a, Single, Married

------------------------------------------------------------------

-- Table: crm_prd_info
-- Before Transformations
SELECT
	*
FROM
	bronze.crm_prd_info;
-- After Transformations
SELECT
	*
FROM
	silver.crm_prd_info;

-- 1. Check for NULLs and Duplicates for Primary Key
-- Expeted Result: none
-- Before
SELECT
	prd_id,
	COUNT(*)
FROM
	bronze.crm_prd_info
GROUP BY
	prd_id
HAVING
	COUNT(*) > 1 OR prd_id IS NULL  -- Result: none

-- After
SELECT
	prd_id,
	COUNT(*)
FROM
	silver.crm_prd_info
GROUP BY
	prd_id
HAVING
	COUNT(*) > 1 OR prd_id IS NULL  -- Result: none

-- 2. Derived Column 
-- Before
SELECT
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5),'-', '_') AS cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key_test
FROM
	bronze.crm_prd_info;

-- After
SELECT
	prd_key,
	cat_id
FROM
	silver.crm_prd_info;

-- 3. Unwanted Spaces
-- Before
SELECT
	prd_nm
FROM
	bronze.crm_prd_info
WHERE
	prd_nm != TRIM(prd_nm); -- Result: none

-- After
SELECT
	prd_nm
FROM
	silver.crm_prd_info
WHERE
	prd_nm != TRIM(prd_nm); -- Result: none

-- 4. Check NULLs and Negative number in cost
-- Before
SELECT
	prd_cost
FROM
	bronze.crm_prd_info
WHERE
	prd_cost IS NULL OR prd_cost < 0; -- Result: 2 NULLs

-- After
SELECT
	prd_cost
FROM
	silver.crm_prd_info
WHERE
	prd_cost IS NULL OR prd_cost < 0; -- Result: none

-- 5. Cardinality Low and Abbrevation used then Data Standardization and Normalization
SELECT DISTINCT
	prd_line
FROM
	bronze.crm_prd_info -- Result: NULL, M, R, S, T

SELECT DISTINCT
	prd_line
FROM
	silver.crm_prd_info -- Result: n/a, Mountain, Road, Other Sales, Touring

-- 6. Check for Invalid Date Orders
-- Data Enrichment
SELECT
	prd_id,
	prd_key,
	prd_nm,
	prd_start_dt,
	DATEADD(DAY, -1, LEAD(prd_start_dt) OVER(
			PARTITION BY prd_key
			ORDER BY prd_start_dt
		)) AS prd_end_dt_test,
	prd_end_dt
FROM
	bronze.crm_prd_info
WHERE
	prd_key IN ('AC-HE-HL-U509', 'AC-HE-HL-U509-R', 'AC-HE-HL-U509-B'); -- Result: Invalid

SELECT
	prd_id,
	prd_key,
	prd_nm,
	prd_start_dt,
	prd_end_dt
FROM
	silver.crm_prd_info
WHERE
	prd_key IN ('HL-U509', 'HL-U509-R', 'HL-U509-B'); -- Result: Valid

---------------------------------------------------------------------------------

-- Table: crm_sales_details
-- Before
SELECT
	*
FROM
	bronze.crm_sales_details;

-- After
SELECT
	*
FROM
	silver.crm_sales_details;

-- 1. NULLs and Duplicates in Primary Key
SELECT
	sls_ord_num,
	COUNT(*)
FROM
	bronze.crm_sales_details
GROUP BY
	sls_ord_num
HAVING
	COUNT(*) > 1 OR sls_ord_num IS NULL; -- Result: Many Duplicates and No NULL

-- 2. Check Foreign Key Matching
SELECT
	*
FROM
	bronze.crm_sales_details
WHERE
	sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info); -- Result: none (expected)

SELECT
	*
FROM
	bronze.crm_sales_details
WHERE
	sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info); -- Result: none (expected)

-- 3. Check for Invalid Values
SELECT
	sls_order_dt
FROM
	bronze.crm_sales_details
WHERE
	sls_order_dt <= 0 OR sls_order_dt IS NULL OR LEN(sls_order_dt) != 8; -- Result: 15 value = 0; No negative value; 2 invalid value

SELECT
	sls_ship_dt
FROM
	bronze.crm_sales_details
WHERE
	sls_ship_dt <= 0 OR sls_ship_dt IS NULL OR LEN(sls_ship_dt) != 8;  -- Result: None

SELECT
	sls_due_dt
FROM
	bronze.crm_sales_details
WHERE
	sls_due_dt <= 0 OR sls_due_dt IS NULL OR LEN(sls_due_dt) != 8;  -- Result: None

-- 4. Check for Invalid Date Orders
SELECT
	*
FROM
	bronze.crm_sales_details
WHERE
	sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- Check Data Consistency within sales, quantity and price
SELECT
	sls_sales,
	sls_quantity,
	sls_price
FROM
	bronze.crm_sales_details
WHERE
	sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL; -- Result: 15 NULL

SELECT
	sls_sales,
	sls_quantity,
	sls_price
FROM
	bronze.crm_sales_details
WHERE
	sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0; -- Result: 10 values are zero or negative

SELECT
	sls_sales,
	sls_quantity,
	sls_price
FROM
	bronze.crm_sales_details
WHERE
	ABS(sls_sales) != sls_quantity * ABS(sls_price); -- Result: 13 multiplication error

-- Main Value: Price
-- Before
SELECT DISTINCT
	sls_sales,
	sls_quantity,
	sls_price,
	CASE       -- When Sales Value is Inconsistent  OR Multiplication fault
		WHEN (sls_sales IS NULL OR sls_sales = 0) OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
		ELSE ABS(sls_sales)
	END AS sls_sales_test,
	sls_quantity AS quantity,
	CASE       -- When Price Value is Inconsistent
		WHEN sls_price IS NULL OR sls_price = 0 THEN ABS(sls_sales) / NULLIF(sls_quantity, 0)
		ELSE ABS(sls_price)
	END AS sls_price_test
FROM
	bronze.crm_sales_details
WHERE
	sls_sales != sls_quantity * sls_price
	OR
	sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
	OR
	sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
ORDER BY
	sls_sales,
	sls_quantity,
	sls_price;

-- After
SELECT DISTINCT
	sls_sales,
	sls_quantity,
	sls_price
FROM 
	silver.crm_sales_details
WHERE
	sls_sales != sls_quantity * sls_price
	OR
	sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
	OR
	sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
ORDER BY
	sls_sales,
	sls_quantity,
	sls_price;

----------------------------------------------------------------------

-- Table: erp_cust_az12
-- Before
SELECT
	*
FROM
	bronze.erp_cust_az12;

-- After
SELECT
	*
FROM
	silver.erp_cust_az12;

-- 1. Check NULLs and Duplicates in Primary Key
SELECT
	cid,
	COUNT(*)
FROM
	bronze.erp_cust_az12
GROUP BY
	cid
HAVING
	COUNT(*) > 1 OR cid IS NULL;  -- Result: There is no NULLs and Duplicates

-- 2. Check for Foreign Key
-- Expected Result: None
SELECT
	cid,
	SUBSTRING(cid, LEN(cid) - 9, LEN(cid)) AS cid_new, -- Last 10 characters 
	bdate,
	gen
FROM
	bronze.erp_cust_az12
WHERE
	SUBSTRING(cid, LEN(cid) - 9, LEN(cid)) NOT IN (SELECT cst_key FROM bronze.crm_cust_info) -- Result: None


-- 3. Consistent date column
SELECT
	*
FROM
	bronze.erp_cust_az12
WHERE
	bdate IS NULL; -- Result: None

SELECT
	*
FROM
	bronze.erp_cust_az12
WHERE
	bdate > DATEADD(YEAR, -18, GETDATE()); -- Result: 16 Inconsistent Value

-- 4. Consistency check for low cardinality column
SELECT DISTINCT
	CASE 
		WHEN TRIM(gen) IS NULL OR TRIM(gen) = '' THEN 'n/a'
		WHEN TRIM(gen) = 'F' THEN 'Female'
		WHEN TRIM(gen) = 'M' THEN 'Male'
		ELSE TRIM(gen)
	END gen
FROM
	bronze.erp_cust_az12;

--------------------------------------------------------------------------

-- Table: erp_loc_a101
-- Before
SELECT
	*
FROM
	bronze.erp_loc_a101;

-- After
SELECT
	*
FROM
	silver.erp_loc_a101;

-- 1. Check for NULLs
SELECT
	*
FROM
	bronze.erp_loc_a101
WHERE
	cid IS NULL; -- Result: none

-- 2. Check for foreign key
SELECT
	REPLACE(cid, '-', '') AS cid,
	cntry
FROM
	bronze.erp_loc_a101
WHERE
	REPLACE(cid, '-', '') NOT IN (SELECT cst_key FROM bronze.crm_cust_info); -- Result: none

-- 3. Check for unwanted spaces 
SELECT
	*
FROM
	bronze.erp_loc_a101
WHERE
	cntry != TRIM(cntry); -- Result: none

-- 4. Check for Low cardinality column
-- Data standardization and normalization
-- Before
SELECT DISTINCT
	cntry,
	CASE
		WHEN UPPER(TRIM(cntry)) IN ('USA','US','UNITED STATES') THEN 'USA'
		WHEN UPPER(TRIM(cntry)) IN ('DE','GERMANY') THEN 'Germany'
		WHEN UPPER(TRIM(cntry)) = '' OR cntry IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
	END AS cntry_new
FROM
	bronze.erp_loc_a101;

-- After
SELECT DISTINCT
	cntry
FROM
	silver.erp_loc_a101;

---------------------------------------------------------------

-- Table: erp_px_cat_g1v2
-- Before
SELECT
	*
FROM
	bronze.erp_px_cat_g1v2;

-- After
SELECT
	*
FROM
	silver.erp_px_cat_g1v2;


-- 1. NULLs and Duplcates in primary key
SELECT
	id,
	COUNT(*)
FROM
	bronze.erp_px_cat_g1v2
GROUP BY
	id
HAVING
	COUNT(*) > 1 OR id IS NULL; -- Result: None

-- 2. Data Normalization
SELECT DISTINCT
	cat,
	subcat
FROM
	bronze.erp_px_cat_g1v2
ORDER BY
	cat;

SELECT DISTINCT
	maintenance
FROM
	bronze.erp_px_cat_g1v2