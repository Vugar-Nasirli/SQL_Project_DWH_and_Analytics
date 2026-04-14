-- Stored Procedure for FULL LOAD (TRUNCATE & INSERT) all tables
CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	SET @batch_start_time = GETDATE();
	BEGIN TRY
		PRINT('=============================');
		PRINT('Loading Bronze Layer...');
		PRINT('=============================');
		PRINT('');
		PRINT('-----------------------------');
		PRINT('Loading CRM Tables...');
		PRINT('-----------------------------');

		SET @start_time = GETDATE();
		-- Truncate
		PRINT('');
		PRINT('>> Truncating Table: bronze.crm_cust_info');
		TRUNCATE TABLE bronze.crm_cust_info;
		-- Bulk Insert
		PRINT('');
		PRINT('>> Inserting Data Into: bronze.crm_cust_info');
		BULK INSERT bronze.crm_cust_info
		FROM 'path\to\file'
		WITH
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT('');
		PRINT('>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds');
		PRINT('-----------------------------');

		SET @start_time = GETDATE();
		-- Truncate
		PRINT('');
		PRINT('>> Truncating Table: bronze.crm_prd_info');
		TRUNCATE TABLE bronze.crm_prd_info;
		-- Bulk Insert
		PRINT('');
		PRINT('>> Inserting Data Into: bronze.crm_prd_info');
		BULK INSERT bronze.crm_prd_info
		FROM 'path\to\file'
		WITH
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT('');
		PRINT('>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds');
		PRINT('-----------------------------');

		SET @start_time = GETDATE();
		-- Truncate
		PRINT('');
		PRINT('>> Truncating Table: bronze.crm_sales_details');
		TRUNCATE TABLE bronze.crm_sales_details;
		-- Bulk Insert
		PRINT('');
		PRINT('>> Inseerting Data Into: crm_sales_details');
		BULK INSERT bronze.crm_sales_details
		FROM 'path\to\file'
		WITH
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT('');
		PRINT('>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds');
		PRINT('-----------------------------');

		PRINT('');
		PRINT('-----------------------------');
		PRINT('Loading ERP Tables...');
		PRINT('-----------------------------');

		SET @start_time = GETDATE();
		-- Truncate
		PRINT('');
		PRINT('>> Truncating Table: bronze.erp_cust_az12');
		TRUNCATE TABLE bronze.erp_cust_az12;
		-- Bulk Insert
		PRINT('');
		PRINT('>> Inserting Data Into: bronze.erp_cust_az12');
		BULK INSERT bronze.erp_cust_az12
		FROM 'path\to\file'
		WITH
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT('');
		PRINT('>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds');
		PRINT('-----------------------------');

		SET @start_time = GETDATE();
		-- Truncate
		PRINT('');
		PRINT('>> Truncating Table: bronze.erp_loc_a101');
		TRUNCATE TABLE bronze.erp_loc_a101;
		-- Bulk Insert
		PRINT('');
		PRINT('>> Inserting Data Into: bronze.erp_loc_a101');
		BULK INSERT bronze.erp_loc_a101
		FROM 'path\to\file'
		WITH
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT('');
		PRINT('>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds');
		PRINT('-----------------------------');

		SET @start_time = GETDATE();
		-- Truncate
		PRINT('');
		PRINT('>> Truncating Table: bronze.erp_px_cat_g1v2');
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		-- Bulk Insert
		PRINT('');
		PRINT('>> Inserting Data Into: bronze.erp_px_cat_g1v2');
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'path\to\file'
		WITH
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT('');
		PRINT('>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds');
		PRINT('-----------------------------');
	END TRY
	BEGIN CATCH
		PRINT('===================================================');
		PRINT('ERROR OCCURED DURING LOADING BRONZE LAYER!');
		PRINT('Error Message: ' + ERROR_MESSAGE());
		PRINT('Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR));
		PRINT('Error State: ' + CAST(ERROR_STATE() AS NVARCHAR));
		PRINT('Error Line: ' + CAST(ERROR_LINE() AS NVARCHAR));
		PRINT('===================================================');
	END CATCH
	SET @batch_end_time = GETDATE();
	PRINT('');
	PRINT('==============================================');
	PRINT('Loading Bronze Layer Is Completed!');
	PRINT('Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds');
	PRINT('==============================================');
END;

EXEC bronze.load_bronze;
