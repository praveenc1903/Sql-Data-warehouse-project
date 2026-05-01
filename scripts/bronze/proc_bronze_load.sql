/*
================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
        - Truncates the bronze tables before loading data.
        - Uses the 'BULK INSERT' command to load data from CSV files to bronze tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
================================================================
*/

-- ================================================================
-- Bronze Layer: Bulk Insert from Source CSV Files
-- ================================================================

USE DataWarehouse;
GO

-- CREATING THE STORED PROCEDURE
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start DATETIME;

    SET @batch_start = GETDATE();

    BEGIN TRY
        PRINT '================================================================'
        PRINT 'Bulk Loading Bronze Layer'
        PRINT 'Batch Start Time: ' + CONVERT(VARCHAR, @batch_start, 120)
        PRINT '================================================================'

        PRINT '================================================================'
        PRINT 'Loading CRM Source Tables'
        PRINT '================================================================'

        -- 1. Customer Info
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_cust_info'
        TRUNCATE TABLE bronze.crm_cust_info;
        PRINT '>> Inserting Data Into: bronze.crm_cust_info'
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\codes\sql-dwh-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Done: bronze.crm_cust_info | Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds'

        -- 2. Product Info
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_prd_info'
        TRUNCATE TABLE bronze.crm_prd_info;
        PRINT '>> Inserting Data Into: bronze.crm_prd_info'
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\codes\sql-dwh-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Done: bronze.crm_prd_info | Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds'

        -- 3. Sales Details
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_sales_details'
        TRUNCATE TABLE bronze.crm_sales_details;
        PRINT '>> Inserting Data Into: bronze.crm_sales_details'
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\codes\sql-dwh-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Done: bronze.crm_sales_details | Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds'

        PRINT '================================================================'
        PRINT 'Loading ERP Source Tables'
        PRINT '================================================================'

        -- 4. Location Data
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_loc_a101'
        TRUNCATE TABLE bronze.erp_loc_a101;
        PRINT '>> Inserting Data Into: bronze.erp_loc_a101'
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\codes\sql-dwh-project\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Done: bronze.erp_loc_a101 | Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds'

        -- 5. Customer Demographics
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_cust_az12'
        TRUNCATE TABLE bronze.erp_cust_az12;
        PRINT '>> Inserting Data Into: bronze.erp_cust_az12'
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\codes\sql-dwh-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Done: bronze.erp_cust_az12 | Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds'

        -- 6. Product Category
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2'
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2'
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\codes\sql-dwh-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Done: bronze.erp_px_cat_g1v2 | Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds'

        PRINT '================================================================'
        PRINT 'Bronze Layer Load Complete'
        PRINT 'Total Batch Duration: ' + CAST(DATEDIFF(SECOND, @batch_start, GETDATE()) AS VARCHAR) + ' seconds'
        PRINT 'Batch End Time: ' + CONVERT(VARCHAR, GETDATE(), 120)
        PRINT '================================================================'

    END TRY
    BEGIN CATCH
        PRINT '================================================================'
        PRINT 'ERROR OCCURRED DURING BRONZE LAYER LOAD'
        PRINT '================================================================'
        PRINT 'Error Number:    ' + CAST(ERROR_NUMBER() AS VARCHAR)
        PRINT 'Error Severity:  ' + CAST(ERROR_SEVERITY() AS VARCHAR)
        PRINT 'Error State:     ' + CAST(ERROR_STATE() AS VARCHAR)
        PRINT 'Error Procedure: ' + ISNULL(ERROR_PROCEDURE(), 'N/A')
        PRINT 'Error Line:      ' + CAST(ERROR_LINE() AS VARCHAR)
        PRINT 'Error Message:   ' + ERROR_MESSAGE()
        PRINT '================================================================'
    END CATCH
END
GO

-- Execute the stored procedure
EXEC bronze.load_bronze;
