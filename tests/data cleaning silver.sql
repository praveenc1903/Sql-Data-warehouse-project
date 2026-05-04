/*
================================================================
Data Quality Checks: crm_cust_info
Purpose: Validate bronze data before silver layer load.
         Identifies duplicates, NULLs, spacing issues,
         and inconsistent coded values.
================================================================
*/

-- ================================================================
-- CHECK 1: Duplicates & NULLs in Primary Key (cst_id)
-- Expectation: Each cst_id should appear once; no NULLs allowed
-- Action: Duplicates are handled by ROW_NUMBER() in silver load;
--         NULLs are excluded via WHERE cst_id IS NOT NULL
-- ================================================================
SELECT cst_id, COUNT(*) AS duplicate_count
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Verify silver table has no remaining duplicates after load
SELECT * 
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY cst_id 
            ORDER BY cst_create_date DESC
        ) AS flag_last
    FROM silver.crm_cust_info
) t
WHERE flag_last > 1;  -- Should return 0 rows if dedup worked correctly

-- ================================================================
-- CHECK 2: Unwanted Leading/Trailing Spaces in Name Fields
-- Expectation: No whitespace padding in first or last names
-- Action: TRIM() applied during silver load
-- ================================================================
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

-- ================================================================
-- CHECK 3: Preview Cleansed Data (Bronze -> Silver Transform)
-- Shows deduplicated, trimmed output before insert
-- ================================================================
SELECT 
    cst_id,
    TRIM(cst_firstname)  AS cst_firstname,
    TRIM(cst_lastname)   AS cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY cst_id 
            ORDER BY cst_create_date DESC
        ) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) t
WHERE flag_last = 1;

-- ================================================================
-- CHECK 4: Data Consistency — Gender Values
-- Expectation: Only 'M', 'F' (and possibly NULL/blank)
-- Action: Standardised to 'Male', 'Female', 'n/a' in silver load
-- ================================================================
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;

-- ================================================================
-- CHECK 5: Data Consistency — Marital Status Values
-- Expectation: Only 'S', 'M' (and possibly NULL/blank)
-- Action: Standardised to 'Single', 'Married', 'n/a' in silver load
-- ================================================================
SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info;




/*
================================================================
Data Quality Checks: crm_prd_info
Purpose: Validate bronze product data before silver layer load.
         Identifies duplicates, NULLs, spacing issues,
         invalid costs, inconsistent codes, and date logic errors.
================================================================
*/

-- ================================================================
-- CHECK 1: Duplicates & NULLs in Primary Key (prd_id)
-- Expectation: Each prd_id should be unique; no NULLs
-- Result: No duplicates or NULLs found
-- ================================================================
SELECT prd_id, COUNT(*) AS duplicate_count
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- ================================================================
-- CHECK 2: Unwanted Spaces in Product Name
-- Expectation: No leading/trailing whitespace in prd_nm
-- Result: No spacing issues found
-- ================================================================
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- ================================================================
-- CHECK 3: Invalid Product Costs (Negatives or NULLs)
-- Expectation: All costs should be >= 0 and not NULL
-- Action: NULLs replaced with 0 via ISNULL() in silver load
-- ================================================================
SELECT prd_cost 
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- ================================================================
-- CHECK 4: Data Consistency — Product Line Values
-- Expectation: Only 'M', 'R', 'S', 'T' (and possibly NULL/blank)
-- Action: Standardised to Mountain/Road/Other Sales/Touring/n/a
-- ================================================================
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info;

-- ================================================================
-- CHECK 5: Date Logic — Start Date Must Not Exceed End Date
-- Expectation: prd_start_dt <= prd_end_dt for all records
-- Note: prd_end_dt is derived via LEAD() in silver load,
--       so this check validates the raw bronze dates
-- ================================================================
SELECT * 
FROM bronze.crm_prd_info
WHERE prd_start_dt > prd_end_dt;

select * from 
silver.crm_prd_info;


/*
================================================================
Data Quality Checks: crm_sales_details
Purpose: Validate bronze sales data before silver layer load.
         Identifies invalid dates, date logic errors,
         and inconsistent financial values.
================================================================
*/

-- ================================================================
-- CHECK 1: Invalid Order Dates
-- Expectation: All dates should be 8 digits (YYYYMMDD) and > 0
-- Action: Invalid dates replaced with NULL via NULLIF in silver load
-- ================================================================
SELECT NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8;

-- ================================================================
-- CHECK 2: Invalid Ship Dates
-- Expectation: All dates should be 8 digits (YYYYMMDD) and > 0
-- Action: Invalid dates replaced with NULL via NULLIF in silver load
-- ================================================================
SELECT NULLIF(sls_ship_dt, 0) AS sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8;

-- ================================================================
-- CHECK 3: Invalid Due Dates
-- Expectation: All dates should be 8 digits (YYYYMMDD) and > 0
-- Action: Invalid dates replaced with NULL via NULLIF in silver load
-- ================================================================
SELECT NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 OR LEN(sls_due_dt) != 8;

-- ================================================================
-- CHECK 4: Date Logic — Order Date Must Not Exceed Ship/Due Date
-- Expectation: order_dt <= ship_dt AND order_dt <= due_dt
-- ================================================================
SELECT * 
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
   OR sls_order_dt > sls_due_dt;

-- ================================================================
-- CHECK 5: Sales Amount Consistency
-- Business Rule: sales = quantity * price
-- Expectation: No negatives, no NULLs, no zeroes,
--              and sales must equal quantity * price
-- ================================================================
SELECT DISTINCT
    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details
WHERE CAST(sls_sales AS INT) <= 0 
   OR CAST(sls_quantity AS INT) <= 0 
   OR CAST(sls_price AS INT) <= 0
   OR CAST(sls_sales AS INT) != CAST(sls_quantity AS INT) * CAST(sls_price AS INT)
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
ORDER BY sls_sales, sls_quantity, sls_price;

-- ================================================================
-- CHECK 6: Preview Corrected Sales Values (Before vs After)
-- Shows how invalid records will be fixed in the silver load:
--   - NULL/negative/zero sales: recalculated as quantity * ABS(price)
--   - NULL/negative price: derived as ABS(sales) / quantity
-- ================================================================
SELECT 
    sls_sales    AS old_sls_sales,
    sls_quantity,
    sls_price    AS old_sls_price,

    CASE 
        WHEN CAST(sls_sales AS INT) IS NULL 
          OR CAST(sls_sales AS INT) <= 0 
          OR CAST(sls_sales AS INT) != CAST(sls_quantity AS INT) * ABS(CAST(sls_price AS INT))
        THEN CAST(sls_quantity AS INT) * ABS(CAST(sls_price AS INT))
        ELSE CAST(sls_sales AS INT)
    END AS sls_sales,

    CASE 
        WHEN CAST(sls_price AS INT) <= 0 
          OR sls_price IS NULL
        THEN ABS(CAST(sls_sales AS INT)) / NULLIF(CAST(sls_quantity AS INT), 0)
        ELSE CAST(sls_price AS INT)
    END AS sls_price

FROM bronze.crm_sales_details
WHERE CAST(sls_sales AS INT) <= 0 
   OR CAST(sls_quantity AS INT) <= 0 
   OR CAST(sls_price AS INT) <= 0
   OR CAST(sls_sales AS INT) != CAST(sls_quantity AS INT) * CAST(sls_price AS INT)
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
ORDER BY sls_sales, sls_quantity, sls_price;



/*
================================================================
Data Quality Checks: erp_cust_az12
Purpose: Validate bronze ERP customer data before silver load.
         Identifies orphan records, invalid dates,
         and inconsistent gender values.
================================================================
*/

-- ================================================================
-- CHECK 1: Orphan Records — CIDs Not Found in CRM Customer Table
-- Expectation: Every ERP customer should have a matching CRM record
-- Note: 'NAS' prefix stripped before comparison to align key formats
-- Action: Investigate orphans — may indicate missing CRM data
--         or incorrect key mapping
-- ================================================================
SELECT 
    cid AS original_cid,
    CASE 
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) 
        ELSE cid 
    END AS cleaned_cid,
    bdate,
    gen 
FROM bronze.erp_cust_az12
WHERE CASE 
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) 
        ELSE cid 
      END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info);

-- ================================================================
-- CHECK 2: Invalid Birth Dates — Too Old or in the Future
-- Expectation: Birth dates should be after 1945 and not in the future
-- Action: Replaced with NULL in silver load
-- ================================================================
SELECT * 
FROM bronze.erp_cust_az12
WHERE bdate < '1/1/1945' 
   OR bdate > GETDATE();

-- ================================================================
-- CHECK 3: Gender Value Consistency — Preview Standardised Output
-- Expectation: Only 'Male', 'Female', or 'n/a' after transformation
-- Action: Standardised in silver load using CASE mapping
-- ================================================================
SELECT DISTINCT
    CASE 
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
        ELSE 'n/a'
    END AS gen 
FROM bronze.erp_cust_az12;




/*
================================================================
Data Quality Checks: erp_loc_a101
Purpose: Validate bronze ERP location data before silver load.
         Identifies orphan records and inconsistent country values.
================================================================
*/

-- ================================================================
-- CHECK 1: Orphan Records — CIDs Not Found in CRM Customer Table
-- Expectation: Every ERP location record should match a CRM customer
-- Note: Dashes removed from cid before comparison
-- Action: Investigate orphans — may indicate missing CRM data
--         or incorrect key mapping
-- ================================================================
SELECT 
    REPLACE(cid, '-', '') AS cid,
    cntry
FROM bronze.erp_loc_a101
WHERE REPLACE(cid, '-', '') NOT IN 
    (SELECT cst_key FROM silver.crm_cust_info);

-- ================================================================
-- CHECK 2: Country Value Consistency — Preview Standardised Output
-- Expectation: Full country names after transformation
-- Action: Codes mapped to full names in silver load:
--         'DE' -> Germany, 'US'/'USA' -> United States
--         Blank/NULL -> 'n/a', others trimmed as-is
-- ================================================================
SELECT 
    cntry AS old_cntry,
    CASE 
        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
        ELSE TRIM(cntry)
    END AS cntry
FROM bronze.erp_loc_a101;




/*
================================================================
Data Quality Checks: erp_px_cat_g1v2
Purpose: Validate bronze ERP product category data before silver load.
         Identifies spacing issues and inconsistent values
         in category, subcategory, and maintenance fields.
================================================================
*/

-- ================================================================
-- CHECK 1: Preview Raw Data
-- ================================================================
SELECT id, cat, subcat, maintenance
FROM bronze.erp_px_cat_g1v2;

-- ================================================================
-- CHECK 2: Unwanted Spaces in Category Field
-- Expectation: No leading/trailing whitespace in cat
-- Action: TRIM() applied in silver load
-- ================================================================
SELECT * 
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat);

-- ================================================================
-- CHECK 3: Unwanted Spaces in Subcategory & Maintenance Fields
-- Expectation: No leading/trailing whitespace
-- Action: TRIM() applied in silver load
-- ================================================================
SELECT * 
FROM bronze.erp_px_cat_g1v2
WHERE subcat != TRIM(subcat) 
   OR maintenance != TRIM(maintenance);

-- ================================================================
-- CHECK 4: Data Consistency — Category Values
-- Expectation: Clean, consistent category names
-- Action: Review distinct values for any misspellings or variants
-- ================================================================
SELECT DISTINCT cat
FROM bronze.erp_px_cat_g1v2;

-- ================================================================
-- CHECK 5: Data Consistency — Subcategory Values
-- Expectation: Clean, consistent subcategory names
-- Action: Review distinct values for any misspellings or variants
-- ================================================================
SELECT DISTINCT subcat
FROM bronze.erp_px_cat_g1v2;

-- ================================================================
-- CHECK 6: Data Consistency — Maintenance Values
-- Expectation: Clean, consistent values (e.g. Yes/No)
-- Action: Review distinct values for standardisation in silver load
-- ================================================================
SELECT DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2;
