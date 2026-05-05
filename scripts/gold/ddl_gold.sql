/*
===============================================================================
Gold Layer: Dimension Views
===============================================================================
Script Purpose:
    Creates dimension views for the gold layer by joining and enriching
    silver layer tables into business-ready dimensional models.

Views Created:
    - gold.dim_customers: Customer dimension with demographics and location
    - gold.dim_products:  Product dimension with category details
===============================================================================
*/

-- ================================================================
-- DIMENSION: Customers
-- Source: CRM customer info enriched with ERP demographics and location
-- Business Logic:
--   - Surrogate key generated via ROW_NUMBER()
--   - CRM is the master source for gender; ERP used as fallback
--   - Left joins preserve all CRM customers even without ERP matches
-- ================================================================
CREATE VIEW gold.dim_customers AS 
SELECT 
    ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,
    ci.cst_id            AS customer_id,
    ci.cst_key           AS customer_number,
    ci.cst_firstname     AS first_name,
    ci.cst_lastname      AS last_name,
    la.cntry             AS country, 
    ci.cst_marital_status AS marital_status,
    ca.bdate             AS birthdate,
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is master for gender
        ELSE COALESCE(ca.gen, 'n/a')                -- Fallback to ERP gender
    END AS gender,
    ci.cst_create_date   AS create_date	
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;
GO

-- ================================================================
-- DIMENSION: Products
-- Source: CRM product info enriched with ERP category details
-- Business Logic:
--   - Surrogate key generated via ROW_NUMBER()
--   - Only current/active products included (prd_end_dt IS NULL)
--   - Left join preserves products even without category matches
-- ================================================================
CREATE VIEW gold.dim_products AS 
SELECT 
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
    pn.prd_id      AS product_id,
    pn.prd_key     AS product_number,
    pn.prd_nm      AS product_name,
    pn.cat_id      AS category_id,
    pc.cat         AS category,
    pc.subcat      AS sub_category,
    pc.maintenance,
    pn.prd_cost    AS product_cost,
    pn.prd_line    AS product_line,
    pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL; -- Filter out historical data, keep only current products
GO

/*
===============================================================================
Gold Layer: Fact Table
===============================================================================
Script Purpose:
    Creates the fact view for the gold layer by joining silver layer
    transactional data with gold dimension views to build a star schema
    fact table ready for reporting and analytics.

View Created:
    - gold.fact_sales: Sales fact table linking to dim_customers and dim_products

===============================================================================
*/

-- ================================================================
-- FACT: Sales
-- Source: CRM sales details joined with gold dimension views
-- Business Logic:
--   - Links transactional sales data to product and customer dimensions
--   - Uses surrogate keys from dimension views for star schema integrity
--   - Left joins preserve all sales records even without dimension matches
--   - Grain: one row per product per order
-- ================================================================
CREATE VIEW gold.fact_sales AS 
SELECT 
    sd.sls_ord_num  AS order_number,      -- Unique order identifier
    pr.product_key,                        -- Surrogate key from dim_products
    cu.customer_key,                       -- Surrogate key from dim_customers
    sd.sls_order_dt AS order_date,         -- Date order was placed
    sd.sls_ship_dt  AS shipping_date,      -- Date order was shipped
    sd.sls_due_dt   AS due_date,           -- Expected delivery date
    sd.sls_sales    AS sales_amount,       -- Total sales value (quantity × price)
    sd.sls_quantity AS quantity,            -- Number of units sold
    sd.sls_price    AS price               -- Unit price of the product
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr             -- Join to product dimension
    ON sd.sls_prd_key = pr.product_number  -- Match on product business key
LEFT JOIN gold.dim_customers cu            -- Join to customer dimension
    ON sd.sls_cust_id = cu.customer_id;    -- Match on customer business key
GO

