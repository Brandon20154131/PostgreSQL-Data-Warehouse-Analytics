/*
===============================================================================
Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This sript performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
===============================================================================
*/


-- Loading crm_cust_info
TRUNCATE TABLE silver.crm_cust_info;

INSERT INTO silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
)
SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) as cst_firstname, -- removed white spaces
TRIM(cst_lastname) as cst_lastname, -- removed white spaces
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
	WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	ELSE 'Unknown'
	END as cst_marital_status, -- Normalised marital status values to readable format
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
	WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	ELSE 'Unknown'
	END as cst_gndr, -- Normalised gender values to readable format
cst_create_date
FROM (
	SELECT *,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as latest_update
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
)
WHERE latest_update = 1;



-- Loading crm_prd_info
TRUNCATE TABLE silver.crm_prd_info;

INSERT INTO silver.crm_prd_info (
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)
SELECT
prd_id,
REPLACE(SUBSTRING(prd_key,1,5), '-','_') as cat_id, -- Derived column, extract category ID
SUBSTRING(prd_key,7,LENGTH(prd_key)) as prd_key, -- Derived column, extract product key
prd_nm,
COALESCE(prd_cost,0) as prd_cost, -- Removed records with null product cost
CASE UPPER(TRIM(prd_line))  
	WHEN 'R' THEN 'Road'
	WHEN 'M' THEN 'Mountain'
	WHEN 'S' THEN 'other Sales'
	WHEN 'T' THEN 'Touring'
	ELSE 'Unknown'
	END as prd_line, -- Map product line codes to descriptive values
CAST(prd_start_dt as DATE), -- Data type casting
CAST(LEAD (prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)  - INTERVAL '1 day' as DATE) as prd_end_dt -- Data Enrichment - Calculate end date as one day before the next start date
FROM bronze.crm_prd_info;



-- Loading crm sales details
TRUNCATE TABLE silver.crm_sales_details;

INSERT INTO silver.crm_sales_details (
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)
SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN LENGTH(sls_order_dt :: text) != 8 OR sls_order_dt = 0 THEN NULL -- handling invalid date
	ELSE CAST(CAST(sls_order_dt as VARCHAR) as DATE) -- datatype casting
END as sls_order_dt,
CASE WHEN LENGTH(sls_ship_dt :: text) != 8 OR sls_ship_dt = 0 THEN NULL
	ELSE CAST(CAST(sls_ship_dt as VARCHAR) as DATE)
END as sls_ship_dt,
CASE WHEN LENGTH(sls_due_dt :: text) != 8 OR sls_due_dt = 0 THEN NULL
	ELSE CAST(CAST(sls_due_dt as VARCHAR) as DATE)
END as sls_due_dt,
CASE 
	WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) -- handling missing and invalid data by deriving from existing data
		THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
END as sls_sales, -- recalculate sales if original values is missing or incorrect
sls_quantity,
CASE
	WHEN sls_price IS NULL OR sls_price <= 0
		THEN sls_sales/NULLIF(sls_quantity,0)
	ELSE sls_price -- derive price if original value is invalid
END as sls_price
FROM bronze.crm_sales_details;



-- Loading erp customer (az12)
TRUNCATE TABLE silver.erp_cust_az12;

INSERT INTO silver.erp_cust_az12(
cid,
bdate,
gen
)
SELECT
CASE 
	WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LENGTH(cid)) -- removed 'NAS' prefix 
	ELSE cid
END as cid,
CASE 
	WHEN bdate > NOW()::date THEN NULL 
	ELSE bdate
END as bdate, -- set future birthdates to NULL
CASE 
	WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
	WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
	ELSE 'Unknown'
END as gen -- normalised gender values and handled unknown cases
FROM bronze.erp_cust_az12;



-- Loading erp location (a101)
TRUNCATE TABLE silver.erp_loc_a101;

INSERT INTO silver.erp_loc_a101(
cid,
cntry
)
SELECT
REPLACE(cid,'-','') as cid,
CASE 
	WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
	WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'Unkown'
	ELSE TRIM(cntry)
END as cntry
FROM bronze.erp_loc_a101;



-- Loading product category (g1v2)
TRUNCATE TABLE silver.erp_px_cat_g1v2;

INSERT INTO silver.erp_px_cat_g1v2(
id,
cat,
subcat,
maintenance
)
SELECT 
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2; 
