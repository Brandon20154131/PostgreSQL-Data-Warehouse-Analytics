/*
===============================================================================
Data Exploration and Cleaning - Bronze to Silver Layer
===============================================================================
Script Purpose:
    This script performs data exploration and cleaning operations to transform
    raw data from the Bronze layer into clean standardised data in the Silver layer.
    
    Operations include:
    - Identifying and removing duplicate records
    - Detecting and trimming leading/trailing spaces
    - Standardising abbreviated values and codes
    - Validating data types and formats
    - Checking for NULL values and invalid data
    - Enforcing business rules and data constraints
    - Transforming foreign key relationships
    - Deriving missing values where possible
    
Usage Notes:
    - This script documents the exploration and the cleaning performed on Bronze layer data
    - Each section includes checks followed by transformation logic
    - Cleaned data is inserted into the corresponding Silver layer tables
    - Run validation queries at the end of each section to verify data quality
===============================================================================
*/



--------------------------- CRM customer info

-- displays all duplicates and NULL cst_id (primary key)
SELECT cst_id, COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 ;

-- selects primary key to look at the duplicated data
SELECT *
FROM bronze.crm_cust_info
WHERE cst_id = 29466;

-- ranks the duplicates by the creation date as we only want the latest updated entry
SELECT *, 
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as latest_update
FROM bronze.crm_cust_info
WHERE cst_id = 29466;

-- displays no duplicates
SELECT *
FROM (
	SELECT *,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as latest_update
	FROM bronze.crm_cust_info
) 
WHERE latest_update = 1;


-- checking for data with leading and trailing spaces
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

SELECT cst_marital_status
FROM bronze.crm_cust_info
WHERE cst_marital_status != TRIM(cst_marital_status);

SELECT cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);
--> we want to remove all leading and trailing spaces


-- data standardisation and consistency
-- checking the distinct values
-- we get 'F' , 'M' , [null] for gender
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;

-- we get 'S' , 'M' , [null] for marital status
SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info;
--> we want to standardise gender outputs so that 'F' = 'Female' , 'M' = 'Male' , [null] = 'unknown'
--> we want to standardise marital status outputs so that 'S' = 'Single' , 'M' = 'Married' , [null] = 'unknown'
--> we will apply UPPER() just incase mixed-case values appear later
--> we will TRIM() just incase spaces appear later

-- displays table that has been cleaned
SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) as cst_firstname,
TRIM(cst_lastname) as cst_lastname,
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
	WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	ELSE 'Unknown'
	END as cst_marital_status,
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
	WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	ELSE 'Unknown'
	END as cst_gndr,
cst_create_date
FROM (
	SELECT *,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as latest_update
	FROM bronze.crm_cust_info
) as t
WHERE t.latest_update = 1;


-- insert into silver layer
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
TRIM(cst_firstname) as cst_firstname,
TRIM(cst_lastname) as cst_lastname,
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
	WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	ELSE 'Unknown'
	END as cst_marital_status,
CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
	WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
	ELSE 'Unknown'
	END as cst_gndr,
cst_create_date
FROM (
	SELECT *,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as latest_update
	FROM bronze.crm_cust_info
) as t
WHERE t.latest_update = 1;


-- dislays cleaned table in silver layer
SELECT *
FROM silver.crm_cust_info;

-- no duplicate or null cst_id (primary key)
SELECT cst_id, COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 ;

-- no entries with leading and trailing spaces
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

SELECT cst_marital_status
FROM silver.crm_cust_info
WHERE cst_marital_status != TRIM(cst_marital_status);

SELECT cst_gndr
FROM silver.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);

-- entries have been unabbreviated 
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info;


--------------------------- CRM product info

-- no duplicates or nulls
SELECT prd_id, COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- getting category id from product key 
SELECT *, SUBSTRING(prd_key,1,5) as cat_id
FROM bronze.crm_prd_info;

-- We create the cat_id column because it will serve as a foreign key 
-- that references the primary key 'id' in the parent table erp_px_cat_g1v2
SELECT *
FROM bronze.erp_px_cat_g1v2;

-- getting shortened product key from product key
SELECT *, SUBSTRING(prd_key,7,LENGTH(prd_key)) as prd_key
FROM bronze.crm_prd_info;
-- We create the prd_key column because it will serve as a foreign key 
-- that references the primary key 'sls_prd_key' in the parent table crm_sales_details
SELECT *
FROM bronze.crm_sales_details;

-- checking leading and trailing spaces
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- checking for NULLs or negative numbers
SELECT *
FROM bronze.crm_prd_info
WHERE prd_cost<0 or prd_cost IS NULL;
-- 2 NULLS
-- use COALESCE(column_name, 'replacement_value') to replace NULL values with 0

-- standardisation
-- checking the distinct values
SELECT DISTINCT(prd_line)
FROM bronze.crm_prd_info;
-- we get 'M' , 'R' , 'S' , 'T' , [null]
--> we want to standardise product line outputs so that 'S' = 'other Sales' , 'M' = 'Mountain' , 'T' = 'Touring', 'R' = 'Road', [null] = 'unknown'
--> we will apply UPPER() just incase mixed-case values appear later
--> we will TRIM() just incase spaces appear later

-- check for invalid date orders
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;
-- we get a few entries where the start date happens after the end date which doesnt make sense

SELECT
prd_id,
prd_key,
prd_nm,
CAST(prd_start_dt as DATE),
CAST(LEAD (prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)  - INTERVAL '1 day' as DATE) as prd_end_dt_test
FROM bronze.crm_prd_info;



SELECT
prd_id,
REPLACE(SUBSTRING(prd_key,1,5), '-','_') as cat_id,
SUBSTRING(prd_key,7,LENGTH(prd_key)) as prd_key,
prd_nm,
COALESCE(prd_cost,0) as prd_cost,
CASE UPPER(TRIM(prd_line))  
	WHEN 'R' THEN 'Road'
	WHEN 'M' THEN 'Mountain'
	WHEN 'S' THEN 'other Sales'
	WHEN 'T' THEN 'Touring'
	ELSE 'Unknown'
	END as prd_line,
CAST(prd_start_dt as DATE),
CAST(LEAD (prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)  - INTERVAL '1 day' as DATE) as prd_end_dt_test
FROM bronze.crm_prd_info;



--------------------------- CRM sales details
SELECT * 
FROM bronze.crm_sales_details;

-- check for leading and trailing spaces
SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num);


-- sls_prd_key is a primary key that is refered to as prd_key FROM silver.crm_prd_info
SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info);
-- all sls_prd_key records match prd_key FROM silver.crm_prd_info

-- sls_cust_id is a primary key that is refered to as prd_key FROM silver.crm_cst_info
SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info);
-- all sls_cust_id records match cst_id FROM silver.crm_cust_info

-- changing integar dates to DATE datatypes
-- check for negative
SELECT sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt < 0;

SELECT sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt < 0;

SELECT sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt < 0;

-- check for dates that = 0
SELECT sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt = 0;

SELECT sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt = 0;

SELECT sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt = 0;

-- replace values with NULLs
SELECT NULLIF(sls_order_dt, 0) as sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt = 0;
-- 0 converted to nulls

-- integars are formated as '20101229' which is 8 figures, check to see if any records do not = 8 figures (YYYYMMDD)
SELECT sls_order_dt
FROM bronze.crm_sales_details
WHERE LENGTH(sls_order_dt::text) !=8;
-- 2 records found that do not = 8 figures

-- checking for invalid date orders
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_ship_dt > sls_due_dt;
-- no invalid dates, dates are ordered correctly

-- business rules
-- slaes = quanity x price
-- no negative, 0s or NULLs are allowed
SELECT DISTINCT sls_sales, sls_quantity, sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity*sls_price
OR sls_sales IS NULL  
OR sls_quantity IS NULL 
OR sls_price IS NULL
OR sls_sales <= 0 
OR sls_quantity <=0 
OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price
;

-- data enrichment rules
-- if sales is negative, 0 or NULLS. derive it using quantity and price
-- if price is 0 or NULL, calculate it using sales and quantity
-- if price is negative, convert it to positive value

SELECT 
CASE 
	WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
END as sls_sales,
CASE
	WHEN sls_price IS NULL OR sls_price <= 0
		THEN sls_sales/NULLIF(sls_quantity,0)
	ELSE sls_price
END as sls_price, 
sls_quantity
FROM bronze.crm_sales_details;


SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN LENGTH(sls_order_dt :: text) != 8 OR sls_order_dt = 0 THEN NULL
	ELSE CAST(CAST(sls_order_dt as VARCHAR) as DATE)
END as sls_order_dt,
CASE WHEN LENGTH(sls_ship_dt :: text) != 8 OR sls_ship_dt = 0 THEN NULL
	ELSE CAST(CAST(sls_ship_dt as VARCHAR) as DATE)
END as sls_ship_dt,
CASE WHEN LENGTH(sls_due_dt :: text) != 8 OR sls_due_dt = 0 THEN NULL
	ELSE CAST(CAST(sls_due_dt as VARCHAR) as DATE)
END as sls_due_dt,
CASE 
	WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
END as sls_sales,
sls_quantity,
CASE
	WHEN sls_price IS NULL OR sls_price <= 0
		THEN sls_sales/NULLIF(sls_quantity,0)
	ELSE sls_price
END as sls_price
FROM bronze.crm_sales_details;

SELECT *
FROM bronze.crm_sales_details;



-- erp customer (az12)
-- cid primary key to cst_key from crm_cust_info
SELECT *
FROM bronze.erp_cust_az12;
-- some cids have NAS infront of it so we need to standardise it by remove NAS from the records
SELECT 
CASE 
	WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LENGTH(cid))
	ELSE cid
END as cid,
bdate,
gen
FROM bronze.erp_cust_az12;

-- checking out of range birth dates; people born in the future
SELECT 
CASE 
	WHEN bdate > NOW()::date THEN NULL 
	ELSE bdate
END as bdate
FROM bronze.erp_cust_az12
ORDER BY bdate DESC;

-- standardising genders
SELECT DISTINCT gen
FROM bronze.erp_cust_az12;
-- we get a mixture of f, female, m, male, blank, null values
SELECT DISTINCT
CASE 
	WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
	WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
	ELSE 'Unknown'
END as gen
FROM bronze.erp_cust_az12;

	





SELECT
CASE 
	WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LENGTH(cid))
	ELSE cid
END as cid,
CASE 
	WHEN bdate > NOW()::date THEN NULL 
	ELSE bdate
END as bdate,
CASE 
	WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
	WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
	ELSE 'Unknown'
END as gen
FROM bronze.erp_cust_az12;

SELECT*
FROM silver.erp_cust_az12
ORDER BY bdate DESC;



-- erp location a101

SELECT
REPLACE(cid,'-','') as cid
FROM bronze.erp_loc_a101;

SELECT DISTINCT cntry
FROM bronze.erp_loc_a101;




SELECT 
CASE 
	WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
	WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'Unkown'
	ELSE TRIM(cntry)
END as cntry
FROM bronze.erp_loc_a101;

SELECT
REPLACE(cid,'-','') as cid
FROM bronze.erp_loc_a101;

SELECT *
FROM bronze.erp_loc_a101;

SELECT *
FROM silver.erp_loc_a101;


-- erp_px_cat_g1v2

-- checking leading and trailing spaces
SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)
;

-- standardisation
SELECT DISTINCT cat
FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT subcat
FROM bronze.erp_px_cat_g1v2;

SELECT DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2;






SELECT 
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2;

SELECT 
id,
cat,
subcat,
maintenance
FROM silver.erp_px_cat_g1v2;
