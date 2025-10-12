SELECT *
FROM silver.crm_cust_info;

-- creating customer entity
SELECT
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	ci.cst_gndr,
	ci.cst_create_date,
	ca.bdate,
	ca.gen,
	la.cntry
fROM silver.crm_cust_info as ci
LEFT JOIN silver.erp_cust_az12 as ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 as la
ON ci.cst_key = la.cid
;

-- testing for duplicates
SELECT cst_id
FROM (SELECT
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	ci.cst_gndr,
	ci.cst_create_date,
	ca.bdate,
	ca.gen,
	la.cntry
fROM silver.crm_cust_info as ci
LEFT JOIN silver.erp_cust_az12 as ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 as la
ON ci.cst_key = la.cid)
GROUP BY cst_id
HAVING COUNT(*) > 1;

-- data integration
-- we will use crm as the master table incase the genders from ci.cst_gen and ca.gen are different
-- when the gender is unknown from ci.cst_gen, we will use ca.gen 
SELECT
	ci.cst_gndr,
	ca.gen,
	CASE 
		WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr -- CRM is the Master for gender information
		ELSE COALESCE(ca.gen,'Unknown') 
	END as new_gender
fROM silver.crm_cust_info as ci
LEFT JOIN silver.erp_cust_az12 as ca
ON ci.cst_key = ca.cid
;


-- implement gender changes, rearrange column order and rename columns for better readability
-- create view
CREATE VIEW gold.dim_customers AS
SELECT
	ROW_NUMBER() OVER(ORDER BY ci.cst_id) as customer_key, -- surrogate key 
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname as Last_name,
	CASE 
		WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr -- CRM is the Master for gender information
		ELSE COALESCE(ca.gen,'Unknown') 
	END as gender,
	ca.bdate as birthdate,
	ci.cst_marital_status as marital_status,
	la.cntry as country,
	ci.cst_create_date as create_date
fROM silver.crm_cust_info as ci
LEFT JOIN silver.erp_cust_az12 as ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 as la
ON ci.cst_key = la.cid;

-- test dimension customer view
SELECT *
FROM gold.dim_customers;


-- creating product entity
SELECT
	pn.prd_id,
	pn.cat_id,
	pn.prd_key,
	pn.prd_nm,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt,
	pn.prd_end_dt,
FROM silver.crm_prd_info pn;

-- we only want current products so not products that have ended production
SELECT prd_key, COUNT(*)
FROM (
	SELECT
		pn.prd_id,
		pn.cat_id,
		pn.prd_key,
		pn.prd_nm,
		pn.prd_cost,
		pn.prd_line,
		pn.prd_start_dt,
		pc.cat,
		pc.subcat,
		pc.maintenance
	FROM silver.crm_prd_info pn
	LEFT JOIN silver.erp_px_cat_g1v2 pc
	ON pn.cat_id = pc.id
	WHERE prd_end_dt IS NULL -- NULL production end date means its still in production
)
GROUP BY prd_key
HAVING COUNT(*) > 1; -- testing for duplicates

-- rearrange column order and rename columns for better readability
SELECT
ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) as product_key -- surrogate key
	pn.prd_id as product_id,
	pn.prd_key as product_number,
	pn.prd_nm as product_name,
	pn.cat_id as category_id,
	pc.cat as category,
	pc.subcat as subcategory,
	pc.maintenance,
	pn.prd_cost as cost,
	pn.prd_line as product_line,
	pn.prd_start_dt as start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL;

-- test dimension product view
SELECT *
FROM gold.dim_products;


-- create order entity
SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM silver.crm_sales_details;


-- building Fact sales
-- using the dimensions surrogate keys instead of IDs to easily connect facts with dimensions
-- data look up
-- rearrange column order and rename columns for better readability
-- organise table: dimension keys - dates - measures
SELECT 
    sd.sls_ord_num as order_number,
	pr.product_key,
	cu.customer_key,
    sd.sls_order_dt as order_date,
    sd.sls_ship_dt as shipping_date,
    sd.sls_due_dt as due_date,
    sd.sls_sales as sales_amount,
    sd.sls_quantity as quantity,
    sd.sls_price as price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id;

-- test fact sales
SELECT *
FROM gold.fact_sales;

-- check if all dimension tables can successfully join to the fact table
SELECT *
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
WHERE c.customer_key IS NULL; -- everything is matching

SELECT *
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL; -- everything is matching

