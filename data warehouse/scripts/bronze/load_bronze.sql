    -- crm_cust_info
	TRUNCATE TABLE bronze.crm_cust_info;
    COPY bronze.crm_cust_info
    FROM '/Users/id20154131/Desktop/project/portfolio/sql_data_warehouse/Datasets/source_crm/cust_info.csv'
    WITH (FORMAT csv, HEADER true);

    -- crm_prd_info
    TRUNCATE TABLE bronze.crm_prd_info;
    COPY bronze.crm_prd_info
    FROM '/Users/id20154131/Desktop/project/portfolio/sql_data_warehouse/Datasets/source_crm/prd_info.csv'
    WITH (FORMAT csv, HEADER true);
  
    -- crm_sales_details
    TRUNCATE TABLE bronze.crm_sales_details;
    COPY bronze.crm_sales_details
    FROM '/Users/id20154131/Desktop/project/portfolio/sql_data_warehouse/Datasets/source_crm/sales_details.csv'
    WITH (FORMAT csv, HEADER true);

    -- erp_loc_a101
    TRUNCATE TABLE bronze.erp_loc_a101;
    COPY bronze.erp_loc_a101
    FROM '/Users/id20154131/Desktop/project/portfolio/sql_data_warehouse/Datasets/source_erp/loc_a101.csv'
    WITH (FORMAT csv, HEADER true);
  
    -- erp_cust_az12
    TRUNCATE TABLE bronze.erp_cust_az12;
    COPY bronze.erp_cust_az12
    FROM '/Users/id20154131/Desktop/project/portfolio/sql_data_warehouse/Datasets/source_erp/cust_az12.csv'
    WITH (FORMAT csv, HEADER true);
   
    -- erp_px_cat_g1v2
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    COPY bronze.erp_px_cat_g1v2
    FROM '/Users/id20154131/Desktop/project/portfolio/sql_data_warehouse/Datasets/source_erp/px_cat_g1v2.csv'
    WITH (FORMAT csv, HEADER true);

SELECT COUNT(*)
FROM bronze.crm_cust_info;

SELECT COUNT(*) 
FROM bronze.crm_prd_info;

SELECT COUNT(*) 
FROM bronze.crm_sales_details;

SELECT COUNT(*)
FROM bronze.erp_loc_a101;

SELECT COUNT(*)
FROM bronze.erp_cust_az12;

SELECT *
FROM bronze.erp_px_cat_g1v2;
