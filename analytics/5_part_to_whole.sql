/*
===============================================================================
Part-to-Whole Analysis
===============================================================================
Purpose:
    - To measure how individual segments or entities contribute to a larger total.
    - To identify which categories, regions or customer groups are driving overall performance.
    - Useful for understanding distribution, contribution ratios and proportional impact within the business.

Key SQL Functions Used:
    - Window Functions: SUM() OVER() for calculating total sums across partitions.
    - Arithmetic Operations: Division for percentage calculations.
    - String Functions: CONCAT() for formatting percentage outputs.
    - Conditional Logic: CASE for customer tier segmentation.
===============================================================================
*/


-- ============================================================================
--    Category Contribution to Overall Sales
--    Purpose: Determine which product categories generate the highest share of total sales revenue.
WITH category_sales AS (
    SELECT
        p.category,
        SUM(f.sales_amount) AS total_amount
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p
        ON p.product_key = f.product_key
    GROUP BY p.category
)
SELECT 
    category,
    total_amount,
    SUM(total_amount) OVER() AS overall_amount,
    CONCAT(ROUND((total_amount / SUM(total_amount) OVER()) * 100, 2), '%') AS percentage_of_total
FROM category_sales
ORDER BY total_amount DESC;


-- ============================================================================
--    Regional Contribution to Overall Sales
--    Purpose: Identify which countries contribute most to total sales revenue.
WITH country_sales AS (
    SELECT
        c.country,
        SUM(f.sales_amount) AS total_amount
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_customers c
        ON f.customer_key = c.customer_key
    GROUP BY c.country
)
SELECT 
    country,
    total_amount,
    SUM(total_amount) OVER() AS overall_amount,
    CONCAT(ROUND((total_amount / SUM(total_amount) OVER()) * 100, 2), '%') AS percentage_of_total
FROM country_sales
ORDER BY total_amount DESC;


-- ============================================================================
--    Gender Contribution to Overall Sales
--    Purpose: Measure sales contribution by gender demographics to understand purchasing trends across customer segments.
WITH gender_sales AS (
    SELECT
        c.gender,
        SUM(f.sales_amount) AS total_amount
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_customers c
        ON f.customer_key = c.customer_key
    GROUP BY c.gender
)
SELECT 
    gender,
    total_amount,
    SUM(total_amount) OVER() AS overall_amount,
    CONCAT(ROUND((total_amount / SUM(total_amount) OVER()) * 100, 2), '%') AS percentage_of_total
FROM gender_sales
ORDER BY total_amount DESC;


-- ============================================================================
--    Customer Tier Contribution to Overall Sales
--    Purpose:
--        - Categorize customers into tiers based on spending and lifespan.
--        - Analyze which customer segments (VIP, Regular, New) contribute most to total sales.
WITH customer_spending AS (
    SELECT 
        c.customer_key,
        SUM(f.sales_amount) AS total_amount,
        MIN(f.order_date) AS first_order,
        MAX(f.order_date) AS last_order,
        EXTRACT(YEAR FROM AGE(MAX(f.order_date), MIN(f.order_date))) * 12 + 
        EXTRACT(MONTH FROM AGE(MAX(f.order_date), MIN(f.order_date))) AS lifespan
    FROM gold.fact_sales f
    JOIN gold.dim_customers c
        ON f.customer_key = c.customer_key
    GROUP BY c.customer_key
),
customer_segment AS (
    SELECT 
        customer_key,
        total_amount,
        lifespan,
        CASE 
            WHEN lifespan >= 12 AND total_amount >= 5000 THEN 'VIP Customer'
            WHEN lifespan >= 12 AND total_amount < 5000 THEN 'Regular Customer'
            ELSE 'New Customer'
        END AS customer_tier
    FROM customer_spending
)
SELECT 
    customer_tier,
    COUNT(customer_key) AS total_customers,
    SUM(total_amount) AS total_amount,
    CONCAT(ROUND((SUM(total_amount) / SUM(SUM(total_amount)) OVER()) * 100, 2), '%') AS percentage_of_total
FROM customer_segment
GROUP BY 1
ORDER BY 4 DESC;
