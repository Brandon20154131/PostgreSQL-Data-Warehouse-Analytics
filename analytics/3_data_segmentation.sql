/*
===============================================================================
Data Segmentation Analysis
===============================================================================
Purpose:
    - To group entities (products, customers, etc.) into logical segments based on defined numeric or behavioral ranges.
    - To better understand the relationship between different measures such as cost, revenue and customer longevity.
    - To enable targeted marketing, pricing, and operational strategies by identifying patterns within each segment.

Key SQL Functions Used:
    - CASE: For defining range-based or conditional segments.
    - DATE and AGE functions: To calculate customer lifespans.
    - Aggregate Functions: SUM(), COUNT(), MIN(), MAX() for grouping and metrics.
===============================================================================
*/


-- ============================================================================
--    Product Segmentation by Cost Range
--    Purpose: To categorize products into cost-based ranges and analyze how many products fall within each price segment.
WITH product_segment AS (
    SELECT
        product_key,
        product_name,
        cost,
        CASE
            WHEN cost < 100 THEN 'Below 100'
            WHEN cost BETWEEN 100 AND 500 THEN '100-500'
            WHEN cost BETWEEN 501 AND 1000 THEN '501-1000'
            ELSE 'Above 1000'
        END AS cost_range
    FROM gold.dim_products
)
SELECT
    cost_range,
    COUNT(product_key) AS total_products
FROM product_segment
GROUP BY cost_range
ORDER BY total_products DESC;


-- ============================================================================
--    Customer Segmentation by Spending and Lifespan
--    Purpose: To classify customers into tiers based on spending behavior and relationship duration with the company.
--              - VIP: ≥12 months lifespan and spending > £5,000
--              - Regular: ≥12 months lifespan and spending ≤ £5,000
--              - New: <12 months lifespan
WITH customer_spending AS (
    SELECT 
        c.customer_key,
        SUM(f.sales_amount) AS total_spending,
        MIN(f.order_date) AS first_order,
        MAX(f.order_date) AS last_order,
        EXTRACT(YEAR FROM AGE(MAX(f.order_date), MIN(f.order_date))) * 12 
            + EXTRACT(MONTH FROM AGE(MAX(f.order_date), MIN(f.order_date))) AS lifespan
    FROM gold.fact_sales f
    JOIN gold.dim_customers c
        ON f.customer_key = c.customer_key
    GROUP BY c.customer_key
),
customer_segment AS (
    SELECT 
        customer_key,
        total_spending,
        lifespan,
        CASE 
            WHEN lifespan >= 12 AND total_spending >= 5000 THEN 'VIP Customer'
            WHEN lifespan >= 12 AND total_spending < 5000 THEN 'Regular Customer'
            ELSE 'New Customer'
        END AS customer_tier
    FROM customer_spending
)
SELECT 
    customer_tier,
    COUNT(customer_key) AS total_customers
FROM customer_segment
GROUP BY 1
ORDER BY 2 DESC;
