/*
===============================================================================
Customer Report
===============================================================================
Purpose:
    - To consolidate key customer metrics and behavioral insights into one 
      analytical view.
    - To provide a foundation for customer segmentation, KPI tracking, and 
      retention analysis.

Highlights:
    1. Gathers essential customer and transaction fields such as names, ages, 
       and sales metrics.
    2. Segments customers by both spending tiers (VIP, Regular, New) and 
       demographic age groups.
    3. Aggregates customer-level metrics:
        • Total orders
        • Total sales
        • Total quantity purchased
        • Total products purchased
        • Lifespan (in months)
    4. Computes key performance indicators (KPIs):
        • Recency (months since last purchase)
        • Average order value (AOV)
        • Average monthly spend (AMS)

Key SQL Functions Used:
    - Aggregates: SUM(), COUNT(), MAX(), MIN() for summarizing behavior.
    - Window & Date Functions: AGE(), EXTRACT(), NOW() for lifespan and recency.
    - Conditional Logic: CASE for segmentation into tiers and age brackets.
===============================================================================
*/


-- ============================================================================
--    Create Customer Report View
--    Purpose: To create a reusable gold-layer view with customer-level 
--             performance and behavioral insights.
CREATE VIEW gold.report_customers AS
WITH base_query AS ( -- Base Query: Retrieves essential transaction and customer fields
    SELECT
        f.order_number,
        f.product_key,
        f.order_date,
        f.sales_amount,
        f.quantity,
        c.customer_key,
        c.customer_number,
        CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
        EXTRACT(YEAR FROM AGE(NOW(), c.birthdate)) AS age
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_customers c
        ON c.customer_key = f.customer_key
    WHERE f.order_date IS NOT NULL
),
customer_aggregation AS ( -- Customer Aggregation: Summarizes core performance metrics
    SELECT
        customer_key,
        customer_number,
        customer_name,
        age,
        COUNT(DISTINCT order_number) AS total_orders,
        SUM(sales_amount) AS total_sales,
        SUM(quantity) AS total_quantity,
        COUNT(DISTINCT product_key) AS total_products,
        MAX(order_date) AS last_order_date,
        EXTRACT(YEAR FROM AGE(MAX(order_date), MIN(order_date))) * 12 + 
        EXTRACT(MONTH FROM AGE(MAX(order_date), MIN(order_date))) AS lifespan
    FROM base_query
    GROUP BY 1, 2, 3, 4
)
SELECT	
    customer_key,
    customer_number,
    customer_name,
    age,
    CASE 
        WHEN age < 20 THEN 'Under 20'
        WHEN age BETWEEN 20 AND 29 THEN '20-29'
        WHEN age BETWEEN 30 AND 39 THEN '30-39'
        WHEN age BETWEEN 40 AND 49 THEN '40-49'
        ELSE '50 and Above'
    END AS age_bracket, -- Demographic Segmentation by Age Bracket
    CASE 
        WHEN lifespan >= 12 AND total_sales >= 5000 THEN 'VIP Customer'
        WHEN lifespan >= 12 AND total_sales < 5000 THEN 'Regular Customer'
        ELSE 'New Customer'
    END AS customer_tier,-- Customer Tier Segmentation by Spend & Lifespan
    last_order_date,
    (EXTRACT(YEAR FROM AGE(NOW(), last_order_date)) * 12) +
    EXTRACT(MONTH FROM AGE(NOW(), last_order_date)) AS recency,
    total_orders,
    total_sales,
    total_quantity,
    total_products,
    lifespan,
    ROUND(
        CASE
            WHEN total_orders = 0 THEN 0
            ELSE total_sales / total_orders
        END, 2
    ) AS avg_order_value,  -- Average Order Value (AOV)
    ROUND(
        CASE
            WHEN lifespan = 0 THEN total_sales
            ELSE total_sales / lifespan
        END, 2
    ) AS avg_monthly_spend  -- Average Monthly Spend (AMS)
FROM customer_aggregation;


-- ============================================================================
--    Validation Query: Preview Report Output
SELECT *
FROM gold.report_customers;
