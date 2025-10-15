/*
===============================================================================
Performance Analysis
===============================================================================
Purpose:
    - To evaluate and compare performance across key dimensions such as customers, products, categories and time periods.
    - To identify top-performing customers or products and assess their year-over-year growth and performance against averages.
    - To support business insights like customer retention, product success and category contribution.

Key SQL Functions Used:
    - Window Functions: ROW_NUMBER(), LAG(), AVG(), PARTITION BY for ranking and comparative metrics.
    - Aggregate Functions: SUM(), COUNT(), ROUND() for performance measurement.
    - Conditional Logic: CASE for classifying performance trends.
===============================================================================
*/


-- ============================================================================
--    Top 5 Customers by Total Spend Each Year
--    Purpose: Identify the customers contributing the most revenue annually.
WITH yearly_customer_sales AS (
    SELECT 
        EXTRACT(YEAR FROM f.order_date) AS order_year,
        c.customer_key,
        CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
        SUM(f.sales_amount) AS total_amount,
        ROW_NUMBER() OVER (
            PARTITION BY EXTRACT(YEAR FROM f.order_date) 
            ORDER BY SUM(f.sales_amount) DESC
        ) AS rank
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_customers c
        ON f.customer_key = c.customer_key
    WHERE f.order_date IS NOT NULL
    GROUP BY 1, 2, 3
)
SELECT 
    order_year,
    customer_key,
    customer_name,
    total_amount,
    rank
FROM yearly_customer_sales
WHERE rank <= 5
ORDER BY 1, rank;


-- ============================================================================
--    Top 5 Customers by Total Quantity Purchased Each Year
--    Purpose: Highlight customers purchasing the highest product volumes annually, regardless of total spend.
WITH yearly_customer_sales AS (
    SELECT 
        EXTRACT(YEAR FROM f.order_date) AS order_year,
        c.customer_key,
        CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
        SUM(f.quantity) AS total_quantity,
        ROW_NUMBER() OVER (
            PARTITION BY EXTRACT(YEAR FROM f.order_date) 
            ORDER BY SUM(f.quantity) DESC
        ) AS rank
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_customers c
        ON f.customer_key = c.customer_key
    WHERE f.order_date IS NOT NULL
    GROUP BY 1, 2, 3
)
SELECT 
    order_year,
    customer_key,
    customer_name,
    total_quantity,
    rank
FROM yearly_customer_sales
WHERE rank <= 5
ORDER BY 1, rank;


-- ============================================================================
--    Sales and Quantity by Category and Subcategory
--    Purpose: Assess category-level performance to identify which subcategories contribute most to total sales and quantity.
SELECT 
    p.category,
    p.subcategory,
    SUM(f.sales_amount) AS total_amount,
    SUM(f.quantity) AS total_quantity
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
    ON f.product_key = p.product_key
WHERE f.order_date IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 3 DESC;


-- ============================================================================
--    Yearly Product Performance vs. Average and Previous Year
--    Purpose:
--        - Compare each product’s yearly performance against its own historical average.
--        - Identify whether sales increased or decreased compared to the previous year.
WITH year_product_sales AS (
    SELECT 
        EXTRACT(YEAR FROM f.order_date) AS order_year, 
        p.product_name, 
        SUM(f.sales_amount) AS current_sales
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p
        ON f.product_key = p.product_key
    WHERE f.order_date IS NOT NULL
    GROUP BY 1, 2
)
SELECT
    order_year,
    product_name,
    current_sales,
    
    -- Compare to historical average
    ROUND(AVG(current_sales) OVER (PARTITION BY product_name), 2) AS avg_sales,
    ROUND(current_sales - AVG(current_sales) OVER (PARTITION BY product_name), 2) AS difference_avg,
    CASE 
        WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) > 0 THEN 'Above Avg'
        WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) < 0 THEN 'Below Avg'
        ELSE 'Avg'
    END AS avg_change,

    -- Compare to previous year
    LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS prev_year_sales,
    current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS difference_sales,
    CASE 
        WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) > 0 THEN 'Increase'
        WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) < 0 THEN 'Decrease'
        ELSE 'No change'
    END AS prev_change
FROM year_product_sales
ORDER BY 2, 1;


-- ============================================================================
--    Profit Margins 
--    Purpose: show profit margins each product makes
SELECT
    p.category,
    p.subcategory,
    p.product_name,
    SUM(f.sales_amount) as total_revenue,
    SUM(f.quantity) as total_quantity,
    MAX(f.price) as price,
    MAX(p.cost) as cost,
    SUM(f.quantity * (f.price - p.cost)) as profit,
    CONCAT(ROUND(((SUM(f.quantity * (f.price - p.cost))::DECIMAL / SUM(f.sales_amount)) * 100),2),'%') AS profit_margin
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON f.product_key = p.product_key
GROUP BY 1,2,3
ORDER BY 1,2,3;
