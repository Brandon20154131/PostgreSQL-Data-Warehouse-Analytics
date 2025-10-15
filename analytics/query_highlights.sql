/*
===============================================================================
QUERY HIGHLIGHTS
===============================================================================
Overview:
    Contains six curated analytical queries forming the foundation of the 
    Gold Layer in a modern data warehouse. Each query transforms curated 
    transactional data into high-value, insight-ready datasets for reporting, 
    business intelligence and performance monitoring.

Included Analyses:
	1️	Profit margins 
		- Shows the profit margins each product makes
		
    2️ Rolling 3-Month Average
        - Smooths short-term fluctuations and reveals underlying trends.

    3️ Sales and Quantity by Category and Subcategory
        - Breaks down category-level performance to identify top revenue drivers.

    4️ Yearly Product Performance vs. Average and Previous Year
        - Combines moving averages and lag functions to evaluate product trends 
          against both historical averages and prior year performance.

    5️ Customer Tier Contribution to Overall Sales
        - Segments customers by tenure and total spend (VIP, Regular, New) and 
          measures each segment’s contribution to total revenue.

    6️ Customer Report View
        - Builds a reusable analytical view consolidating customer behavior 
          (recency, frequency, value) and segmentation insights.

    7️ Product Report View
        - Aggregates core product-level KPIs such as revenue, recency, and 
          sales frequency, with performance tier classification.

Key Highlights:
    • Modular CTE-based design for clarity and scalability.
    • Advanced SQL constructs:
        - Window Functions (LAG, LEAD, AVG OVER, ROWS)
        - Date & Time Functions (AGE, EXTRACT, DATE_TRUNC)
        - Conditional Logic (CASE)
===============================================================================
*/



-- ============================================================================
--    Profit margins 
--    Purpose: Shows the profit margins each product makes
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


-- ============================================================================
--    Rolling 3-Month Average
--    Purpose: To smooth out short-term fluctuations and reveal underlying trends.
--    Granularity: Monthly
--    Rolling Window: 3 months (current + 2 preceding months)
SELECT 
    DATE_TRUNC('month', order_date)::date AS order_month,
    SUM(sales_amount) AS total_sales,
    ROUND(
        AVG(SUM(sales_amount)) OVER (
            ORDER BY DATE_TRUNC('month', order_date)::date
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 2
    ) AS rolling_3_month_avg
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATE_TRUNC('month', order_date)::date
ORDER BY 1;

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
    ROUND(AVG(current_sales) OVER (PARTITION BY product_name), 2) AS avg_sales,
    ROUND(current_sales - AVG(current_sales) OVER (PARTITION BY product_name), 2) AS difference_avg,
    CASE 
        WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) > 0 THEN 'Above Avg'
        WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) < 0 THEN 'Below Avg'
        ELSE 'Avg'
    END AS avg_change,
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
--    Customer Tier Contribution to Overall Sales
--    Purpose:
--        - Categorize customers into tiers based on spending and lifespan.
--              - VIP: ≥12 months lifespan and spending > £5,000
--              - Regular: ≥12 months lifespan and spending ≤ £5,000
--              - New: <12 months lifespan
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

/*
===============================================================================
Customer Report
===============================================================================
Purpose:
    - To consolidate key customer metrics and behavioral insights into one analytical view.
    - To provide a foundation for customer segmentation, KPI tracking and retention analysis.

Highlights:
    1. Gathers essential customer and transaction fields such as names, ages and sales metrics.
    2. Segments customers by both spending tiers (VIP, Regular, New) and demographic age groups.
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
--    Purpose: To create a reusable gold-layer view with customer-level performance and behavioral insights.
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

/*
===============================================================================
Product Report
===============================================================================
Purpose:
    - To consolidate key product-level metrics and behavioral insights into one analytical view.
    - Enables performance analysis by category, product tier, and time-based sales patterns.

Highlights:
    1. Gathers essential product details such as product name, category, subcategory and cost.
    2. Segments products by total revenue to identify:
        • High performers
        • Mid performers
        • Low performers
    3. Aggregates core product-level metrics:
        • Total orders
        • Total sales
        • Total quantity sold
        • Total unique customers
        • Lifespan (in months)
    4. Computes valuable KPIs:
        • Recency (months since last sale)
        • Average selling price (ASP)
        • Average order revenue (AOR)
        • Average monthly revenue (AMR)

Key SQL Functions Used:
    - Aggregations: SUM(), COUNT(), MAX(), MIN(), AVG()
    - Time-based: AGE(), EXTRACT(), NOW()
    - Conditional logic: CASE for product tier segmentation
===============================================================================
*/


-- ============================================================================
--    Create Product Report View
--    Purpose: To build a reusable gold-layer view summarizing product-level performance and sales KPIs.
CREATE VIEW gold.report_products AS
WITH base_query AS ( -- Base Query: Retrieves essential transaction and product fields
	SELECT
		f.order_number,
		f.order_date,
		f.customer_key,
		f.sales_amount,
		f.quantity,
		p.product_key,
		p.product_name,
		p.category,
		p.subcategory,
		p.cost
	FROM gold.fact_sales f
	LEFT JOIN gold.dim_products p
	    ON p.product_key = f.product_key
	WHERE f.order_date IS NOT NULL
),
product_aggregation AS ( -- Product Aggregation: Summarizes key performance metrics at the product level
	SELECT
		product_key,
		product_name,
		category,
		subcategory,
		cost,
		EXTRACT(YEAR FROM AGE(MAX(order_date), MIN(order_date))) * 12 + 
	  EXTRACT(MONTH FROM AGE(MAX(order_date), MIN(order_date))) AS lifespan,
		MAX(order_date) AS last_sale_date,
		COUNT(DISTINCT order_number) AS total_orders,
		COUNT(DISTINCT customer_key) AS total_customers,
		SUM(sales_amount) AS total_sales,
		SUM(quantity) AS total_quantity,
		ROUND(AVG(CAST(sales_amount AS NUMERIC) / NULLIF(quantity, 0)), 2) AS avg_selling_price -- Average Selling Price (ASP)
	FROM base_query
	GROUP BY 1, 2, 3, 4, 5
)
SELECT 
	product_key,
	product_name,		
	category,
	subcategory,
	cost,
	last_sale_date,
    (EXTRACT(YEAR FROM AGE(NOW(), last_sale_date)) * 12) +
    EXTRACT(MONTH FROM AGE(NOW(), last_sale_date)) AS recency,
	CASE 
		WHEN total_sales > 50000 THEN 'High Performer'
		WHEN total_sales >= 10000 THEN 'Mid Performer'
		ELSE 'Low Performer'
	END AS product_tier, -- Product Tier Segmentation based on total revenue
	lifespan,
	total_orders,
	total_sales,
	total_quantity,
	total_customers,
	avg_selling_price,
	ROUND(
        CASE 
		    WHEN total_orders = 0 THEN 0
		    ELSE total_sales / total_orders
	    END, 2
    ) AS avg_order_revenue, -- Average Order Revenue (AOR)
	ROUND(
        CASE
		    WHEN lifespan = 0 THEN total_sales
		    ELSE total_sales / lifespan
	    END, 2
    ) AS avg_monthly_spend -- Average Monthly Revenue (AMR)
FROM product_aggregation;
