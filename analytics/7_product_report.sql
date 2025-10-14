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


-- ============================================================================
--    Validation Query: Preview Report Output
SELECT *
FROM gold.report_products;
