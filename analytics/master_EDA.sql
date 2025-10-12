SELECT *
FROM gold.dim_customers;

SELECT *
FROM gold.dim_products;

SELECT *
FROM gold.fact_sales;

-- change over time
-- total sales, customers and quantity by month
SELECT
	DATE_TRUNC('month',order_date)::date as order_date,
	SUM(sales_amount) as total_sales,
	COUNT(DISTINCT(customer_key)) as total_customer,
	SUM(quantity) as total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY 1
ORDER BY 1;

-- cumulative analysis
-- running total (continuous total)
-- granularity: monthly
WITH monthly_sales as (
SELECT
	DATE_TRUNC('month',order_date)::date as order_date,
	SUM(sales_amount) as total_sales
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY 1
)
SELECT 
	order_date,
	total_sales,
	SUM(total_sales) OVER(ORDER BY order_date) AS running_total
FROM monthly_sales
ORDER BY 1;

-- running total in each year (monthly sales with yearly resets)
-- granularity: monthly
-- running total: yearly resets
WITH monthly_sales as (
SELECT
	DATE_TRUNC('month',order_date)::date as order_date,
	SUM(sales_amount) as total_sales
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY 1
)
SELECT 
	order_date,
	total_sales,
	SUM(total_sales) OVER(PARTITION BY EXTRACT(YEAR FROM order_date) ORDER BY order_date) AS running_total
FROM monthly_sales
ORDER BY 1;

-- running total for yearly sales
-- granularity: yearly
WITH yearly_sales as
(SELECT
	DATE_TRUNC('year',order_date)::date as order_date,
	SUM(sales_amount) as total_sales
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY 1
)
SELECT 
	order_date,
	total_sales,
	SUM(total_sales) OVER(ORDER BY order_date) AS running_total
FROM yearly_sales
ORDER BY 1;

-- moving average of price
WITH yearly_summary as (
	SELECT
		DATE_TRUNC('year',order_date)::date as order_date,
		SUM(sales_amount) as total_sales,
		AVG(price) as avg_price
	FROM gold.fact_sales
	WHERE order_date IS NOT NULL
	GROUP BY 1
)
SELECT 
	order_date,
	total_sales,
	SUM(total_sales) OVER(ORDER BY order_date) AS running_total,
	ROUND(AVG(avg_price) OVER(ORDER BY order_date),2) AS moving_average_price
FROM yearly_summary
ORDER BY 1;

-- performance analysis
-- analyse the yearly perfromance of products by comparing their sales to both the average sales performance of the product and the previous years sales
WITH year_product_sales as (
	SELECT 
		EXTRACT(YEAR FROM f.order_date) as order_year, 
		p.product_name, 
		SUM(f.sales_amount) as current_sales
	FROM gold.fact_sales f
	LEFT JOIN gold.dim_products p
	ON f.product_key = p.product_key
	WHERE order_date IS NOT NULL
	GROUP BY 1, 2
)
SELECT
	order_year,
	product_name,
	current_sales,
	ROUND(AVG(current_sales) OVER(PARTITION BY product_name),2) as avg_sales,
	ROUND(current_sales - AVG(current_sales) OVER(PARTITION BY product_name),2) as difference_avg,
	CASE 
		WHEN current_sales - AVG(current_sales) OVER(PARTITION BY product_name) > 0 THEN 'Above Avg'
		WHEN current_sales - AVG(current_sales) OVER(PARTITION BY product_name) < 0 THEN 'Below Avg'
		ELSE 'Avg'
	END as avg_change,
-- year-over-year analysis by product
	LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) as prev_year_sales,
	current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) as difference_sales,
	CASE 
		WHEN current_sales - AVG(current_sales) OVER(PARTITION BY product_name) > 0 THEN 'Increase'
		WHEN current_sales - AVG(current_sales) OVER(PARTITION BY product_name) < 0 THEN 'Decrease'
		ELSE 'No change'
	END as prev_change
FROM year_product_sales
ORDER BY 2,1
;

-- part to whole
-- analyse how an individual part is performing compared to the overall
-- which categories contribute the most to overall slaes?
WITH category_sales as (
	SELECT
		category,
		SUM(sales_amount) as total_amount
	FROM gold.fact_sales f
	LEFT JOIN gold.dim_products p
	ON p.product_key = f.product_key
	GROUP BY category
)
SELECT 
	category,
	total_amount,
	SUM(total_amount) OVER() as overall_amount,
	CONCAT(ROUND((total_amount / SUM(total_amount) OVER())*100,2),'%') as percentage_of_total
FROM category_sales
ORDER BY total_amount DESC;

-- data segmentation
-- group the data based on specific range to better understand the correlation between two measures
-- segment products into cost ranges and how many products fall into each segment
WITH product_segment as (
	SELECT
		product_key,
		product_name,
		cost,
		CASE
			WHEN cost < 100 THEN 'Below 100'
			WHEN cost BETWEEN 100 AND 500 THEN '100-500'
			WHEN cost BETWEEN 501 AND 1000 THEN '501-1000'
			ELSE 'Above 1000'
		END as cost_range
	FROM gold.dim_products
)
SELECT
	cost_range,
	COUNT(product_key) as total_products
FROM product_segment
GROUP BY cost_range
ORDER BY total_products DESC;

-- group customers into three sections based on their spending history:
-- VIP: Customers with at least 12 months of history and spending more than £5,000
-- Regular: Customers with at least 12 months of history and spending £5,000 or less
-- New: Customers with a lifespan less than 12 months
-- find the total number of customers in each group
WITH customer_spending as 
(
	SELECT 
	    c.customer_key,
	    SUM(f.sales_amount) AS total_spending,
	    MIN(f.order_date) AS first_order,
	    MAX(f.order_date) AS last_order,
	    EXTRACT(YEAR FROM AGE(MAX(f.order_date), MIN(f.order_date))) * 12 + 
	    EXTRACT(MONTH FROM AGE(MAX(f.order_date), MIN(f.order_date))) AS lifespan
	FROM gold.fact_sales f
	JOIN gold.dim_customers c
	ON f.customer_key = c.customer_key
	GROUP BY c.customer_key
),
customer_segment as 
(
	SELECT 
		customer_key,
		total_spending,
		lifespan,
		CASE 
			WHEN lifespan >= 12 AND total_spending >= 5000 THEN 'VIP customer'
			WHEN lifespan >= 12 AND total_spending < 5000 THEN 'Regular customer'
			ELSE 'New customer'
		END as customer_tier
	FROM customer_spending
)
SELECT 
	customer_tier,
	COUNT(customer_key) as total_customers
FROM customer_segment
GROUP BY 1
ORDER BY 2 DESC
;

-- customer report
-- purpose: this report consolidates key customer metrics and behaviours
-- highlights:
-- 1. gether essiential fields such as names, ages and transaction details
-- 2. segment customer info into categories (VIP, Regular, New) and age groups
-- 3. aggregate customer level metrics: total orders, total sales, total quantity purchased, total products, lifespan (in months)
-- 4. calculate variable KPIs: recency (months since last order), average order value, average monthly spend
WITH base_query as -- base query: retrieves core columns from tables
(
	SELECT
		f.order_number,
		f.product_key,
		f.order_date,
		f.sales_amount,
		f.quantity,
		c.customer_key,
		c.customer_number,
		CONCAT(c.first_name,' ', c.last_name) as customer_name,
		EXTRACT(YEAR FROM AGE(NOW(), c.birthdate)) as age
	FROM gold.fact_sales f
	LEFT JOIN gold.dim_customers c
	ON c.customer_key = f.customer_key
	WHERE order_date IS NOT NULL
), customer_aggregation as -- customer aggregation: summarises key metrics at the customer level
(
SELECT
	customer_key,
	customer_number,
	customer_name,
	age,
	COUNT(DISTINCT order_number) as total_orders,
	SUM(sales_amount) as total_sales,
	SUM(quantity) as total_quantity,
	COUNT(DISTINCT product_key) as total_products,
	MAX(order_date) as last_order_date,
	EXTRACT(YEAR FROM AGE(MAX(order_date), MIN(order_date))) * 12 + 
    EXTRACT(MONTH FROM AGE(MAX(order_date), MIN(order_date))) AS lifespan
FROM base_query
GROUP BY 1,2,3,4
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
	END as age_bracket,
	CASE 
		WHEN lifespan >= 12 AND total_sales >= 5000 THEN 'VIP customer'
		WHEN lifespan >= 12 AND total_sales < 5000 THEN 'Regular customer'
		ELSE 'New customer'
	END as customer_tier,
	last_order_date,
    (EXTRACT(YEAR FROM AGE(NOW(), last_order_date)) * 12) +
    EXTRACT(MONTH FROM AGE(NOW(), last_order_date)) AS recency,
	total_orders,
	total_sales,
	total_quantity,
	total_products,
	lifespan,
	-- compute average order value 
	ROUND(CASE
		WHEN total_orders = 0 THEN 0
		ELSE total_sales/total_orders 
	END,2) as avg_order_value,
	-- compute average monthly spend
	ROUND(CASE
		WHEN lifespan = 0 then total_sales
		ELSE total_sales/lifespan
	END,2) as avg_monthly_spend
FROM customer_aggregation
;

-- product report
-- purpose: this report consolidates key product metrics and behaviours
-- highlights:
-- 1. gathers essential fields such as product name, category, subcategory and cost
-- 2. segment products by revenue to identify high performers, mid-range or low performers
-- 3. aggregate product level metrics: total orders, total sales, total quantitiy sold, total customers (unique), lifespan (in months)
-- 4. calculate valuable KPIs: recency (months since last sale), average order value, average monthly revenue
WITH base_query as -- base query: retrieves core columns from tables
(
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
	WHERE order_date IS NOT NULL
), product_aggregation as -- product aggregation: summarises key metrics at the product level
(
	SELECT
		product_key,
		product_name,
		category,
		subcategory,
		cost,
		EXTRACT(YEAR FROM AGE(MAX(order_date), MIN(order_date))) * 12 + 
	    EXTRACT(MONTH FROM AGE(MAX(order_date), MIN(order_date))) AS lifespan,
		MAX(order_date) as last_sale_date,
		COUNT(DISTINCT order_number) as total_orders,
		COUNT(DISTINCT customer_key) as total_customers,
		SUM(sales_amount) as total_sales,
		SUM(quantity) as total_quantity,
		ROUND(AVG(CAST(sales_amount AS NUMERIC) / NULLIF(quantity, 0)), 2) AS avg_selling_price
	FROM base_query
	GROUP BY 1,2,3,4,5
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
		WHEN total_sales > 50000 THEN 'High performer'
		WHEN total_sales >= 10000 THEN 'Mid performer'
		ELSE 'Low performer'
	END as product_tier,	
	lifespan,
	total_orders,
	total_sales,
	total_quantity,
	total_customers,
	avg_selling_price,
	-- average order revenue
	ROUND(CASE 
		WHEN total_orders = 0 THEN 0
		ELSE total_sales / total_orders
	END,2) as avg_order_revenue,
	-- average monthly spend
	ROUND(CASE
		WHEN lifespan = 0 then total_sales
		ELSE total_sales/lifespan
	END,2) as avg_monthly_spend
FROM product_aggregation;
 
-- create views
CREATE VIEW gold.report_customers as
WITH base_query as -- base query: retrieves core columns from tables
(
	SELECT
		f.order_number,
		f.product_key,
		f.order_date,
		f.sales_amount,
		f.quantity,
		c.customer_key,
		c.customer_number,
		CONCAT(c.first_name,' ', c.last_name) as customer_name,
		EXTRACT(YEAR FROM AGE(NOW(), c.birthdate)) as age
	FROM gold.fact_sales f
	LEFT JOIN gold.dim_customers c
	ON c.customer_key = f.customer_key
	WHERE order_date IS NOT NULL
), customer_aggregation as -- customer aggregation: summarises key metrics at the customer level
(
SELECT
	customer_key,
	customer_number,
	customer_name,
	age,
	COUNT(DISTINCT order_number) as total_orders,
	SUM(sales_amount) as total_sales,
	SUM(quantity) as total_quantity,
	COUNT(DISTINCT product_key) as total_products,
	MAX(order_date) as last_order_date,
	EXTRACT(YEAR FROM AGE(MAX(order_date), MIN(order_date))) * 12 + 
    EXTRACT(MONTH FROM AGE(MAX(order_date), MIN(order_date))) AS lifespan
FROM base_query
GROUP BY 1,2,3,4
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
	END as age_bracket,
	CASE 
		WHEN lifespan >= 12 AND total_sales >= 5000 THEN 'VIP customer'
		WHEN lifespan >= 12 AND total_sales < 5000 THEN 'Regular customer'
		ELSE 'New customer'
	END as customer_tier,
	last_order_date,
    (EXTRACT(YEAR FROM AGE(NOW(), last_order_date)) * 12) +
    EXTRACT(MONTH FROM AGE(NOW(), last_order_date)) AS recency,
	total_orders,
	total_sales,
	total_quantity,
	total_products,
	lifespan,
	-- compute average order value 
	ROUND(CASE
		WHEN total_orders = 0 THEN 0
		ELSE total_sales/total_orders 
	END,2) as avg_order_value,
	-- compute average monthly spend
	ROUND(CASE
		WHEN lifespan = 0 then total_sales
		ELSE total_sales/lifespan
	END,2) as avg_monthly_spend
FROM customer_aggregation
;

CREATE VIEW gold.report_products as
WITH base_query as -- base query: retrieves core columns from tables
(
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
	WHERE order_date IS NOT NULL
), product_aggregation as -- product aggregation: summarises key metrics at the product level
(
	SELECT
		product_key,
		product_name,
		category,
		subcategory,
		cost,
		EXTRACT(YEAR FROM AGE(MAX(order_date), MIN(order_date))) * 12 + 
	    EXTRACT(MONTH FROM AGE(MAX(order_date), MIN(order_date))) AS lifespan,
		MAX(order_date) as last_sale_date,
		COUNT(DISTINCT order_number) as total_orders,
		COUNT(DISTINCT customer_key) as total_customers,
		SUM(sales_amount) as total_sales,
		SUM(quantity) as total_quantity,
		ROUND(AVG(CAST(sales_amount AS NUMERIC) / NULLIF(quantity, 0)), 2) AS avg_selling_price
	FROM base_query
	GROUP BY 1,2,3,4,5
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
		WHEN total_sales > 50000 THEN 'High performer'
		WHEN total_sales >= 10000 THEN 'Mid performer'
		ELSE 'Low performer'
	END as product_tier,	
	lifespan,
	total_orders,
	total_sales,
	total_quantity,
	total_customers,
	avg_selling_price,
	-- average order revenue
	ROUND(CASE 
		WHEN total_orders = 0 THEN 0
		ELSE total_sales / total_orders
	END,2) as avg_order_revenue,
	-- average monthly spend
	ROUND(CASE
		WHEN lifespan = 0 then total_sales
		ELSE total_sales/lifespan
	END,2) as avg_monthly_spend
FROM product_aggregation;

SELECT *
FROM gold.report_customers;

SELECT *
FROM gold.report_products;
	



