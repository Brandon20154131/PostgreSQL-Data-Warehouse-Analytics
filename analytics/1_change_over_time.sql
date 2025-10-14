/*
===============================================================================
Change Over Time Analysis
===============================================================================
Purpose:
    - To track trends, growth, and changes in key metrics over time.
    - To support time-series and seasonal sales analysis.
    - To measure growth or decline across months and years.

Key SQL Functions Used:
    - DATE_TRUNC(): Extracts and truncates a date to the specified precision (month/year).
    - LAG(): Compares current period metrics with previous period metrics.
    - SUM(), COUNT(): Aggregate functions for calculating totals.
    - CASE: Used for classifying growth or decline patterns.
===============================================================================
*/

-- ============================================================================
--    Total Sales, Customers and Quantity by Month
--    Purpose: To observe monthly performance trends in sales volume and customer activity.
SELECT
	DATE_TRUNC('month',order_date)::date as order_date,
	SUM(sales_amount) as total_sales,
	COUNT(DISTINCT(customer_key)) as total_customer,
	SUM(quantity) as total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY 1
ORDER BY 1;

-- ============================================================================
--    Total Sales, Customers, and Quantity by Year
--    Purpose: To evaluate yearly sales growth and customer expansion.
SELECT
	DATE_TRUNC('year',order_date)::date as order_date,
	SUM(sales_amount) as total_sales,
	COUNT(DISTINCT(customer_key)) as total_customer,
	SUM(quantity) as total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY 1
ORDER BY 1;

-- ============================================================================
--    Year-Over-Year Sales Comparison
--    Purpose: To measure annual sales changes and identify year-over-year growth or decline.
SELECT
    DATE_TRUNC('year', order_date)::date as order_year,
    SUM(sales_amount) as total_sales,
    LAG(SUM(sales_amount)) OVER(ORDER BY DATE_TRUNC('year', order_date)::date) as prev_sales,
	SUM(sales_amount) - LAG(SUM(sales_amount)) OVER(ORDER BY DATE_TRUNC('year', order_date)::date) as difference_sales,
	CASE 
		WHEN SUM(sales_amount) - LAG(SUM(sales_amount)) OVER(ORDER BY DATE_TRUNC('year', order_date)::date) > 0 THEN 'Increase'
		WHEN SUM(sales_amount) - LAG(SUM(sales_amount)) OVER(ORDER BY DATE_TRUNC('year', order_date)::date) < 0 THEN 'Decrease'
		ELSE 'No change'
	END as prev_change
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY 1
ORDER BY 1;

-- ============================================================================
--    Month-Over-Month Sales Comparison
--    Purpose: To track monthly sales movement and detect short-term growth or dips.
SELECT
    DATE_TRUNC('month', order_date)::date as order_year,
    SUM(sales_amount) as total_sales,
    LAG(SUM(sales_amount)) OVER(ORDER BY DATE_TRUNC('month', order_date)::date) as prev_sales,
	SUM(sales_amount) - LAG(SUM(sales_amount)) OVER(ORDER BY DATE_TRUNC('month', order_date)::date) as difference_sales,
	CASE 
		WHEN SUM(sales_amount) - LAG(SUM(sales_amount)) OVER(ORDER BY DATE_TRUNC('month', order_date)::date) > 0 THEN 'Increase'
		WHEN SUM(sales_amount) - LAG(SUM(sales_amount)) OVER(ORDER BY DATE_TRUNC('month', order_date)::date) < 0 THEN 'Decrease'
		ELSE 'No change'
	END as prev_change
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY 1
ORDER BY 1;
