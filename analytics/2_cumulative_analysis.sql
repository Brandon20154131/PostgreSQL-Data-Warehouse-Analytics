/*
===============================================================================
Cumulative Analysis
===============================================================================
Purpose:
    - To calculate running totals and rolling averages that show accumulated progress or performance over time.
    - To measure long-term growth trajectories or progressive totals.
    - To identify cumulative performance patterns and seasonal accumulation.

Key SQL Functions Used:
    - DATE_TRUNC(): Truncates dates to a specified granularity (month, year).
    - SUM() OVER(): Computes running totals across ordered sets.
    - PARTITION BY: Allows resetting cumulative totals (e.g., each year).
    - AVG() OVER(): Calculates moving averages for smoothing trends.
===============================================================================
*/


-- ============================================================================
--    Running Total (Continuous Total)
--    Purpose: To compute an ever-increasing cumulative total of sales over time.
--    Granularity: Monthly
WITH monthly_sales AS (
    SELECT
        DATE_TRUNC('month', order_date)::date AS order_month,
        SUM(sales_amount) AS total_sales
    FROM gold.fact_sales
    WHERE order_date IS NOT NULL
    GROUP BY 1
)
SELECT 
    order_month,
    total_sales,
    SUM(total_sales) OVER (ORDER BY order_month) AS running_total
FROM monthly_sales
ORDER BY 1;


-- ============================================================================
--    Running Total with Yearly Resets
--    Purpose: To compute cumulative monthly sales that restart every new year.
--    Granularity: Monthly
--    Running Total: Resets by Year
WITH monthly_sales AS (
    SELECT
        DATE_TRUNC('month', order_date)::date AS order_month,
        SUM(sales_amount) AS total_sales
    FROM gold.fact_sales
    WHERE order_date IS NOT NULL
    GROUP BY 1
)
SELECT 
    order_month,
    total_sales,
    SUM(total_sales) OVER (
        PARTITION BY EXTRACT(YEAR FROM order_month)
        ORDER BY order_month
    ) AS running_total
FROM monthly_sales
ORDER BY 1;


-- ============================================================================
--    Running Total for Yearly Sales
--    Purpose: To show accumulated revenue growth over years.
--    Granularity: Yearly
WITH yearly_sales AS (
    SELECT
        DATE_TRUNC('year', order_date)::date AS order_year,
        SUM(sales_amount) AS total_sales
    FROM gold.fact_sales
    WHERE order_date IS NOT NULL
    GROUP BY 1
)
SELECT 
    order_year,
    total_sales,
    SUM(total_sales) OVER (ORDER BY order_year) AS running_total
FROM yearly_sales
ORDER BY 1;


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
