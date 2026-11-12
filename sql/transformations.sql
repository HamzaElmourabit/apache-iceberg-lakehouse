SELECT
    Country,
    SUM(Quantity) AS total_quantity,
    SUM(Revenue) AS total_revenue,
    COUNT(DISTINCT Invoice) AS invoice_count,
    COUNT(DISTINCT Customer_ID) AS customer_count
FROM local.online_retail_silver
GROUP BY Country;