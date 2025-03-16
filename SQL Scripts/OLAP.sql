USE metro_dwh;

SELECT 
    p.productID,
    p.productName,
    t.month,
    CASE 
        WHEN t.is_weekend = 1 THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type,
    SUM(s.totalRevenue) AS totalRevenue
    FROM Sales s
JOIN Products p ON s.productID = p.productID
JOIN Timedim t ON DATE(s.orderDate) = t.date
WHERE t.year = 2019
GROUP BY 1, 2, 3, 4
ORDER BY t.month, day_type, totalRevenue DESC
Limit 5;


-- Second
WITH QuarterlyRevenue AS (
    SELECT 
        Products.storeID,
        Products.storeName,
        QUARTER(orderDate) AS quarter,
        SUM(totalRevenue) AS revenue
    FROM Sales
    JOIN Products ON Sales.productID = Products.productID
    WHERE YEAR(orderDate) = 2017
    GROUP BY Products.storeID, Products.storeName, QUARTER(orderDate)
)
SELECT 
    storeID, 
    storeName,
    quarter, 
    revenue,
    LAG(revenue) OVER (PARTITION BY storeID ORDER BY quarter) AS previousRevenue,
    (revenue - LAG(revenue) OVER (PARTITION BY storeID ORDER BY quarter)) / LAG(revenue) OVER (PARTITION BY storeID ORDER BY quarter) * 100 AS growthRate
FROM QuarterlyRevenue;


-- Q3
SELECT 
    Products.storeID, 
    Products.storeName, 
    Products.supplierID, 
    Products.supplierName, 
    Products.productID, 
    SUM(Sales.totalRevenue) AS totalSales
FROM Sales
JOIN Products ON Sales.productID = Products.productID
GROUP BY Products.storeID, Products.storeName, Products.supplierID, Products.supplierName, Products.productID
ORDER BY Products.storeID, Products.supplierID;


-- Q4
WITH SeasonalSales AS (
    SELECT 
        Products.productID, 
        Products.storeID,
        Products.storeName,
        CASE 
            WHEN MONTH(orderDate) IN (3, 4, 5) THEN 'Spring'
            WHEN MONTH(orderDate) IN (6, 7, 8) THEN 'Summer'
            WHEN MONTH(orderDate) IN (9, 10, 11) THEN 'Fall'
            ELSE 'Winter'
        END AS season,
        SUM(totalRevenue) AS totalSales
    FROM Sales
    JOIN Products ON Sales.productID = Products.productID
    GROUP BY Products.productID, Products.storeID, Products.storeName, season
)
SELECT *
FROM SeasonalSales
ORDER BY season, storeID;


-- Q5
WITH MonthlyRevenue AS (
    SELECT 
        Products.storeID, 
        Products.storeName,
        Products.supplierID, 
        Products.supplierName,
        DATE_FORMAT(orderDate, '%Y-%m') AS orderMonth,
        SUM(totalRevenue) AS monthlyRevenue
    FROM Sales
    JOIN Products ON Sales.productID = Products.productID
    GROUP BY Products.storeID, Products.storeName, Products.supplierID, Products.supplierName, orderMonth
)
SELECT 
    storeID, 
    storeName,
    supplierID, 
    supplierName,
    orderMonth,
    monthlyRevenue,
    LAG(monthlyRevenue) OVER (PARTITION BY storeID, supplierID ORDER BY orderMonth) AS previousRevenue,
    (monthlyRevenue - LAG(monthlyRevenue) OVER (PARTITION BY storeID, supplierID ORDER BY orderMonth)) / LAG(monthlyRevenue) OVER (PARTITION BY storeID, supplierID ORDER BY orderMonth) * 100 AS volatility
FROM MonthlyRevenue;


-- q6
with transactionproducts as (
    select 
        transactionid, 
        productid
    from 
        sales
),
productpairs as (
    select 
        least(a.productid, b.productid) as product1,
        greatest(a.productid, b.productid) as product2,
        count(*) as paircount
    from 
        transactionproducts a
    join 
        transactionproducts b
    on 
        a.transactionid = b.transactionid and a.productid < b.productid
    group by 
        least(a.productid, b.productid), 
        greatest(a.productid, b.productid)
)
select 
    product1, 
    product2, 
    paircount
from 
    productpairs
order by 
    paircount desc
limit 5;
-- Q7
SELECT 
    Products.storeID, 
    Products.storeName, 
    Products.supplierID, 
    Products.supplierName, 
    Sales.productID,
    YEAR(orderDate) AS year, 
    SUM(totalRevenue) AS yearlyRevenue
FROM Sales
JOIN Products ON Sales.productID = Products.productID
GROUP BY Products.storeID, Products.storeName, Products.supplierID, Products.supplierName, Sales.productID, YEAR(orderDate) WITH ROLLUP;


-- 	Q8

WITH HalfYearlySales AS (
    SELECT 
        Sales.productID,
        Products.productName,
        CASE WHEN MONTH(orderDate) BETWEEN 1 AND 6 THEN 'H1' ELSE 'H2' END AS half,
        SUM(totalRevenue) AS totalRevenue,
        SUM(quantity) AS totalQuantity
    FROM Sales
    JOIN Products ON Sales.productID = Products.productID
    GROUP BY Sales.productID, Products.productName, half
)
SELECT *
FROM HalfYearlySales
ORDER BY productID, half;

-- Q9
WITH DailySales AS (
    SELECT 
        Sales.productID, 
        Products.productName,
        DATE(orderDate) AS orderDate, 
        SUM(totalRevenue) AS dailySales
    FROM Sales
    JOIN Products ON Sales.productID = Products.productID
    GROUP BY Sales.productID, Products.productName, DATE(orderDate)
),
DailyAverages AS (
    SELECT 
        productID, 
        AVG(dailySales) AS avgDailySales
    FROM DailySales
    GROUP BY productID
)
SELECT 
    DailySales.productID, 
    productName,
    orderDate, 
    dailySales, 
    avgDailySales,
    CASE WHEN dailySales > 2 * avgDailySales THEN 'Outlier' ELSE 'Normal' END AS spikeFlag
FROM DailySales
JOIN DailyAverages ON DailySales.productID = DailyAverages.productID
WHERE dailySales > 2 * avgDailySales;



-- Q10
CREATE VIEW REGION_STORE_QUARTERLY_SALES AS
SELECT 
    Products.storeName AS region,
    Products.storeID, 
    QUARTER(orderDate) AS quarter,
    SUM(totalRevenue) AS quarterlySales
FROM Sales
JOIN Products ON Sales.productID = Products.productID
GROUP BY Products.storeName, Products.storeID, QUARTER(orderDate)
ORDER BY Products.storeName, Products.storeID;

SELECT * FROM REGION_STORE_QUARTERLY_SALES;
