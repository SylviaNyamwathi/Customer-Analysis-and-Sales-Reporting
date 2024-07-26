-- Task 1.1

-- This query provides a detailed overview of all individual customers, including their identity, contact, location information, and sales data.

WITH LatestAddress AS (
    -- Select the latest address for each customer
    SELECT 
        customer_address.CustomerID,
        address.AddressID,
        address.AddressLine1,
        address.AddressLine2,
        address.City,
        state_province.Name AS State,
        country_region.Name AS Country,
        ROW_NUMBER() OVER (PARTITION BY customer_address.CustomerID ORDER BY address.AddressID DESC) AS row_number
    FROM 
        `tc-da-1.adwentureworks_db.customeraddress` customer_address
    JOIN 
        `tc-da-1.adwentureworks_db.address` address ON customer_address.AddressID = address.AddressID
    JOIN 
        `tc-da-1.adwentureworks_db.stateprovince` state_province ON address.StateProvinceID = state_province.StateProvinceID
    JOIN 
        `tc-da-1.adwentureworks_db.countryregion` country_region ON state_province.CountryRegionCode = country_region.CountryRegionCode
),
CustomerInfo AS (
    -- Get identity and contact information for individual customers, along with their latest address
    SELECT 
        customer.CustomerID,
        contact.FirstName,
        contact.LastName,
        CONCAT(contact.FirstName, ' ', contact.LastName) AS FullName,
        CASE 
            WHEN contact.Title IS NOT NULL THEN CONCAT(contact.Title, ' ', contact.LastName)
            ELSE CONCAT('Dear ', contact.LastName)
        END AS AddressingTitle,
        contact.EmailAddress AS Email,
        contact.Phone,
        customer.AccountNumber,
        customer.CustomerType,
        latest_address.AddressLine1,
        latest_address.AddressLine2,
        latest_address.City,
        latest_address.State,
        latest_address.Country,
    FROM 
        `tc-da-1.adwentureworks_db.customer` customer
    JOIN 
        `tc-da-1.adwentureworks_db.individual` i ON customer.CustomerID = i.CustomerID
    JOIN 
        `tc-da-1.adwentureworks_db.contact` contact ON i.ContactID = contact.ContactID
    LEFT JOIN 
        LatestAddress latest_address ON customer.CustomerID = latest_address.CustomerID AND latest_address.row_number = 1 
),
CustomerSales AS (
    -- Aggregate sales data for each customer
    SELECT 
        sales_order_header.CustomerID,
        COUNT(sales_order_header.SalesOrderID) AS NumberOfOrders,
        ROUND(SUM(sales_order_header.TotalDue),3) AS TotalAmountWithTax,
        MAX(sales_order_header.OrderDate) AS LastOrderDate
    FROM 
        `tc-da-1.adwentureworks_db.salesorderheader` sales_order_header
    GROUP BY 
        sales_order_header.CustomerID
)
-- Final select to combine all information and limit to top 200 rows by total amount with tax
SELECT 
    customer_info.CustomerID,
    customer_info.FirstName,
    customer_info.LastName,
    customer_info.FullName,
    customer_info.AddressingTitle,
    customer_info.Email,
    customer_info.Phone,
    customer_info.AccountNumber,
    customer_info.CustomerType,
    customer_info.City,
    customer_info.AddressLine1,
    customer_info.AddressLine2,
    customer_info.State,
    customer_info.Country,
    customer_sales.NumberOfOrders,
    customer_sales.TotalAmountWithTax,
    customer_sales.LastOrderDate
FROM 
    CustomerInfo customer_info
LEFT JOIN 
    CustomerSales customer_sales ON customer_info.CustomerID = customer_sales.CustomerID
WHERE 
    customer_info.CustomerType = 'I'
ORDER BY 
    customer_sales.TotalAmountWithTax DESC
LIMIT 200;

-- Task 1.2

-- This query provides a detailed overview of the top 200 individual customers who have the highest total amount (with tax) 
-- and have not ordered in the last 365 days, based on the latest order date in the database.

-- Get the latest order date in the database to determine the current date context
WITH LatestOrderDate AS (
    SELECT MAX(OrderDate) AS LatestOrderDate
    FROM `tc-da-1.adwentureworks_db.salesorderheader` sales_order_header
)
, LatestAddress AS (
    -- Select the latest address for each customer
    SELECT 
        customer_address.CustomerID,
        address.AddressID,
        address.AddressLine1,
        address.AddressLine2,
        address.City,
        state_province.Name AS State,
        country_region.Name AS Country,
        ROW_NUMBER() OVER (PARTITION BY customer_address.CustomerID ORDER BY address.AddressID DESC) AS row_number
    FROM 
        `tc-da-1.adwentureworks_db.customeraddress` customer_address
    JOIN 
        `tc-da-1.adwentureworks_db.address` address ON customer_address.AddressID = address.AddressID
    JOIN 
        `tc-da-1.adwentureworks_db.stateprovince` state_province ON address.StateProvinceID = state_province.StateProvinceID
    JOIN 
        `tc-da-1.adwentureworks_db.countryregion` country_region ON state_province.CountryRegionCode = country_region.CountryRegionCode
)
, CustomerInfo AS (
    -- Get identity and contact information for individual customers, along with their latest address
    SELECT 
        customer.CustomerID,
        contact.FirstName,
        contact.LastName,
        CONCAT(contact.FirstName, ' ', contact.LastName) AS FullName,
        CASE 
            WHEN contact.Title IS NOT NULL THEN CONCAT(contact.Title, ' ', contact.LastName)
            ELSE CONCAT('Dear ', contact.LastName)
        END AS AddressingTitle,
        contact.EmailAddress AS Email,
        contact.Phone,
        customer.AccountNumber,
        customer.CustomerType,
        latest_address.AddressLine1,
        latest_address.AddressLine2,
        latest_address.City,
        latest_address.State,
        latest_address.Country
    FROM 
        `tc-da-1.adwentureworks_db.customer` customer
    JOIN 
        `tc-da-1.adwentureworks_db.individual` individual ON customer.CustomerID = individual.CustomerID
    JOIN 
        `tc-da-1.adwentureworks_db.contact` contact ON individual.ContactID = contact.ContactID
    LEFT JOIN 
        LatestAddress latest_address ON customer.CustomerID = latest_address.CustomerID AND latest_address.row_number = 1
)
, CustomerSales AS (
    -- Aggregate sales data for each customer
    SELECT 
        sales_order_header.CustomerID,
        COUNT(sales_order_header.SalesOrderID) AS NumberOfOrders,
        ROUND(SUM(sales_order_header.TotalDue), 3) AS TotalAmountWithTax,
        MAX(sales_order_header.OrderDate) AS LastOrderDate
    FROM 
        `tc-da-1.adwentureworks_db.salesorderheader` sales_order_header
    GROUP BY 
        sales_order_header.CustomerID
)
, InactiveCustomers AS (
    -- Identify customers who have not ordered in the last 365 days
    SELECT 
        customer_sales.CustomerID,
        customer_sales.NumberOfOrders,
        customer_sales.TotalAmountWithTax,
        customer_sales.LastOrderDate,
        latest_order_date.LatestOrderDate
    FROM 
        CustomerSales customer_sales
    CROSS JOIN 
        LatestOrderDate latest_order_date
    WHERE 
        DATE_ADD(customer_sales.LastOrderDate, INTERVAL 365 DAY) < latest_order_date.LatestOrderDate
)
-- Final select to combine all information and limit to top 200 rows by total amount with tax
SELECT 
    customer_info.CustomerID,
    customer_info.FirstName,
    customer_info.LastName,
    customer_info.FullName,
    customer_info.AddressingTitle,
    customer_info.Email,
    customer_info.Phone,
    customer_info.AccountNumber,
    customer_info.CustomerType,
    customer_info.City,
    customer_info.State,
    customer_info.Country,
    customer_info.AddressLine1,
    customer_info.AddressLine2,
    inactive_customers.NumberOfOrders,
    inactive_customers.TotalAmountWithTax,
    inactive_customers.LastOrderDate
FROM 
    CustomerInfo customer_info
JOIN 
    InactiveCustomers inactive_customers ON customer_info.CustomerID = inactive_customers.CustomerID
ORDER BY 
    inactive_customers.TotalAmountWithTax DESC
LIMIT 200;

-- Task 1.3

-- This query enriches the original query by adding a new column to mark active and inactive customers based on their order activity in the last 365 days.
-- It selects the top 500 rows ordered by CustomerID in descending order.

-- Select the latest address for each customer
WITH LatestAddress AS (
    SELECT 
        customer_address.CustomerID,
        address.AddressID,
        address.AddressLine1,
        address.AddressLine2,
        address.City,
        state_province.Name AS State,
        country_region.Name AS Country,
        ROW_NUMBER() OVER (PARTITION BY customer_address.CustomerID ORDER BY address.AddressID DESC) AS row_number
    FROM 
        `tc-da-1.adwentureworks_db.customeraddress` customer_address
    JOIN 
        `tc-da-1.adwentureworks_db.address` address ON customer_address.AddressID = address.AddressID
    JOIN 
        `tc-da-1.adwentureworks_db.stateprovince` state_province ON address.StateProvinceID = state_province.StateProvinceID
    JOIN 
        `tc-da-1.adwentureworks_db.countryregion` country_region ON state_province.CountryRegionCode = country_region.CountryRegionCode
)
, CustomerInfo AS (
    -- Get identity and contact information for individual customers, along with their latest address
    SELECT 
        customer.CustomerID,
        contact.FirstName,
        contact.LastName,
        CONCAT(contact.FirstName, ' ', contact.LastName) AS FullName,
        CASE 
            WHEN contact.Title IS NOT NULL THEN CONCAT(contact.Title, ' ', contact.LastName)
            ELSE CONCAT('Dear ', contact.LastName)
        END AS AddressingTitle,
        contact.EmailAddress AS Email,
        contact.Phone,
        customer.AccountNumber,
        customer.CustomerType,
        latest_address.AddressLine1,
        latest_address.AddressLine2,
        latest_address.City,
        latest_address.State,
        latest_address.Country
    FROM 
        `tc-da-1.adwentureworks_db.customer` customer
    JOIN 
        `tc-da-1.adwentureworks_db.individual` individual ON customer.CustomerID = individual.CustomerID
    JOIN 
        `tc-da-1.adwentureworks_db.contact` contact ON individual.ContactID = contact.ContactID
    LEFT JOIN 
        LatestAddress latest_address ON customer.CustomerID = latest_address.CustomerID AND latest_address.row_number = 1
)
, CustomerSales AS (
    -- Aggregate sales data for each customer
    SELECT 
        sales_order_header.CustomerID,
        COUNT(sales_order_header.SalesOrderID) AS NumberOfOrders,
        ROUND(SUM(sales_order_header.TotalDue), 3) AS TotalAmountWithTax,
        MAX(sales_order_header.OrderDate) AS LastOrderDate
    FROM 
        `tc-da-1.adwentureworks_db.salesorderheader` sales_order_header
    GROUP BY 
        sales_order_header.CustomerID
)
, LatestOrderDate AS (
    -- Get the latest order date in the database to determine the current date context
    SELECT 
        MAX(sales_order_header.OrderDate) AS LatestOrderDate
    FROM 
        `tc-da-1.adwentureworks_db.salesorderheader` sales_order_header
)
, EnrichedCustomerInfo AS (
    -- Enrich customer information by adding a status column for active and inactive customers based on order activity in the last 365 days
    SELECT 
        customer_info.CustomerID,
        customer_info.FirstName,
        customer_info.LastName,
        customer_info.FullName,
        customer_info.AddressingTitle,
        customer_info.Email,
        customer_info.Phone,
        customer_info.AccountNumber,
        customer_info.CustomerType,
        customer_info.City,
        customer_info.State,
        customer_info.Country,
        customer_info.AddressLine1,
        customer_info.AddressLine2,
        customer_sales.NumberOfOrders,
        customer_sales.TotalAmountWithTax,
        customer_sales.LastOrderDate,
        CASE 
            WHEN customer_sales.LastOrderDate >= DATE_SUB(latest_order_date.LatestOrderDate, INTERVAL 365 DAY) THEN 'Active' --  filters to include only those where the order_date is within the last 365 days.
            ELSE 'Inactive'
        END AS CustomerStatus
    FROM 
        CustomerInfo customer_info
    LEFT JOIN 
        CustomerSales customer_sales ON customer_info.CustomerID = customer_sales.CustomerID
    CROSS JOIN 
        LatestOrderDate latest_order_date
    WHERE 
        customer_info.CustomerType = 'I'
)
-- Final select to combine all information and limit to top 500 rows by CustomerID in descending order
SELECT 
    *
FROM 
    EnrichedCustomerInfo
ORDER BY 
    CustomerID DESC
LIMIT 500;

-- Task 1.4

-- This query extracts data on all active customers from North America
-- It includes customers who have either ordered at least 2500 in total amount (with tax) or have placed 5 or more orders.
-- The output includes the address split into two columns and is ordered by country, state, and the date of the last order.

-- Step 1: Select the latest address for each customer
WITH LatestAddress AS (
    SELECT 
        customer_address.CustomerID,
        address.AddressID,
        address.AddressLine1,
        address.AddressLine2,
        address.City,
        state_province.Name AS State,
        country_region.Name AS Country,
        ROW_NUMBER() OVER (PARTITION BY customer_address.CustomerID ORDER BY address.AddressID DESC) AS row_number
    FROM 
        `tc-da-1.adwentureworks_db.customeraddress` customer_address
    JOIN 
        `tc-da-1.adwentureworks_db.address` address ON customer_address.AddressID = address.AddressID
    JOIN 
        `tc-da-1.adwentureworks_db.stateprovince` state_province ON address.StateProvinceID = state_province.StateProvinceID
    JOIN 
        `tc-da-1.adwentureworks_db.countryregion` country_region ON state_province.CountryRegionCode = country_region.CountryRegionCode
    JOIN 
        `tc-da-1.adwentureworks_db.salesterritory` sales_territory ON sales_territory.CountryRegionCode = country_region.CountryRegionCode
    WHERE 
        sales_territory.Group = 'North America' 
)
, CustomerInfo AS (
    -- Step 2: Get identity and contact information for individual customers, along with their latest address
    SELECT 
        customer.CustomerID,
        contact.FirstName,
        contact.LastName,
        CONCAT(contact.FirstName, ' ', contact.LastName) AS FullName,
        CASE 
            WHEN contact.Title IS NOT NULL THEN CONCAT(contact.Title, ' ', contact.LastName)
            ELSE CONCAT('Dear ', contact.LastName)
        END AS AddressingTitle,
        contact.EmailAddress AS Email,
        contact.Phone,
        customer.AccountNumber,
        customer.CustomerType,
        latest_address.AddressLine1,
        latest_address.AddressLine2,
        latest_address.City,
        latest_address.State,
        latest_address.Country,
        -- Split the address into two columns: address_no and Address_st
        SUBSTR(latest_address.AddressLine1, 1, INSTR(latest_address.AddressLine1, ' ') - 1) AS address_no,
        SUBSTR(latest_address.AddressLine1, INSTR(latest_address.AddressLine1, ' ') + 1) AS Address_st,
    FROM 
        `tc-da-1.adwentureworks_db.customer` customer
    JOIN 
        `tc-da-1.adwentureworks_db.individual` i ON customer.CustomerID = i.CustomerID
    JOIN 
        `tc-da-1.adwentureworks_db.contact` contact ON i.ContactID = contact.ContactID
    LEFT JOIN 
        LatestAddress latest_address ON customer.CustomerID = latest_address.CustomerID AND latest_address.row_number = 1
)
, CustomerSales AS (
    -- Step 3: Aggregate sales data for each customer
    SELECT 
        sales_order_header.CustomerID,
        COUNT(sales_order_header.SalesOrderID) AS NumberOfOrders,
        ROUND(SUM(sales_order_header.TotalDue), 3) AS TotalAmountWithTax,
        MAX(sales_order_header.OrderDate) AS LastOrderDate
    FROM 
        `tc-da-1.adwentureworks_db.salesorderheader` sales_order_header
    GROUP BY 
        sales_order_header.CustomerID
)
, LatestOrderDate AS (
    -- Step 4: Get the latest order date in the database to determine the current date context
    SELECT 
        MAX(sales_order_header.OrderDate) AS LatestOrderDate
    FROM 
        `tc-da-1.adwentureworks_db.salesorderheader` sales_order_header
)
, EnrichedCustomerInfo AS (
    -- Step 5: Enrich customer information by adding a status column for active and inactive customers based on order activity in the last 365 days
    SELECT 
        customer_info.CustomerID,
        customer_info.FirstName,
        customer_info.LastName,
        customer_info.FullName,
        customer_info.AddressingTitle,
        customer_info.Email,
        customer_info.Phone,
        customer_info.AccountNumber,
        customer_info.CustomerType,
        customer_info.City,
        customer_info.State,
        customer_info.Country,
        customer_info.AddressLine1,
        customer_info.AddressLine2,
        customer_info.address_no,
        customer_info.Address_st,
        customer_sales.NumberOfOrders,
        customer_sales.TotalAmountWithTax,
        customer_sales.LastOrderDate,
        CASE 
            WHEN customer_sales.LastOrderDate >= DATE_SUB(latest_order_date.LatestOrderDate, INTERVAL 365 DAY) THEN 'Active'
            ELSE 'Inactive'
        END AS CustomerStatus
    FROM 
        CustomerInfo customer_info
    LEFT JOIN 
        CustomerSales customer_sales ON customer_info.CustomerID = customer_sales.CustomerID
    CROSS JOIN 
        LatestOrderDate latest_order_date
    WHERE 
        customer_info.CustomerType = 'I'
        AND (customer_sales.TotalAmountWithTax >= 2500 OR customer_sales.NumberOfOrders >= 5)  
)
-- Step 6: Final select to combine all information and filter only active customers
SELECT 
    CustomerID,
    FirstName,
    LastName,
    FullName,
    AddressingTitle,
    Email,
    Phone,
    AccountNumber,
    CustomerType,
    City,
    State,
    Country,
    AddressLine1,
    AddressLine2,
    address_no,
    Address_st,
    NumberOfOrders,
    TotalAmountWithTax,
    CustomerStatus,
    LastOrderDate
FROM 
    EnrichedCustomerInfo
WHERE 
    CustomerStatus = 'Active'  
ORDER BY 
    Country,   
    State,     
    LastOrderDate DESC
LIMIT 500;

-- Task 2.1

-- This query generates a report of monthly sales numbers in each country and region.
-- It includes the number of orders, customers, and salespersons in each month along with the total amount with tax earned.

-- Step 1: Calculate monthly sales data
WITH MonthlySales AS (
    SELECT 
        LAST_DAY(CAST(OrderDate AS DATE)) AS Month, 
        sales_territory.CountryRegionCode AS CountryRegionCode,  
        sales_territory.Name AS Region, 
        COUNT(DISTINCT sales_order_header.SalesOrderID) AS NumberOfOrders,  
        COUNT(DISTINCT sales_order_header.CustomerID) AS NumberOfCustomers,  
        COUNT(DISTINCT sales_order_header.SalesPersonID) AS NumberOfSalesPersons, 
        CAST(SUM(sales_order_header.TotalDue) AS INT) AS TotalAmountWithTax
    FROM 
        `tc-da-1.adwentureworks_db.salesorderheader` sales_order_header
    LEFT JOIN 
        `tc-da-1.adwentureworks_db.salesterritory` sales_territory ON sales_order_header.TerritoryID = sales_territory.TerritoryID  
    GROUP BY 
        Month,  
        CountryRegionCode,  
        Region  
)

-- Step 2: Select the calculated monthly sales data
SELECT 
    Month,  
    CountryRegionCode,  
    Region,  
    NumberOfOrders,  
    NumberOfCustomers,  
    NumberOfSalesPersons,  
    TotalAmountWithTax  
FROM 
    MonthlySales
ORDER BY
    CountryRegionCode DESC;

-- Task 2.2

-- Enrich 2.1 query with the cumulative_sum of the total amount with tax earned per country & region.

-- Calculate monthly sales data
WITH MonthlySales AS (
    SELECT 
        LAST_DAY(CAST(OrderDate AS DATE)) AS Month,
        sales_territory.CountryRegionCode AS CountryRegionCode,
        sales_territory.Name AS Region,
        COUNT(DISTINCT sales_order_header.SalesOrderID) AS NumberOfOrders,
        COUNT(DISTINCT sales_order_header.CustomerID) AS NumberOfCustomers,
        COUNT(DISTINCT sales_order_header.SalesPersonID) AS NumberOfSalesPersons,
        CAST(SUM(sales_order_header.TotalDue) AS INT) AS TotalAmountWithTax
    FROM 
        `tc-da-1.adwentureworks_db.salesorderheader` sales_order_header
    LEFT JOIN 
        `tc-da-1.adwentureworks_db.salesterritory` sales_territory ON sales_order_header.TerritoryID = sales_territory.TerritoryID
    GROUP BY 
        Month, 
        CountryRegionCode, 
        Region
),

-- Calculate cumulative sum of total amount with tax earned per country and region
CumulativeSales AS (
    SELECT 
        Month,
        CountryRegionCode,
        Region,
        TotalAmountWithTax,
        NumberOfOrders,
        NumberOfCustomers,
        NumberOfSalesPersons,
        SUM(TotalAmountWithTax) OVER (PARTITION BY CountryRegionCode ORDER BY Month) AS cumulative_sum
    FROM 
        MonthlySales
)

-- Select enriched sales data
SELECT 
    Month,
    CountryRegionCode,
    Region,
    NumberOfOrders,
    NumberOfCustomers,
    NumberOfSalesPersons,
    TotalAmountWithTax,
    cumulative_sum
FROM 
    CumulativeSales
ORDER BY
    CountryRegionCode, 
    Month;

-- Task 2.3

-- Enrich 2.2 query by adding ‘sales_rank’ column that ranks rows from best to worst for each country based on total amount with tax earned each month.

-- Calculate monthly sales data
WITH MonthlySales AS (
    SELECT 
        LAST_DAY(CAST(OrderDate AS DATE)) AS Month,
        sales_territory.CountryRegionCode AS CountryRegionCode,
        sales_territory.Name AS Region,
        COUNT(DISTINCT sales_order_header.SalesOrderID) AS NumberOfOrders,
        COUNT(DISTINCT sales_order_header.CustomerID) AS NumberOfCustomers,
        COUNT(DISTINCT sales_order_header.SalesPersonID) AS NumberOfSalesPersons,
        CAST(SUM(sales_order_header.TotalDue) AS INT) AS TotalAmountWithTax
    FROM 
        `tc-da-1.adwentureworks_db.salesorderheader` sales_order_header
    LEFT JOIN 
        `tc-da-1.adwentureworks_db.salesterritory` sales_territory ON sales_order_header.TerritoryID = sales_territory.TerritoryID
    -- WHERE 
        -- sales_territory.Name = 'France'
    GROUP BY 
        Month,
        CountryRegionCode,
        Region

-- Calculate cumulative sum and sales rank
),
CumulativeSales AS (
    SELECT 
        Month,
        CountryRegionCode,
        Region,
        NumberOfOrders,
        NumberOfCustomers,
        NumberOfSalesPersons,
        TotalAmountWithTax,
        SUM(TotalAmountWithTax) OVER (PARTITION BY CountryRegionCode ORDER BY Month) AS cumulative_sum,
        RANK() OVER (PARTITION BY CountryRegionCode ORDER BY TotalAmountWithTax DESC) AS country_sales_rank
    FROM 
        MonthlySales

-- Select enriched sales data with rank
)
SELECT 
    Month,
    CountryRegionCode,
    Region,
    NumberOfOrders,
    NumberOfCustomers,
    NumberOfSalesPersons,
    TotalAmountWithTax,
    country_sales_rank,
    cumulative_sum
FROM 
    CumulativeSales
ORDER BY 
    CountryRegionCode, Month DESC;

-- Task 2.4

/*
2.4 Enrich 2.3 query with country-level tax data:
- Add 'mean_tax_rate': average tax rate per country.
- Add 'perc_provinces_w_tax': percentage of provinces with available tax rates.
- Use highest tax rate per state for calculations.
- Ignore isonlystateprovinceFlag.
*/

WITH MonthlySales AS (
    SELECT 
        LAST_DAY(CAST(OrderDate AS DATE)) AS Month,
        sales_territory.CountryRegionCode AS CountryRegionCode,
        sales_territory.Name AS Region,
        COUNT(DISTINCT sales_order_header.SalesOrderID) AS NumberOfOrders,
        COUNT(DISTINCT sales_order_header.CustomerID) AS NumberOfCustomers,
        COUNT(DISTINCT sales_order_header.SalesPersonID) AS NumberOfSalesPersons,
        CAST(SUM(sales_order_header.TotalDue) AS INT) AS TotalAmountWithTax
    FROM 
        `tc-da-1.adwentureworks_db.salesorderheader` sales_order_header
    LEFT JOIN 
        `tc-da-1.adwentureworks_db.salesterritory` sales_territory ON sales_order_header.TerritoryID = sales_territory.TerritoryID
    -- WHERE
        -- sales_territory.CountryRegionCode = 'US'
    GROUP BY 
        Month,
        CountryRegionCode,
        Region
),
CumulativeSales AS (
    -- Calculate cumulative sums
    SELECT 
        Month,
        CountryRegionCode,
        Region,
        TotalAmountWithTax,
        NumberOfOrders,
        NumberOfCustomers,
        NumberOfSalesPersons,
        SUM(TotalAmountWithTax) OVER (PARTITION BY CountryRegionCode ORDER BY Month) AS cumulative_sum,
        RANK() OVER (PARTITION BY CountryRegionCode ORDER BY TotalAmountWithTax DESC) AS country_sales_rank
    FROM 
        MonthlySales
),
TaxRates AS (
     -- Retrieve maximum tax rates per state
    SELECT
        state_province.CountryRegionCode,
        MAX(sales_tax_rate.TaxRate) AS MaxTaxRate,
        state_province.StateProvinceID
    FROM 
        tc-da-1.adwentureworks_db.salestaxrate sales_tax_rate
    JOIN 
        tc-da-1.adwentureworks_db.stateprovince state_province ON sales_tax_rate.StateProvinceID = state_province.StateProvinceID
    GROUP BY
        state_province.CountryRegionCode,
        state_province.StateProvinceID
),
CountryTaxStats AS (
    -- Calculate mean tax rate and percentage of provinces with tax data per country
    SELECT 
        CountryRegionCode,
        ROUND(AVG(MaxTaxRate), 1) AS mean_tax_rate,
        ROUND(COUNT(StateProvinceID) * 1.0 / NULLIF((SELECT COUNT(*) FROM tc-da-1.adwentureworks_db.stateprovince WHERE CountryRegionCode = tax.CountryRegionCode), 0), 2) AS perc_provinces_w_tax
    FROM 
        TaxRates tax
    GROUP BY 
        CountryRegionCode
)

-- Select final results
SELECT 
    cumulative_sales.Month,
    cumulative_sales.CountryRegionCode,
    cumulative_sales.Region,
    cumulative_sales.NumberOfOrders,
    cumulative_sales.NumberOfCustomers,
    cumulative_sales.NumberOfSalesPersons,
    cumulative_sales.TotalAmountWithTax,
    cumulative_sales.cumulative_sum,
    cumulative_sales.country_sales_rank,
    country_tax_stats.mean_tax_rate,
    country_tax_stats.perc_provinces_w_tax
FROM 
    CumulativeSales cumulative_sales
LEFT JOIN 
    CountryTaxStats country_tax_stats ON cumulative_sales.CountryRegionCode = country_tax_stats.CountryRegionCode
ORDER BY
    cumulative_sales.CountryRegionCode, cumulative_sales.Month DESC;