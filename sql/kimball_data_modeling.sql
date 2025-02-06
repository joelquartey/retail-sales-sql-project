/*
    Dimensional Data Modeling - Kimball Approach
    ----------------------------------------------------
    This approach to designing data warehouses was introduced by Ralph Kimball.
    A data warehouse is a centralized repository of aggregated data from various sources to support analytical capabilities and business intelligence
    systems. OLAP (Online Analytical Processing). It accumulates large amount of historical data, making it a single source of truth for making decisions.


    - Designing the model:
        - Choose the business process
        - Defining the grain (description/key business questions of what the dimensional model should be focusing on.)
        - Identify the dimensions (dim)
        - Identify the fact (fact)

    Note:
        - facts are observations, measures or numerical values that can be aggregated. e.g. sales_amount
        - dimensions (dims) are groups of hierarchies or descriptors that describes the facts. date, time, product are all
            examples of dimensions.

    - Identify dimensions:
        - dim_date, dim_product_category, dim_payment_method, dim_region, dim_customer

    - Identify facts:
        - fact_sales



    The Star Schema
    ---------------------------------

    This is the simplest and most popular type of data modeling technique used in data warehousing which consist of a centralized fact table that contain
    the measures of interest, surrounded by one or more dimension tables. The attributes in the dimension tables are used to SLICE and DICE the data
    in the fact table, allowing users to analyze the data from different perspective.

    Normalization is the term used to describe data that's stored in a way that reduces repetitious data
    Dimension tables enable filtering and grouping (GROUP BY, WHERE)
    Fact tables enable aggregations (SUM, COUNT, AVG)
*/




  /*

    -Key Business Questions Our Data Warehouse should be answering:
    1. Average Quarterly revenue generated by our product categories by regions?
    2. How are customers paying for their goods across regions?

    *****************************************************************************************
    There could be several business questions to be answered.
    Business question 2 was not answered. We will leave it open for collaborators to come up with some business questions
    and solutions to them. ***

  */




 /*
    ----------------------------------------------------------------------------------------------------------
    Note: All the tables below are to be created under the retail sales data warehouse schema: RetailSalesDW
    -----------------------------------------------------------------------------------------------------------
*/
SHOW search_path;
SET search_path TO schema_name, RetailSalesDW;



/*
    - Creating the date dimension (dim_date)
    We need to model this table such that we can tell which year, quarter, month, week, day a particular data point is. And in some instance
    we should be able to tell if that data point is a holiday or not.
*/
CREATE TABLE dim_date(
    date_key SERIAL PRIMARY KEY,
    full_date DATE,
    year INT,
    quarter INT,
    quarter_name TEXT,
    month INT,
    month_name TEXT,
    day_of_month INT,
    day_of_week INT,
    day_name TEXT,
    is_holiday BIT
);


-- Creating the product_category dimension (dim_product_category)
CREATE TABLE dim_product_category(
    category_key INT PRIMARY KEY,
    product_category TEXT
);



-- Creating the region dimension (dim_region)
CREATE TABLE dim_region(
    region_key INT PRIMARY KEY,
    region TEXT
);



-- Creating the payment_method dimension (dim_payment_method)
CREATE TABLE dim_payment_method(
    payment_method_key INT PRIMARY KEY,
    payment_method TEXT
);



-- Creating the customer dimension (dim_customer)
CREATE TABLE dim_customer(
    customer_key INT PRIMARY KEY,
    customer_no TEXT,
    first_name TEXT,
    last_name TEXT,
    gender TEXT,
    email TEXT

);




/*
    Creating the fact tables: fact_sale
*/
CREATE TABLE fact_sales(
    sale_key SERIAL PRIMARY KEY,
    date_key INT,
    customer_key INT,
    product_category_key INT,
    product_quantity INT,
    sale_amount NUMERIC(18, 2),
    sale_discount NUMERIC(4, 2),
    region_key INT,
    payment_method_key INT
);






/*
    ETL pipeline to populate the dimension and fact tables
*/
INSERT INTO dim_date
(full_date, year, quarter, quarter_name, month, month_name, day_of_month, day_of_week, day_name, is_holiday)
SELECT
    DISTINCT invoice_date,
             EXTRACT(YEAR FROM invoice_date) sales_year,
             EXTRACT(QUARTER FROM invoice_date) sales_quarter,
             CASE
                 WHEN EXTRACT(QUARTER FROM invoice_date) = 1 THEN 'First'
                 WHEN EXTRACT(QUARTER FROM invoice_date) = 2 THEN 'Second'
                 WHEN EXTRACT(QUARTER FROM invoice_date) = 3 THEN 'Third'
                ELSE 'Fourth'
            END AS sales_quarter_name,

             EXTRACT(MONTH FROM invoice_date) sales_month,
             TO_CHAR(invoice_date, 'Month') sales_month_name,
             EXTRACT(DAY FROM invoice_date) sales_day_of_month,

             CASE
                 -- since postgres displays 0 for sunday, we replaced it with 7 instead
                 WHEN EXTRACT(DOW FROM invoice_date) = 0 THEN 7
                 ELSE EXTRACT(DOW FROM invoice_date)
            END AS sales_day_of_week,

             TO_CHAR(invoice_date, 'Day') AS day_name,
             -- this column should indicate if a date is a holiday or not.
             0::BIT AS is_holiday

FROM retailsales.sales_details
ORDER BY invoice_date;




-- Sanity check to check the data
SELECT * FROM dim_date;






/*
    ETL pipeline to populate dim_customer
*/
INSERT INTO dim_customer
(customer_key, customer_no, first_name, last_name, gender, email)
SELECT DISTINCT t1.customer_id, t2.customer_no, t2.first_name, t2.last_name, t2.gender, t2.email
FROM retailsales.sales_details t1
JOIN retailsales.customer t2 ON t1.customer_id=t2.customer_id;




/*
    ETL pipeline to populate dim_product_category
*/
INSERT INTO dim_product_category(category_key, product_category)
SELECT DISTINCT t1.category_id, t2.category
FROM retailsales.sales_details t1
JOIN retailsales.product_category t2 ON t1.category_id=t2.category_id
ORDER BY t1.category_id;


-- sanity check
SELECT * FROM dim_product_category;




/*
    ETL pipeline to populate dim_region
*/
INSERT INTO dim_region(region_key, region)
SELECT DISTINCT t1.region_id, t2.region_name
FROM retailsales.sales_details t1
JOIN retailsales.region t2 ON t1.region_id=t2.region_id
ORDER BY t1.region_id;


-- sanity check
SELECT * FROM dim_region;



/*
    ETL pipeline to populate dim_payment_method
*/
INSERT INTO dim_payment_method(payment_method_key, payment_method)
SELECT DISTINCT t1.payment_method_id, t2.payment_method
FROM retailsales.sales_details t1
JOIN retailsales.payment_method t2 ON t1.payment_method_id=t2.method_id
ORDER BY t1.payment_method_id;






/*
    ETL pipeline to populate fact table: fact_sales
*/
INSERT INTO fact_sales
(date_key, customer_key, product_category_key, product_quantity, sale_amount, sale_discount, region_key, payment_method_key)
SELECT
    t6.date_key,
    t5.customer_key,
    t2.category_key,
    t1.quantity,
    t1.amount,
    t1.discount,
    t3.region_key,
    t4.payment_method_key

FROM retailsales.sales_details t1
JOIN dim_product_category t2 ON t1.category_id=t2.category_key
JOIN dim_region t3 ON t1.region_id=t3.region_key
JOIN dim_payment_method t4 ON t1.payment_method_id=t4.payment_method_key
JOIN dim_customer t5 ON t1.customer_id=t5.customer_key
JOIN dim_date t6 ON t1.invoice_date=t6.full_date
ORDER BY t1.invoice_date;


-- sanity check on fact_sales
SELECT * FROM fact_sales LIMIT 100;







/*
    Answering The Key Business Questions:
    --------------------------------------------
*/

-- 1. Average Quarterly revenue generated by our product categories by regions?
SELECT
    t4.year,
    t4.quarter_name,
    t2.region,
    t3.product_category,
    ROUND(AVG(t1.sale_amount), 2) avg_sales

FROM fact_sales t1
JOIN dim_region t2 ON t1.region_key=t2.region_key
JOIN dim_product_category t3 ON t1.product_category_key=t3.category_key
JOIN dim_date t4 ON t1.date_key=t4.date_key
GROUP BY t4.year, t4.quarter_name, t2.region, t3.product_category
ORDER BY t4.year, t4.quarter_name, t2.region, t3.product_category;






