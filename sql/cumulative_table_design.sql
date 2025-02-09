/*
@author: Joel Quartey
@date: Feb 8, 2025

*/



/*
    CUMULATIVE TABLE DESIGN
    ************************************

    Cumulative Table Design in data modeling is a table structure used to store precomputed aggregated/summarized data (SUM, COUNT, AVG)
    over a period of time, allowing for efficient trend analysis, totals or progressions. These tables are updated periodically to reflect
    cumulative values for metrics of interest.

    It is efficient for fast retrieval of precomputed results, avoiding expensive aggregations at query time and reducing load on transactional tables.
    It is mostly used for growth analysis over time. It's all about holding onto all of history

    Advantages:
    - Ability to do historical analysis without shuffle (Without GROUP BY)
    - Easy 'transition' analysis

    Drawbacks: It can only be backfilled sequentially.


    Pictorial View of Cumulative Table Design:
    -------------------------------------------------------

    1. Get Yesterday(day, month or year) + Today(day, month or year)
    2. Perform:
        - FULL OUTER JOIN
        - COALESCE ids and unchanging dimensions
        - Compute cumulative metrics
    3. Cumulated Output



    For our project, the metrics of interest are:
    - Sales Performance Tracking
    - Market Trend Analysis
    - Customer Segmentation

*/





-- Create a schema to house all OLAP tables
CREATE SCHEMA RetailSalesDW;

-- change schema to dataWarehouse
SET search_path TO schema_name, RetailSalesDW;




/*
    Product Sales Performance
    --------------------------------------
    To monitor cumulative sales performance of each product category by day

    *** seed query for cumulation is the first backfill
 */
CREATE TABLE cum_product_sales(
    category_id INT,
    product_category TEXT,
    cumulative_quantity INT,
    cumulative_sales NUMERIC(18, 2),
    present_date DATE,
    PRIMARY KEY (category_id, present_date)
);



-- Check the first ever date in the table
SELECT MIN(invoice_date), MAX(invoice_date) from retailsales.sales_details;



/*
    ETL pipeline to:
    - Extract the two dataset (yesterday and today)
    - Transform the dataset by combining both datasets using a FULL OUTER JOIN
    - Load into the target table: cum_product_sales

    Since we will be backfilling on a daily basis, we need to toggle only the present_date with yesterday's date and today's date
    Note: that the first time we run this pipeline/query which is our seed query will yield only "today's" data only,
    i.e the first date: 2023-01-01 as today's date and 2022-12-31 as yesterday's date.
*/

INSERT INTO cum_product_sales
WITH yesterday AS (
    -- Get previous day's cumulative values
    SELECT * FROM cum_product_sales
    WHERE present_date = '2023-01-04'
),
today AS (
    -- Compute today's sales per category
    SELECT
        t1.category_id,
        t2.category AS product_category,
        SUM(t1.quantity) AS cumulative_quantity,
        SUM(t1.amount) AS cumulative_sales,
        t1.invoice_date AS present_date
    FROM retailsales.sales_details t1
    LEFT JOIN retailsales.product_category t2 ON t1.category_id = t2.category_id
    WHERE t1.invoice_date = '2023-01-05'
    GROUP BY t1.category_id, t2.category, t1.invoice_date
)
SELECT
    COALESCE(t.category_id, y.category_id) AS category_id,
    COALESCE(t.product_category, y.product_category) AS product_category,
    -- Accumulate quantity and sales from yesterday and today
    COALESCE(y.cumulative_quantity, 0) + COALESCE(t.cumulative_quantity, 0) AS cumulative_quantity,
    COALESCE(y.cumulative_sales, 0) + COALESCE(t.cumulative_sales, 0) AS cumulative_sales,
    -- Always take today's date for new entries
    t.present_date AS present_date
FROM today t
FULL OUTER JOIN yesterday y ON t.category_id = y.category_id;



-- TRUNCATE TABLE cum_product_sales;
SELECT * FROM cum_product_sales;

SELECT * FROM cum_product_sales
WHERE product_category='Electronics';










/*
    Customer Location/Regional Product Sales Metrics
    -----------------------------------------
    Will contain annual aggregated sales data of products across customer locations/regions
 */
CREATE TABLE cum_region_product_sales(
    region_id INT,
    region TEXT,
    category_id INT,
    product_category TEXT,
    cumulative_sales NUMERIC(18, 2),
    current_year INT,
    PRIMARY KEY (region_id, category_id, current_year)
);

SELECT * FROM shopping.retailsales.sales_details;
SELECT MIN(invoice_date), MAX(invoice_date) from retailsales.sales_details;




/*
    ETL pipeline to:
    - Extract the two dataset (yesterday and today)
    - Transform the dataset by combining both datasets using a FULL OUTER JOIN
    - Load into the target table: cum_region_product_sales

    Since we will be backfilling annually, we need to toggle only the current_year and the previous year
*/
INSERT INTO cum_region_product_sales
WITH yesterday AS (
    SELECT * FROM cum_region_product_sales
    WHERE current_year = 2023
),
    today AS (
    SELECT
        t1.region_id,
        t2.region_name AS region,
        t1.category_id,
        t3.category,
        SUM(t1.amount) AS total_sales,
        EXTRACT(YEAR FROM t1.invoice_date::DATE) current_year
    FROM retailsales.sales_details t1
    LEFT JOIN retailsales.region t2 ON t1.region_id=t2.region_id
    LEFT JOIN retailsales.product_category t3 ON t1.category_id=t3.category_id
    WHERE EXTRACT(YEAR FROM t1.invoice_date::DATE) = 2024
    GROUP BY t1.region_id, t2.region_name, t1.category_id, t3.category, current_year
    ORDER BY t2.region_name, t3.category
    )
SELECT
    COALESCE(t.region_id, y.region_id) AS region_id,
    COALESCE(t.region, y.region) AS region,
    COALESCE(t.category_id, y.category_id) AS category_id,
    COALESCE(t.category, y.product_category) AS product_category,
    COALESCE(y.cumulative_sales, 0) + COALESCE(t.total_sales, 0) AS cumulative_sales,
    COALESCE(t.current_year, y.current_year) AS current_year

FROM today t
FULL OUTER JOIN yesterday y ON t.region_id=y.region_id AND t.category_id=y.category_id;




-- SELECT * FROM cum_region_product_sales;
SELECT * FROM cum_region_product_sales WHERE product_category='Books';










/*
    Customer Product Engagement
    -------------------------------------------
    To track how often customers buy certain product
    This data could be useful by feeding it to ML(Machine Learning) models to build recommendation system for the customer.
 */
CREATE TABLE cum_customer_product_engage(
    customer_id INT,
    category_id INT,
    product_category TEXT,
    cum_purchase_count INT,
    cumulative_sales NUMERIC(18, 2),
    current_year INT,
    PRIMARY KEY (customer_id, category_id, current_year)
);




/*
    ETL pipeline to:
    - Extract the two dataset (yesterday and today)
    - Transform the dataset by combining both datasets using a FULL OUTER JOIN
    - Load into the target table: cum_region_product_sales

    Since backfilling will be done sequentially (annually), we need to toggle only the current_year and the previous year
*/
INSERT INTO cum_customer_product_engage
WITH yesterday AS (
    SELECT * FROM cum_customer_product_engage
    WHERE current_year = 2023
),
    today AS (
        SELECT
        t1.customer_id,
        t1.category_id,
        t3.category,
        SUM(t1.quantity) AS purchase_count,
        SUM(t1.amount) AS total_sales,
        EXTRACT(YEAR FROM t1.invoice_date::DATE) current_year
    FROM retailsales.sales_details t1
    LEFT JOIN retailsales.product_category t3 ON t1.category_id=t3.category_id
    WHERE EXTRACT(YEAR FROM t1.invoice_date::DATE) = 2024
    GROUP BY t1.customer_id, t1.category_id, t3.category, current_year
    )
SELECT
    COALESCE(t.customer_id, y.customer_id) AS customer_id,
    COALESCE(t.category_id, y.category_id) AS category_id,
    COALESCE(t.category, y.product_category) AS product_category,
    COALESCE(y.cum_purchase_count, 0) + COALESCE(t.purchase_count, 0) AS cum_purchase_count,
    COALESCE(y.cumulative_sales, 0) + COALESCE(t.total_sales, 0) AS cumulative_sales,
    COALESCE(t.current_year, y.current_year) AS current_year

FROM today t
FULL OUTER JOIN yesterday y ON t.customer_id=y.customer_id AND t.category_id=y.category_id;



-- sanity check
SELECT * FROM cum_customer_product_engage LIMIT 100;
SELECT * FROM cum_customer_product_engage WHERE customer_id=7469;








/*
    Customer Lifetime Value (CLV)
    --------------------------------------------
    This will help us track the total revenue generated by each customer
 */
CREATE TABLE cum_customer_revenue(
    customer_id INT,
    cum_amount_spent NUMERIC(18, 2),
    current_year INT,
    PRIMARY KEY (customer_id, current_year)
);




/*
    ETL pipeline to:
    - Extract the two dataset (yesterday and today)
    - Transform the dataset by combining both datasets using a FULL OUTER JOIN
    - Load into the target table: cum_region_product_sales

    Since backfilling will be done sequentially (annually), we need to toggle only the current_year and the previous year
*/
INSERT INTO cum_customer_revenue
WITH yesterday AS (
    SELECT * FROM cum_customer_revenue
    WHERE current_year = 2023
),
    today AS (
        SELECT
        t1.customer_id,
        SUM(t1.amount) AS total_sales,
        EXTRACT(YEAR FROM t1.invoice_date::DATE) current_year
    FROM retailsales.sales_details t1
    WHERE EXTRACT(YEAR FROM t1.invoice_date::DATE) = 2024
    GROUP BY t1.customer_id, current_year
    )
SELECT
    COALESCE(t.customer_id, y.customer_id) AS customer_id,
    COALESCE(y.cum_amount_spent, 0) + COALESCE(t.total_sales, 0) AS cum_amount_spent,
    COALESCE(t.current_year, y.current_year) AS current_year

FROM today t
FULL OUTER JOIN yesterday y ON t.customer_id=y.customer_id;




-- sanity check
SELECT * FROM cum_customer_revenue LIMIT 100;
SELECT * FROM cum_customer_revenue WHERE current_year=2024;
SELECT * FROM cum_customer_revenue WHERE customer_id=190;





/*
    Modeling complex data types using STRUCT and ARRAYS
    ***********************************************************

    - Customer Purchase History with transaction statistics:

    This will help us model a MASTER DATA with complex data type which could be used by other Data Engineers. This will serve us a
    single source of truth for other data engineers downstream.
 */

SELECT * FROM retailsales.sales_details WHERE customer_id=970;
SELECT * FROM retailsales.retail_sales LIMIT 100;
SELECT * FROM retailsales.customer LIMIT 100;


-- Create a STRUCT type to hold yearly sales stats for each customer
CREATE TYPE sales_stats AS (
    sales_year INT,
    total_discount REAL,
    total_amount_spent REAL,
    total_qty_purchased INT
                                 );


/*
    Cumulative table to store customer sales statistics for each year
    Note that, in the table definition below, the TYPE for transaction_stats is the STRUCT type we just created above
*/
CREATE TABLE cum_customer_sales(
    customer_id INT,
    customer_no TEXT,
    sales_stats sales_stats[],
    current_year INT,
    PRIMARY KEY (customer_id, current_year)
);


/*
    ETL pipeline to:
    - Extract the two dataset (yesterday and today)
    - Transform the dataset by combining both datasets using a FULL OUTER JOIN
    - Load into the target table: cum_customer_transactions

    Since backfilling will be done sequentially (annually), we need to toggle only the current_year and the previous year
*/

INSERT INTO cum_customer_sales
WITH yesterday AS (
    SELECT * FROM cum_customer_sales
    WHERE current_year = 2023
),
    today AS (
        SELECT
        t1.customer_id,
        t2.customer_no,

        EXTRACT(YEAR FROM t1.invoice_date::DATE) sales_year,
        SUM(t1.discount) AS total_discount,
        SUM(t1.amount) AS total_sales,
        SUM(t1.quantity) AS total_quantity,

        EXTRACT(YEAR FROM t1.invoice_date::DATE) current_year

    FROM retailsales.sales_details t1
    LEFT JOIN retailsales.customer t2 ON t1.customer_id=t2.customer_id
    WHERE EXTRACT(YEAR FROM t1.invoice_date::DATE) = 2024
    GROUP BY t1.customer_id, t2.customer_no, sales_year
    )

SELECT
    COALESCE(t.customer_id, y.customer_id) AS customer_id,
    COALESCE(t.customer_no, y.customer_no) AS customer_no,

    CASE
        -- get sales_stats from yesterday's dataset and CAST(::) it to the STRUCT type as an ARRAY
        WHEN y.sales_stats IS NULL
            THEN ARRAY [ROW(
                t.sales_year,
                t.total_discount,
                t.total_sales,
                t.total_quantity
                )::sales_stats]

        /*
            Since we do not want to add to the array if today's value is NULL hence the case below.
            So for a customer who has stopped shopping with us, we want to hold on to that customer's data but not to keep adding more NULL to it.
        */
        WHEN t.sales_year IS NOT NULL

            -- concat (||) sales_stats of yesterday with today's dataset as an ARRAY of type STRUCT
            THEN y.sales_stats || ARRAY [ROW(
                t.sales_year,
                t.total_discount,
                t.total_sales,
                t.total_quantity
                )::sales_stats]
        ELSE y.sales_stats
        END AS sales_stats,

    COALESCE(t.current_year, y.current_year + 1) AS current_year

FROM today t
FULL OUTER JOIN yesterday y ON t.customer_id=y.customer_id;





-- sanity check on the cumulative table
SELECT * FROM cum_customer_sales
WHERE current_year = 2024
AND customer_id = 970;


-- unnesting the array of struct column (sales_stats) into multiple rows
SELECT customer_no, UNNEST(sales_stats) AS sales_stats
FROM cum_customer_sales
WHERE current_year = 2024
AND customer_id = 970;


-- Using CTE (common table expression) to unnest dataset to its original structure
WITH unnested AS (
    SELECT customer_id, customer_no, UNNEST(sales_stats)::sales_stats AS sales_stats
    FROM cum_customer_sales
    WHERE current_year = 2024
)
SELECT customer_no, (sales_stats::sales_stats).*
FROM unnested
WHERE customer_id = 970;
