/*
@author: Joel Quartey
@date: Jan 31, 2025

*/





/*
    Relational Database Modeling & Design
    ----------------------------------------------------

    We want to start off with our main source data we got from kaggle[https://www.kaggle.com/datasets/arnavsmayan/online-retail-sales-dataset]
    We are going to create our database under a schema. We will then create our source table (retail_sales) and then load our data from kaggle into it.

    *** Conceptual modeling using ER-diagrams is not covered in this project. ****

    We will apply normalization (the process of organizing/structuring a database to reduce redundancy and to improve data consistency and integrity) up to
    3NF (Third Normal Form). We will enforce foreign key constraints, check constraints as well as creating indexes to speed up query execution.

    We will also cover idempotency and modeling of slowly-changing dimensions..

*/



-- Create database
CREATE DATABASE Shopping;

-- Connect to the shopping database
\c shopping

-- Create schema under the shopping database
CREATE SCHEMA RetailSales;

SHOW search_path;
SET search_path TO schema_name, RetailSales;




-- Create the base tables to import the main dataset into it
CREATE TABLE retail_sales (
    transaction_id INT,
    timestamp TEXT,
    customer_id INT,
    product_id INT,
    product_category TEXT,
    quantity INT,
    price NUMERIC(10, 2),
    discount NUMERIC(4, 2),
    payment_method TEXT,
    customer_age INT,
    customer_gender TEXT,
    customer_location TEXT,
    total_amount NUMERIC(10, 2)
);

-- import CSV dataset into postgresql using psql command
-- \COPY RetailSales.retail_sales (transaction_id, timestamp, customer_id, product_id, product_category, quantity, price, discount, payment_method, customer_age, customer_gender, customer_location, total_amount) FROM '/Users/joel.quartey/Sites/retail-sales-sql-project/data/online_retail_sales_dataset.csv' DELIMITER ',' CSV HEADER;


-- sanity check on columns created for base table
SELECT * FROM RetailSales.retail_sales LIMIT 100;



-- create a base tables for fake_customer_names and fake_customer_address
CREATE TABLE fake_customer_names (
    customer_name TEXT
);

-- import CSV dataset into postgresql using psql command
-- \COPY RetailSales.fake_customer_names (customer_name) FROM '/Users/joel.quartey/Sites/retail-sales-sql-project/data/fake_customer_names.csv' DELIMITER ',' CSV HEADER;



CREATE TABLE fake_customer_address (
    customer_address TEXT
);

-- import CSV dataset into postgresql using psql command
-- \COPY RetailSales.fake_customer_address (customer_address) FROM '/Users/joel.quartey/Sites/retail-sales-sql-project/data/fake_customer_address.csv' DELIMITER ',' CSV HEADER;

















/*
 DATA EXPLORATION
 ---------------------------
 -- Data Exploration Queries to understand the dataset
*/

SELECT * FROM retail_sales LIMIT 100;

-- show distinct customer_locations
SELECT DISTINCT customer_location FROM retail_sales ORDER BY customer_location;

-- show distinct product category
SELECT DISTINCT product_category FROM retail_sales ORDER BY product_category;

-- show distinct gender
SELECT DISTINCT customer_gender FROM retail_sales ORDER BY customer_gender;

-- show distinct payment method
SELECT DISTINCT payment_method FROM retail_sales ORDER BY payment_method;

-- Min, Max, AVG quantity of products purchased
SELECT MIN(quantity), MAX(quantity), ROUND(AVG(quantity), 4) AVG FROM retail_sales;

-- Min, Max, AVG, price of product purchased
SELECT MIN(price), MAX(price), ROUND(AVG(price), 4) AVG FROM retail_sales;

-- Min, Max, AVG, total_amount paid for products
SELECT MIN(total_amount), MAX(total_amount), ROUND(AVG(total_amount), 4) AVG FROM retail_sales;






/*
 DIMENSIONAL DATA MODELING:
 ***********************************

 Before modeling dimensions (attributes of an entity) it is important to know who your data consumers will be. This is very important because
 it influences how you will model the dimension to meet the needs of your consumers.

 The data consumers within your organization could be:
 - Software Engineers (To run their applications)
 - Data analysts (should be very easy to query, not many complex data types)
 - Data Engineers (should be compact and probably harder to query, could have nested types)
 - Data scientists/Machine Learning Models: They prefer mostly numerical & categorical dataset depending on the model
 - Customers: Should be very easy to interpret charts



 We start with OLTP (Online Transactional Processing) use case. This type of modeling is optimized for:
  - Low latency
  - Low volume queries
  - Used by software engineers
  - Highly normalized
  - It is what drives and powers their applications (operational systems)


 Based on the attributes/dimensions of the base table: retail_sales, we can deduce other entities such as:
 - payment_method
 - product_category
 - customer_location/region
 - customer
 - sales_details

*/


-- Create payment_method table
CREATE TABLE payment_method (
    method_id SERIAL PRIMARY KEY,
    payment_method TEXT NOT NULL,
    is_active SMALLINT DEFAULT 1
);



-- Create product_category table
CREATE TABLE product_category (
    category_id SERIAL PRIMARY KEY,
    category TEXT
);




-- Create region table
CREATE TABLE region (
    region_id SERIAL PRIMARY KEY,
    region_name TEXT
);


-- Create address table
CREATE TABLE address (
    address_id SERIAL PRIMARY KEY,
    street_number TEXT,
    city TEXT,
    zip_code TEXT
);



-- Create customer table
CREATE TABLE customer (
    customer_id SERIAL PRIMARY KEY,
    customer_no TEXT,
    first_name TEXT,
    last_name TEXT,
    gender CHAR,
    email TEXT,
    date_of_birth DATE,
    phone_number TEXT
);







/*
 IDEMPOTENCY & SLOWLY CHANGING DIMENSIONS (SCD)
 ****************************************************

 - Idempotency is the ability of a data pipeline to produce the same results (either in backfill or production) regardless of date, time and/or how many
 times you run them.

 - Backfill: in data pipelines is the process of populating missing/historical data into a database after the pipeline has already been set up. This is mostly
 done when there is the need to load data from earlier periods in time for completeness. Backfilling is a critical operation for maintaining data completeness
 and quality in data pipelines.


 Challenges of Non-Idempotent Pipelines:
 - Backfilling Leads to inconsistencies between old & new data
 - Leads to silent failures
 - Difficult to troubleshoot

 What's the cause and how to correct it:
 - INSERT INTO without TRUNCATE (leads to duplication). Instead:
    * use MERGE to avoid duplicates
    * use INSERT OVERWRITE to overwrite existing data with new data
- START_DATE without END_DATE impacts the pipeline by when it is run. Instead:
    * use window period to control the days of data added.



 SLOWLY CHANGING DIMENSIONS:
 -------------------------------------

 It is important to note that dimensions (attributes of an entity eg. birthday, name, etc. ) come in two types: fixed and slowly-changing.
 A slowly changing dimension is an attribute that changes overtime. (e.g. age, weight, address, department you work in, etc.)

 Since customer address is a slowly changing dimension (SCD), we need to track the history of changes overtime,
 hence we use the TYPE 2 approach:
    - must have a START_DATE and END_DATE to hold full history, tracking everything between the two dates.
    - must have an IS_CURRENT column to indicate the current dimension.


*/

CREATE TABLE customer_address (
    customer_id INTEGER,
    address_id INTEGER,
    start_date DATE,
    end_date DATE,
    is_current SMALLINT DEFAULT 1
);





-- Create sales_details table (hahahaa PK not good)
CREATE TABLE sales_details (
    sales_id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    category_id SMALLINT NOT NULL,
    unit_price NUMERIC(10, 2),
    quantity INT NOT NULL,
    discount NUMERIC(4, 2),
    amount NUMERIC(10, 2) NOT NULL,
    payment_method_id SMALLINT NOT NULL,
    invoice_date DATE NOT NULL,
    region_id SMALLINT NOT NULL
);





/*
 DEFINE TABLE CONSTRAINTS
 -------------------------------------------------------
 As a good practice, it is advisable to separate table constraint definitions especially foreign key, index & check constraints from actual table definitions.
 This is good for easier Constraint Management. Credit: [@Andrew Owusu's Github link]

 Define Constraints, including foreign keys to the tables above
*/

-- FK (Foreign Key) Constraints on customer_address
ALTER TABLE customer_address
ADD CONSTRAINT fk_address_id FOREIGN KEY (address_id) REFERENCES address(address_id),
ADD CONSTRAINT fk_customer_id FOREIGN KEY (customer_id) REFERENCES customer(customer_id);



-- FK (Foreign Key) Constraints on sales_details
ALTER TABLE sales_details
ADD CONSTRAINT fk_sales_customer_id FOREIGN KEY (customer_id) REFERENCES customer(customer_id),
ADD CONSTRAINT fk_sales_category_id FOREIGN KEY (category_id) REFERENCES product_category(category_id),
ADD CONSTRAINT fk_sales_payment_method_id FOREIGN KEY (payment_method_id) REFERENCES payment_method(method_id),
ADD CONSTRAINT fk_sales_region_id FOREIGN KEY (region_id) REFERENCES region(region_id);



-- Add a CHECK constraint on quantity attribute to accept only positive whole numbers ONLY
ALTER TABLE sales_details
ADD CONSTRAINT item_quantity CHECK (quantity > 0);


-- Add an INDEX Constraints on Sales_details to speed up queries (Query Optimization)
CREATE INDEX idx_sales_customer_id ON sales_details (customer_id);








/*
 POPULATION OF RELATIONAL/OLTP TABLES
 -----------------------------------------------------------------------------
 We need to populate our relational/oltp tables we created, by extracting data from our base table. (retail_sales)
*/

-- POPULATE payment_method table
INSERT INTO payment_method (payment_method)
SELECT DISTINCT payment_method
FROM retail_sales
ORDER BY payment_method;



-- POPULATE region table
INSERT INTO region (region_name)
SELECT DISTINCT customer_location
FROM retail_sales
ORDER BY customer_location;



-- POPULATE product_category table
INSERT INTO product_category (category)
SELECT DISTINCT product_category
FROM retail_sales
ORDER BY product_category;






/*
    CUSTOMER ADDRESS DATA MANIPULATION
    ----------------------------------------------
    For the purposes of the demonstration in modeling slowly-changing dimensions (SCDs) we will be using the addresses of the customers
    to demonstrate this scenario. From our base table: fake_customer_address, we extract the street_number, city and zipcode to populate our
    address relation/table.
*/
SELECT s1.street_number,
       (string_to_array(s1.city_zipcode, ','))[1] AS city,
       (string_to_array(s1.city_zipcode, ','))[2] AS zipcode
FROM (
        SELECT (string_to_array(t1.raw_address, '\n'))[1] AS street_number,
           (string_to_array(t1.raw_address, '\n'))[2] AS city_zipcode
        FROM (
                SELECT customer_address,
                   REPLACE(REPLACE(REPLACE(customer_address, '[', ''), ']', ''), '''', '')  AS raw_address
                FROM fake_customer_address
    ) AS t1
) AS s1
WHERE s1.street_number IS NOT NULL;






-- We can now POPULATE the address table
INSERT INTO address (street_number, city, zip_code)
SELECT s1.street_number,
       (string_to_array(s1.city_zipcode, ','))[1] AS city,
       (string_to_array(s1.city_zipcode, ','))[2] AS zipcode
FROM (
        SELECT (string_to_array(t1.raw_address, '\n'))[1] AS street_number,
           (string_to_array(t1.raw_address, '\n'))[2] AS city_zipcode
        FROM (
                SELECT customer_address,
                   REPLACE(REPLACE(REPLACE(customer_address, '[', ''), ']', ''), '''', '')  AS raw_address
                FROM fake_customer_address
    ) AS t1
) AS s1
WHERE s1.street_number IS NOT NULL;



SELECT COUNT(*) FROM address;







/*
 POPULATING THE CUSTOMER TABLE
 ------------------------------------
 In populating the customer table, we need to extract the customer_id, which will serve us customer number in our target table from the retail_sales base table.
 We will then augment the data by extracting some fictitious addresses and names from the fake_customer_address and fake_customer_names respectively.

 Note: the customer_id in the base table will be mapped to customer_no in the customer table because its a string. After insert, an auto customer_id
 which is an integer will be created.
*/

INSERT INTO customer (customer_no, gender)
SELECT  DISTINCT 'C-' || customer_id AS customer_no,
             CASE
                 WHEN customer_gender = 'Male' THEN 'M'
                 WHEN customer_gender = 'Female' THEN 'F'
                 WHEN customer_gender = 'Other' THEN 'F'
                 ELSE NULL
             END AS customer_gender
FROM retail_sales;






/*
 We can now populate the customer_address table by randomly distributing the addresses among the customer_ids from the customer table.

 This table is a solution to the pain of slowly-changing dimension.
 Slowly-changing dimensions are attributes of entities that are dynamic in nature. They have the tendency to change overtime.
 With this table, we will be able to track customer address history as it changes overtime.
*/
-- INSERT INTO customer_address(customer_id, start_date)
SELECT customer_id, CURRENT_DATE
FROM customer
ORDER BY customer_id;





-- POPULATE sales_details table
INSERT INTO sales_details
(customer_id, category_id, unit_price, quantity, discount, amount, payment_method_id, invoice_date, region_id)

SELECT t2.customer_id, t3.category_id, t1.price, t1.quantity, t1.discount, t1.total_amount, t4.method_id, t1.timestamp::DATE sale_date, t5.region_id
FROM retail_sales t1
LEFT JOIN customer t2 ON CONCAT('C-', t1.customer_id)=t2.customer_no
LEFT JOIN product_category t3 ON t1.product_category=t3.category
LEFT JOIN payment_method t4 ON t1.payment_method=t4.payment_method
LEFT JOIN region t5 ON t1.customer_location=t5.region_name;




SELECT * FROM customer LIMIT 100;
SELECT * FROM address;
SELECT * FROM payment_method;
SELECT * FROM product_category;
SELECT * FROM region;
SELECT * FROM retail_sales;
SELECT * FROM sales_details LIMIT 100;
SELECT * FROM sales_details WHERE invoice_date = '2025-02-08';


SELECT MIN(invoice_date), MAX(invoice_date) from sales_details;

SELECT customer_id, COUNT(customer_id) num_recs
FROM sales_details
GROUP BY customer_id
ORDER BY num_recs DESC;


SELECT t2.region_name, ROUND(SUM(t1.amount), 2)
FROM sales_details t1
LEFT JOIN region t2 ON t1.region_id=t2.region_id
GROUP BY t2.region_name
ORDER BY t2.region_name;


