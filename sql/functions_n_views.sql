/*
    Functions:
    --------------------------------

    Functions are similar to stored procedures, they can be called within a SELECT statement or WHERE clause.
    Unlike stored procedures, functions MUST return a value. Does not allow other DML commands such as UPDATE, DELETE, INSERT
    Transactions are not allowed within functions and also, stored procedures cannot be called from a function.

    Types of functions include:
    - Scalar (returns a single value)
    - Table-valued (returns a table result set)
    - Built-in functions (such as MIN, MAX, AVG, CURRENT_DATE)

    So what we want to do is to create a UDF(User Defined Function) => [Function created by a database user to perform a specific kind of operation that
    are not covered by built-in functions.] that will calculate the discounted price to be paid by the customer if a discount is granted on a product category item.

    This UDF will be called within a stored procedure to get the discounted price to be paid by the customer and then check against the amount
    supplied to that stored procedure for validation.

    ... And one last thing, always precede all your UDFs with 'fn_'

*/


-- This UDF will be created under the RetailSales schema
\c shopping;
SET search_path TO schema_name, RetailSales;



DROP FUNCTION IF EXISTS fn_calc_discount_price(NUMERIC, NUMERIC);


CREATE FUNCTION fn_calc_discount_price(unit_price NUMERIC, discount NUMERIC)
RETURNS NUMERIC(10, 2) AS $$

    DECLARE discount_price NUMERIC;
BEGIN
    -- logic here
    discount_price = ROUND((unit_price - (unit_price * discount))::NUMERIC, 2);
    RETURN discount_price;

END; $$
LANGUAGE PLPGSQL;


SELECT * FROM sales_details LIMIT 10;











/*
 We are going to create a trigger function to execute when an event, which is a new transaction is inserted into sales_details table
 We will then check if the invoice_date is present in the dim_date table in the data warehouse under dim_date table. If not then we
 INSERT into the table.


 -- IMPORTANT!
    - For row-level triggers (BEFORE/AFTER events) you return NEW or OLD (for DELETE triggers)
    - For statement-level triggers we return NULL

    since our trigger: trg_date_of_transaction is a statement-level trigger, we must RETURN NULL at the end of the function below
*/

DROP FUNCTION IF EXISTS fn_load_new_date_into_data_warehouse();



CREATE OR REPLACE FUNCTION fn_load_new_date_into_data_warehouse()
RETURNS TRIGGER AS $$

DECLARE transaction_date DATE;

BEGIN

    -- get distinct date from sales_details table:
    SELECT DISTINCT invoice_date
    FROM RetailSales.sales_details
    WHERE invoice_date NOT IN (
        SELECT full_date FROM RetailSalesDW.dim_date
        )
    INTO transaction_date;



    IF transaction_date IS NOT NULL THEN

        INSERT INTO RetailSalesDW.dim_date
        (full_date, year, quarter, quarter_name, month, month_name, day_of_month, day_of_week, day_name, is_holiday)

        SELECT
     transaction_date,
             EXTRACT(YEAR FROM transaction_date) sales_year,
             EXTRACT(QUARTER FROM transaction_date) sales_quarter,
             CASE
                 WHEN EXTRACT(QUARTER FROM transaction_date) = 1 THEN 'First'
                 WHEN EXTRACT(QUARTER FROM transaction_date) = 2 THEN 'Second'
                 WHEN EXTRACT(QUARTER FROM transaction_date) = 3 THEN 'Third'
                ELSE 'Fourth'
            END AS sales_quarter_name,

             EXTRACT(MONTH FROM transaction_date) sales_month,
             TO_CHAR(transaction_date, 'Month') sales_month_name,
             EXTRACT(DAY FROM transaction_date) sales_day_of_month,

             CASE
                 -- since postgres displays 0 for sunday, we replaced it with 7 instead
                 WHEN EXTRACT(DOW FROM transaction_date) = 0 THEN 7
                 ELSE EXTRACT(DOW FROM transaction_date)
            END AS sales_day_of_week,

             TO_CHAR(transaction_date, 'Day') AS day_name,
             -- this column should indicate if a date is a holiday or not.
             0::BIT AS is_holiday;


    END IF;

RETURN NULL;

END;
$$ LANGUAGE PLPGSQL;



