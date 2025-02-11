/*
@author: Joel Quartey
@date: Feb 8, 2025

*/



/*

    Stored Procedures
    ----------------------------------

    A Stored Procedure is a precompiled SQL statements, stored as objects in the database that can be executed repetitively.
    It has several benefits such as;
    - Speed (compiled once and reused each time)
    - Easier maintenance
    - Reduce network traffic (through a simple call)
    - Increased security (helps reduce SQL injection attacks)

    What we want to do with stored procedures is to use it as a tool to INSERT data into our operational table: sales_details.
    Note:
        - Stored procedures may/may not return a value
        - It is good practice to name your stored procedures by preceding it with 'sp_' to give it some consistency.
*/


-- This S.procedure will be created under the RetailSales schema to INSERT data into our sales transaction table: sales_details
\c shopping;
SET search_path TO schema_name, RetailSales;



DROP PROCEDURE IF EXISTS sp_add_sales_transaction(INT, INT, INT, NUMERIC, NUMERIC, INT, INT, NUMERIC);





/*
    sp_add_sales_transaction():
    @params:
        customer_id, category_id, quantity, discount, amount, payment_method_id, region_id, unit_price
*/
CREATE PROCEDURE sp_add_sales_transaction(
    customer_id INT,
    category_id INT,
    quantity INT,
    discount NUMERIC,
    amount NUMERIC,
    payment_method_id INT,
    region_id INT,
    unit_price NUMERIC
)
LANGUAGE PLPGSQL
AS $$

 -- declare a variable to store the return value of the user defined function.
DECLARE calculated_discount_price NUMERIC;

BEGIN

    /*
        We want to validate the amount to be paid by customer, supplied as argument to this stored procedure against actual discounted price.
        We do so by comparing the amount with the discount price returned by the UDF (User defined function) which is a scalar function:

        Note:
         ** The function below MUST have been created before creating this stored procedure **
         ** You will find the function definition in the functions_n_views.sql file **

        fn_calc_discount_price(unit_price NUMERIC, discount NUMERIC)
    */

    -- call the function and store its value INTO the calculated_discount_price
    SELECT fn_calc_discount_price(unit_price, discount) INTO calculated_discount_price;


    -- compare the functions return value against amount supplied as input to the stored procedure.
    IF amount != calculated_discount_price THEN
        RAISE EXCEPTION 'The amount % is not equal to the discounted price %, please check again', amount, calculated_discount_price;
    ELSE
        -- do nothing and continue
        NULL;
    END IF;


    INSERT INTO RetailSales.sales_details
        (customer_id, category_id, quantity, discount, amount, payment_method_id, invoice_date, region_id, unit_price)
    VALUES
    (customer_id, category_id, quantity, discount, amount, payment_method_id, CURRENT_DATE, region_id, unit_price);


-- commit the transaction to execute ALL as one unit
COMMIT;


END;
$$;









/*
    Calling/Executing the procedure sp_add_sales_transaction()
    Note:
        - For Mysql and MSSQL we use the EXEC command to execute a stored procedure.
        - Postgres uses the CALL command to execute the stored procedure
*/
 CALL sp_add_sales_transaction(1928, 2, 1, 0.5, 482.20, 1, 4, 482.20);


-- We can add another transaction to the table, this time lets test our check constraint on the quantity field by making it zero (0)
 -- CALL sp_add_sales_transaction(1928, 2, 0, 0, 482.33, 1, 4, 482.33);




