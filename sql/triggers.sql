/*
@author: Joel Quartey
@date: Feb 8, 2025

*/


/*
    The Triggers
    ---------------------------

    Triggers are a special type of stored procedures that are automatically invoked when an event/activity/action occurs in the database server.
    DML(Data Manipulation Language) triggers run when a user tries to modify data through a DML event such as INSERT, UPDATE or DELETE
    Credit: Microsoft learn [https://learn.microsoft.com/en-us/sql/t-sql/statements/create-trigger-transact-sql?view=sql-server-ver16]

    - They can be specified to execute BEFORE or AFTER an event.
    What we want to do with triggers is to listen on the operational (OLTP) tables: sales_details, customers, region, product_category
    and then trigger the new records to load them into our data warehouse.

    We will implement only one, on sales_details table. In this table we will listen for new transactions. if the date is not in our warehouse: dim_date,
    then we will trigger an insert into dim_date.

    ***** Contributors are welcome to come up with new ideas. *****

    Note: W.r.t naming conventions, it is recommended to name your triggers by preceding them with 'trg_'
*/



-- This trigger will be created under the RetailSales schema
\c shopping;
SET search_path TO schema_name, RetailSales;





/*
 Creating the trigger

 Note: that there are row-level and statement-level triggers in postgresql.
 - Row-level triggers are invoked once for each row affected by the event(INSERT, DELETE, UPDATE)
 - Statement-level triggers are invoked once per SQL statement, regardless of the number of rows affected.
*/

DROP TRIGGER IF EXISTS trg_date_of_transaction ON RetailSales.sales_details;


CREATE TRIGGER trg_date_of_transaction
AFTER INSERT ON RetailSales.sales_details

-- Statement-level trigger
FOR EACH STATEMENT

EXECUTE FUNCTION fn_load_new_date_into_data_warehouse();