/*
@author: Joel Quartey
@date: Feb 11, 2025

*/


/*
    User Permissions and Access
    -----------------------------------------
    What we want to do here is to create two users. We will assign some privileges to our schemas. One will have access to the operational tables
    and the other will be granted permissions on the OLAP schema.

    We will then test the two user accounts by logging in to each user account and then try and write some queries to test the permissions.
    We will be using some DCL (Data Control Language) GRANT
*/


-- conect to shopping database and set schema to RetailSales
\c shopping;
SET search_path TO schema_name, RetailSales;



/*
 Note:
    - Before creating the user, make sure the current user has the create role privilege: rolcreaterole = true in postgres
    - check the current user: SELECT CURRENT_USER

    - check if current user has create role privilege: SELECT * FROM pg_roles;
*/
SELECT CURRENT_USER;
SELECT * FROM pg_roles;






/*
 Advanced User Management Techniques
-----------------------------------------
 What we want to do here is to group users using roles. This technique enables us to manage users efficiently. We will be able to group
 multiple users who have the same responsibilities. Lets assume we have two main teams in the company i.e analytics team and operations team.

 For large companies with many employees, this comes in handy. We can effectively manage permissions by creating roles and grouping the users
 under these roles.

 We will create two roles: analytics_team and operations_team
*/
CREATE ROLE analytics_team;


-- grant only select on all tables under data warehouse to analytics_team
GRANT SELECT ON ALL TABLES IN SCHEMA RetailSalesDW TO analytics_team;

-- This means you must first of all have USAGE on a schema to use objects (tables) within it
GRANT USAGE ON SCHEMA RetailSalesDW TO analytics_team;




-- create operations role and grant permissions on selected tables to this role
CREATE ROLE operations_team;

GRANT SELECT, INSERT, UPDATE
ON TABLE
    RetailSales.region,
    RetailSales.customer,
    RetailSales.customer_address,
    RetailSales.sales_details,
    RetailSales.payment_method,
    RetailSales.product_category

TO operations_team;

-- This means you must first of all have USAGE on a schema to use objects (tables) within it
GRANT USAGE ON SCHEMA RetailSales TO operations_team;




-- create a new user: user1
CREATE USER user1 WITH PASSWORD 'user1@Test';

-- create another user: user2
CREATE USER user2 WITH PASSWORD 'user2@Test';

/*
    Now that we have created our roles and granted privileges to them, we can now assign the users we just created to the
    appropriate roles. This makes user management much more efficient and effective.
*/

GRANT operations_team TO user1;
GRANT analytics_team TO user2;





/*
    REVOKE PRIVILEGES
    -----------------------

    Now, lets revoke the users we created initially (user1, user2). The reason being that, we want to create users with meaningful names hence
    analytics_user and operations_user instead of user1, user2

    BUT first, lets revoke the roles from the users, after that we can DROP (delete) the users
*/
REVOKE operations_team FROM user1;
REVOKE analytics_team FROM user2;



-- delete user1 and user2
DROP USER user1;
DROP USER user2;


-- sanity check to see if the users have been deleted
SELECT * FROM pg_roles;



-- create a new user: operations_user
CREATE USER operations_user WITH PASSWORD 'user1@Test';

-- create another user: analytics_user
CREATE USER analytics_user WITH PASSWORD 'user2@Test';



-- now we can grant the roles to the users
GRANT analytics_team TO analytics_user;
GRANT operations_team TO operations_user;



/*
    To test the configuration, we need to login with the username and password we created to see
    what resources they can access. See the screenshot directory for screenshots of each login

    BUT first, lets check the privileges assigned to the analytics_team role:
*/

SELECT * FROM information_schema.role_table_grants WHERE grantee = 'operations_team';

/*
 You will notice that postgres manages the privileges in a table: role_table_grants under the information_schema. It has:
    - granter: One who created and assigned the privileges
    - grantee: The entity to which the privileges were assigned to.
*/

