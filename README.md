# Retail Sales and Customer Intelligence System

The motivation for this project is to demonstrate and leverage on a variety of SQL techniques and concepts as it applies to real life scenarios.
It leverages a technique in Data Engineering: ETL to set up a data infrastructure using pure SQL only.


## Project Overview:

This project involves designing and implementation of a data warehouse system for a fictitious multinational discount store operator in the global retail industry to analyze sales, customer behavior, and market trends across different regions. The system will be based on the Kimball Data Warehouse methodology, focusing on:

- Sales Performance Tracking
- Market Trend Analysis



:wrench: **SQL Concepts included in this project**

- Relational Data Modeling for OLTP
- Using DDL in database, schema and table creation
- Constraint Management
- Data Exploration using DML Queries and JOINS
- Modeling Slowly-changing Dimensions (SCDs)
- Common Table Expressions (CTEs)
- Cumulative Dimensions using complex data types (Struct and Arrays)
- Dimensional Data Modeling (Kimball) for OLAP

- Functions and Views
- Stored Procedures
- Triggers
- Transactions and Error Handling
- Identity and Access Management using DCL: (GRANT, REVOKE, DENY)
- Adherence to SQL Best Practices(naming conventions, documentations, clean code, etc.. )


## RDBMS 

The main Relational Database Management System and client used for this project:
- [Postgres](https://www.postgresql.org/)
- [DataGrip](https://www.jetbrains.com/datagrip/)


## Data Overview

### Data Consumers:

The intent of the data infrastructure would be able to serve different data consumers as stated below;

- Data analysts/Data scientist
- Data Engineers
- Machine Learning Models
- Ordinary consumer (Company Execs)


## Data Sources:

The main data source used for the project was extracted from kaggle: 

- [Online Retail Sales Dataset](https://www.kaggle.com/datasets/arnavsmayan/online-retail-sales-dataset)

The dataset is a synthetic online retail sales data, featuring customer transactions, product categories, quantities, prices, discounts, payment methods, and customer demographics.





## Credits

This project was inspired by the free 6-Week Data Engineering Bootcamp on youtube by Zack Wilson. My focus was on the first two weeks of the course where I learnt a lot of high-value techniques in dimensional data modeling. Special thanks to [Zack Wilson](https://github.com/EcZachly) for his immense contribution to the data engineering space.




_You will find screenshots of some outputs under each of the concepts explained in the screenshots directory_

