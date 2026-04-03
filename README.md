# DATA_WAREHOUSE_SQL_PROJECT

**About this project**
---
This project is something I built while learning and practicing data analytics concepts using SQL. The main idea was to understand how raw data can be transformed into something meaningful that can actually help in decision-making.

Instead of just running queries, I wanted to go deeper and simulate how real companies store and organize their data — that’s why I worked on building a small data warehouse.

What this project does

In simple terms, this project takes messy/raw data and turns it into structured tables that are easy to analyze.

Loads raw data from source files
Cleans and transforms the data
Organizes it into fact and dimension tables
Makes it ready for reporting and analysis
Tech used

I didn’t use anything fancy — just focused on strong fundamentals:

SQL (mainly for transformations and modeling)
Basic ETL concepts
Relational database (like MySQL / PostgreSQL / SQL Server — depending on what you use)
Data pipeline (how it works)
---
Here’s how I approached it:

Bronze Layer
First, I loaded the raw data as-is without making any changes. This helps keep the original data safe.
Silver Layer
Then I cleaned the data:
removed duplicates
handled null values
fixed data types
Golden Layer
Finally, I created structured tables:
Fact tables → store measurable data (like sales)
Dimension tables → store descriptive data (like customers, products)
What I learned
---
# This project helped me understand things that tutorials don’t really teach properly:

How to think in terms of data modeling
Why structure matters more than just writing queries
How ETL actually works in practice
Importance of clean and consistent data
