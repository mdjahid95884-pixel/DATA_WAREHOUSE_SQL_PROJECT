--=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*--=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*
-- <WELCOME TO MY SQL DATA_WAREHOUSE_PROJECT AS A BEGINEER LEARNER THIS IS MY FIRST DATA_WAREHOUSE_PROJECT>.
--=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*--=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*
--<CREATING DATABSE>

CREATE DATABASE DATAWAREHOUSE;
USE DATAWAREHOUSE;

--<CREATING SCHEMA FOR ARE DATAWAREHOUSE>

CREATE SCHEMA BRONZE;  /*<CREATING BRONZE SCHEMA> */
CREATE SCHEMA SILVER;  /* <CREATING BRONZE SCHEMA> */
CREATE SCHEMA GOLDEN;  /* <CREATING BRONZE SCHEMA> */

--<CREATING TABLE CRM_CUST_INFO WITH COLUMNS>
CREATE TABLE BRONZE.CRM_CUST_INFO(
cst_id INT,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_material_status NVARCHAR(50),
cst_gndr NVARCHAR(20),
cst_create_date DATE
);
--<CREATING TABLE CRM_PRD_INFO WITH COLUMNS>
CREATE TABLE BRONZE.CRM_PRD_INFO(
prd_id INT,
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt DATE,
prd_end_dt DATE
);

--<CREATING TABLE CRM_SALES_DETAILS WITH COLUMNS>
CREATE TABLE BRONZE.CRM_sales_details(
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT
);

--<CREATING TABLE ERP_CUSTAZ12 WITH COLUMNS>
CREATE TABLE BRONZE.ERP_CUSTAZ12(
CID NVARCHAR(50),
BDATE DATE,
GEN NVARCHAR(50)
);

--<CREATING TABLE ERP_LOC_A101 WITH COLUMNS>
CREATE TABLE BRONZE.ERP_LOC_A101(
CID NVARCHAR(50),
CNTRY NVARCHAR(50)
);

--<CREATING TABLE ERP_PX_CAT_G1V2 WITH COLUMNS>
CREATE TABLE BRONZE.ERP_PX_CAT_G1V2(
ID NVARCHAR(50),
CAT NVARCHAR(50),
SUBCAT NVARCHAR(50),
MAINTENANCE NVARCHAR(50)
);

/*AS We have succesfully created are tables now we will gonna upload the data in are table with help of 'BULK INSERT' function 
as it's a fasted way to upload the data into the table without any error*/

BULK INSERT BRONZE.CRM_CUST_INFO
FROM 'C:\Users\zakir\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
WITH(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
		    );
-- As we have succesfully load the data into are table now we are check that data load in the right columns or not
SELECT * FROM BRONZE.CRM_CUST_INFO;
SELECT COUNT(*) FROM BRONZE.CRM_CUST_INFO;

BULK INSERT BRONZE.CRM_PRD_INFO
FROM 'C:\Users\zakir\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
WITH(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
		    );

-- As we have succesfully load the data into are table now we are check that data load in the right columns or not
SELECT * FROM BRONZE.CRM_PRD_INFO;
SELECT COUNT(*) FROM BRONZE.CRM_PRD_INFO;


BULK INSERT BRONZE.CRM_SALES_DETAILS
FROM 'C:\Users\zakir\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
WITH(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
		    );

-- As we have succesfully load the data into are table now we are check that data load in the right columns or not
SELECT * FROM BRONZE.CRM_SALES_DETAILS;
SELECT COUNT(*) FROM BRONZE.CRM_SALES_DETAILS;


BULK INSERT BRONZE.ERP_CUSTAZ12
FROM 'C:\Users\zakir\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
WITH(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
		    );

-- As we have succesfully load the data into are table now we are check that data load in the right columns or not
SELECT * FROM BRONZE.ERP_CUSTAZ12;
SELECT COUNT(*) FROM BRONZE.ERP_CUSTAZ12;



BULK INSERT BRONZE.ERP_LOC_A101
FROM'C:\Users\zakir\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
WITH(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
		    );

-- As we have succesfully load the data into are table now we are check that data load in the right columns or not
SELECT * FROM BRONZE.ERP_LOC_A101;
SELECT COUNT(*) FROM BRONZE.ERP_LOC_A101;


BULK INSERT BRONZE.ERP_PX_CAT_G1V2
FROM 'C:\Users\zakir\Downloads\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
WITH(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
		    );

-- As we have succesfully load the data into are table now we are check that data load in the right columns or not
SELECT * FROM BRONZE.ERP_PX_CAT_G1V2;
SELECT COUNT(*) FROM BRONZE.ERP_PX_CAT_G1V2;
