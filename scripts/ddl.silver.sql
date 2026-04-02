-- ====================== SILVER LAYER ============================================
/* <creating new table for silver layer as we have done with the bronze layer,
    now we are gonna do data manifulation in silver layer> */

-- <Creating new silver crm.customer information table>
IF OBJECT_ID('SILVER.CRM_CUST_INFO','U') IS NOT NULL
	DROP TABLE SILVER.CRM_CUST_INFO
CREATE TABLE SILVER.CRM_CUST_INFO(
cst_id INT,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_material_status NVARCHAR(50),
cst_gndr NVARCHAR(20),
cst_create_date DATE,
dwh_creation_date DATETIME2 DEFAULT GETDATE()
);

-- <Creating new silver crm.product information table>
IF OBJECT_ID('SILVER.CRM_PRD_INFO','U') IS NOT NULL
	DROP TABLE SILVER.CRM_PRD_INFO
CREATE TABLE SILVER.CRM_PRD_INFO(
prd_id INT,
cat_id NVARCHAR(50),
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt DATE,
prd_end_dt DATE,
dwh_creation_date DATETIME2 DEFAULT GETDATE()
);

-- <Creating new silver crm.sales details information table>
IF OBJECT_ID('SILVER.CRM_sales_details','U') IS NOT NULL
	DROP TABLE SILVER.CRM_sales_details
CREATE TABLE SILVER.CRM_sales_details(
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt DATE,
sls_ship_dt DATE,
sls_due_dt DATE,
sls_sales INT,
sls_quantity INT,
sls_price INT,
dwh_creation_date DATETIME2 DEFAULT GETDATE()
);

-- <Creating new silver erp.extra customer information table>
IF OBJECT_ID('SILVER.ERP_CUSTAZ12','U') IS NOT NULL
	DROP TABLE SILVER.ERP_CUSTAZ12
CREATE TABLE SILVER.ERP_CUSTAZ12(
CID NVARCHAR(50),
BDATE DATE,
GEN NVARCHAR(50),
dwh_creation_date DATETIME2 DEFAULT GETDATE()
);

-- <Creating new silver erp.customer location information table>
IF OBJECT_ID('SILVER.ERP_LOC_A101','U') IS NOT NULL
	DROP TABLE SILVER.ERP_LOC_A101
CREATE TABLE SILVER.ERP_LOC_A101(
CID NVARCHAR(50),
CNTRY NVARCHAR(50),
dwh_creation_date DATETIME2 DEFAULT GETDATE()
);

-- <Creating new silver erp.product category information table>
IF OBJECT_ID('SILVER.ERP_PX_CAT_G1V2','U') IS NOT NULL
	DROP TABLE SILVER.ERP_PX_CAT_G1V2
CREATE TABLE SILVER.ERP_PX_CAT_G1V2(
ID NVARCHAR(50),
CAT NVARCHAR(50),
SUBCAT NVARCHAR(50),
MAINTENANCE NVARCHAR(50),
dwh_creation_date DATETIME2 DEFAULT GETDATE()
);

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	TRUNCATE TABLE silver.CRM_CUST_INFO
	INSERT INTO silver.CRM_CUST_INFO(cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_material_status,
	cst_gndr,
	cst_create_date)
	SELECT cst_id,
		   cst_key,
		   TRIM(cst_firstname) AS cst_firstname,
		   TRIM(cst_firstname) AS cst_lastname,
		   CASE 
				WHEN cst_material_status = 'M' THEN 'MARRIED'
				WHEN cst_material_status = 'S'THEN 'SINGLE'
				ELSE 'N/A' 
			END AS cst_material_status,
			CASE 
				WHEN cst_gndr  = 'M' THEN 'MALE'
				WHEN cst_gndr = 'F' THEN 'FEMALE'
				ELSE 'N/A' 
			END AS cst_gndr,
			cst_create_date FROM 
								( SELECT *,
								 ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date) AS Flag_last
								 FROM bronze.CRM_CUST_INFO 
								 WHERE cst_id IS NOT NULL) t
								 WHERE Flag_last = 1;


	TRUNCATE TABLE silver.crm_prd_info
	INSERT INTO silver.crm_prd_info(prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
	)
		SELECT 
			prd_id,
			REPLACE(SUBSTRING(prd_key,1,5),'-','_') cat_id,
			SUBSTRING(prd_key,7,LEN(prd_key)) prd_key,
			prd_nm,
			COALESCE(prd_cost,0) prd_cost,
			CASE 
				WHEN prd_line = 'R' THEN 'Road'
				WHEN prd_line = 'M' THEN 'Mountain'
				WHEN prd_line = 'S' THEN 'Other Sales'
				WHEN prd_line = 'T' THEN 'Turing'
				ELSE 'N/A'
				END AS prd_line,
			prd_start_dt,
			DATEADD(DAY,-1, LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt))AS prd_end_dt
			FROM bronze.CRM_PRD_INFO;


	TRUNCATE TABLE silver.crm_sales_details
	INSERT INTO silver.crm_sales_details(
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price)
	SELECT sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE 
		WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_order_dt AS NVARCHAR) AS DATE) END AS sls_order_dt,
		CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS NVARCHAR) AS DATE) 
	END AS sls_ship_dt,
	CASE 
		WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
		ELSE CAST(CAST(sls_due_dt AS NVARCHAR) AS DATE) 
	END AS sls_due_dt,
	CASE 
		WHEN sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
		THEN sls_quantity* ABS(sls_price) 
		ELSE sls_sales 
	END AS sls_sales,
	sls_quantity,
	CASE 
		WHEN sls_price IS NULL OR sls_price <0
		THEN sls_sales / NULLIF(sls_quantity,0) 
		ELSE sls_price 
	END AS sls_price
	FROM BRONZE.CRM_sales_details


	TRUNCATE TABLE silver.ERP_CUSTAZ12
	INSERT INTO silver.ERP_CUSTAZ12(
	cid,
	bdate,
	gen
	)
	SELECT 
	CASE 
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
		ELSE cid 
	END AS cid,
	CASE 
		WHEN bdate > GETDATE() THEN NULL
		ELSE bdate 
	END AS bdate,
	CASE 
		WHEN TRIM(gen) IN ('M','MALE') THEN 'Male'
		WHEN TRIM(gen) IN ('F','FEMALE') THEN 'Female'
		ELSE 'N/A' 
	END AS gen
	FROM bronze.erp_custaz12


	TRUNCATE TABLE silver.ERP_LOC_A101
	INSERT INTO SILVER.ERP_LOC_A101(
	cid,
	cntry)
	SELECT TRIM(REPLACE(cid,'-','')) AS cid,
	CASE 
		WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
		ELSE cntry
		END AS cntry
	FROM bronze.ERP_LOC_A101;

	TRUNCATE TABLE silver.ERP_PX_CAT_G1V2
	INSERT INTO silver.ERP_PX_CAT_G1V2(
	id,
	cat,
	subcat,
	maintenance
	)
	SELECT * FROM BRONZE.ERP_PX_CAT_G1V2;
END
