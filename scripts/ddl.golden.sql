IF OBJECT_ID('golden.customer_dim','V') IS NOT NULL
	DROP VIEW golden.customer_dim;
GO 
CREATE 
VIEW golden.customer_dim AS
SELECT ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS customer_key,
ci.cst_id AS customer_id,
ci.cst_key AS customer_number,
ci.cst_firstname AS first_name,
ci.cst_lastname AS last_name,
ci.cst_material_status AS mariage_status,
CASE 
	WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr
	ELSE COALESCE(cx.gen,'N/A') 
END AS gender,
cx.bdate,
la.cntry AS country,
cst_create_date AS creation_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.ERP_CUSTAZ12 cx
ON ci.cst_key = cx.cid
LEFT JOIN silver.ERP_LOC_A101 la
ON cx.cid = la.cid;

GO

IF OBJECT_ID('golden.product_dim','V')IS NOT NULL
	DROP VIEW golden.product_dim
GO
CREATE 
VIEW golden.product_dim AS
SELECT 
ROW_NUMBER() OVER(ORDER BY cp.prd_id,cp.prd_start_dt) AS product_key,
cp.prd_id AS product_id,
cp.prd_key AS product_number,
cp.prd_nm AS product_name,
cp.prd_line AS product_line,
cp.cat_id AS category_id,
cx.cat AS category_name,
cx.subcat AS subcategory,
cx.maintenance AS maintenance,
cp.prd_cost AS product_cost,
cp.prd_start_dt AS satrt_date
FROM silver.CRM_PRD_INFO cp
LEFT JOIN silver.erp_px_cat_g1v2 cx
ON cp.cat_id = cx.id
WHERE prd_end_dt IS NULL;

GO

IF OBJECT_ID('golden.order_fact','V') IS NOT NULL
	DROP VIEW golden.order_fact
GO
CREATE 
VIEW golden.order_fact AS
SELECT 
sls_ord_num AS order_number,
pd.product_key,
cd.customer_key,
sls_order_dt AS order_date,
sls_ship_dt AS shpping_date,
sls_due_dt AS due_date,
sls_sales AS sales,
sls_quantity AS quantity,
sls_price AS price
FROM SILVER.CRM_sales_details sd
LEFT JOIN GOLDEN.product_dim pd
ON sd.sls_prd_key = pd.product_number
LEFT JOIN GOLDEN.customer_dim cd
ON sd.sls_cust_id = cd.customer_id;

