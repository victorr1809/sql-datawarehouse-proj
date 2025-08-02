-- Tạo VIEW cho bảng dim_product --
DROP VIEW IF EXISTS gold.dim_products;
CREATE VIEW gold.dim_products AS
select 
	ROW_NUMBER() OVER (order by p1.prd_start_dt, p1.prd_key) as prd_key,
	p1.prd_id,
	p1.prd_key as prd_number,
	p1.prd_nm as prd_name,
	p1.cat_id,
	p2.cat,
	p2.subcat,
	p2.maintenance,
	p1.prd_cost,
	p1.prd_line,
	p1.prd_start_dt
from silver.crm_prd_info p1
LEFT JOIN silver.erp_px_cat_g1v2 p2
ON cat_id = id
WHERE p1.prd_end_dt is null

-- Tạo VIEW cho bảng dim_customers --
DROP VIEW IF EXISTS gold.dim_customers;
CREATE VIEW gold.dim_customers AS
SELECT
	ROW_NUMBER() OVER (order by cst_id) as cst_key,
	c1.cst_id,
	c1.cst_key as cst_number,
	c1.cst_firstname as first_name,
	c1.cst_lastname as last_name,
	c3.cntry as country,
	c1.cst_marital_status as marital_status,
	CASE
		WHEN c1.cst_gndr != 'n/a' then c1.cst_gndr
		ELSE COALESCE(c2.gen, 'n/a')
	END as gender,
	c2.bdate as birthdate,
	c1.cst_create_data as create_date
FROM silver.crm_cust_info as c1
LEFT JOIN silver.erp_cust_az12 as c2
	ON c1.cst_key = c2.cid
LEFT JOIN silver.erp_loc_a101 as c3
	ON c1.cst_key = c3.cid

-- Tạo VIEW cho bảng fact_sales --
DROP VIEW IF EXISTS gold.fact_sales;
CREATE VIEW gold.fact_sales as
select 
	sa.sls_ord_num as order_number,
	pr.prd_key,
	cu.cst_key,
	sa.sls_order_dt as order_date,
	sa.sls_ship_dt as ship_date,
	sa.sls_due_dt as due_date,
	sa.sls_sales as sales_amount,
	sa.sls_quantity as sales_quantity,
	sa.sls_price as sales_price
from silver.crm_sales_details as sa
LEFT JOIN gold.dim_products as pr
	ON sa.sls_prd_key = pr.prd_number
LEFT JOIN gold.dim_customers as cu
	ON sa.sls_cust_id = cu.cst_id
-- from gold.dim_customers
