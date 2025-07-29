INSERT INTO silver.crm_sales_details (
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
)
select 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE WHEN sls_order_dt = 0 or length(sls_order_dt::text) != 8 then NULL
		ELSE TO_DATE(sls_order_dt::text, 'YYYYMMDD')
	END as sls_order_dt,
	CASE WHEN sls_ship_dt = 0 or length(sls_ship_dt::text) != 8 then NULL
		ELSE TO_DATE(sls_ship_dt::text, 'YYYYMMDD')
	END as sls_ship_dt,
	CASE WHEN sls_due_dt = 0 or length(sls_due_dt::text) != 8 then NULL
		ELSE TO_DATE(sls_due_dt::text, 'YYYYMMDD')
	END as sls_due_dt,
	CASE 
		WHEN sls_sales <= 0 or sls_sales is null or sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END as sls_sales,
	sls_quantity,
	CASE 
		WHEN sls_price <= 0 or sls_price is null or sls_price != sls_sales/sls_quantity THEN sls_sales/NULLIF(sls_quantity, 0)
		ELSE sls_price
	END as sls_price
FROM bronze.crm_sales_details
