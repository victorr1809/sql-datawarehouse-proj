/* 
- STORED PROCEDURE: Loading dữ liệu từ lớp BRONZE --> SILVER
- MỤC ĐÍCH: Thực hiện ETL process từ lớp BRONZE sang silver
- PARAMETER: không có
- LƯU Ý: Khi gọi procedure, tất cả dữ liệu cũ trong các bảng sẽ bị xoá
- CÁCH GỌI: CALL silver.load_silver()
*/

CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
BEGIN
	RAISE NOTICE 'LOADING SILVER ....';
	-- SILVER loading crm_cust_info
	TRUNCATE TABLE silver.crm_cust_info;
	INSERT INTO silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_data
	)
	SELECT 
		cst_id,
		cst_key,
		-- Xoá khoảng trắng không mong muốn
		TRIM(cst_firstname) as cst_firstname,
		TRIM(cst_lastname) as cst_lastname,
		-- Chuyển chữ viết tắt thành viết đầy đủ
		CASE
			WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Maried'
			ELSE 'N/A'
		END as cst_marital_status,
		CASE
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			ELSE 'N/A'
		END as cst_gndr,
		cst_create_data
	FROM (
		select *, ROW_NUMBER() over (partition by cst_id order by cst_create_data desc) as flag_id
		from bronze.crm_cust_info
		where cst_id is not null
	) temp
	WHERE flag_id = 1;
	
	-- SILVER loading crm_prd_info
	TRUNCATE TABLE silver.crm_prd_info;
	Insert into silver.crm_prd_info (
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
	)
	select 
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
		SUBSTRING(prd_key, 7, length(prd_key)) as prd_key,
		prd_nm,
		COALESCE(prd_cost, 0) as prd_cost,
		CASE UPPER(TRIM(prd_line))
			WHEN 'R' then 'Road'
			WHEN 'S' then 'Other Sales'
			WHEN 'T' then 'Tour'
			WHEN 'M' then 'Mountain'
			ELSE 'n/a'
		END as prd_line,
		prd_start_dt,
		LEAD(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as prd_end_dt
	From bronze.crm_prd_info;
	
	-- SILVER loading crm_sales_details
	TRUNCATE TABLE silver.crm_sales_details;
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
	FROM bronze.crm_sales_details;
	
	-- SILVER loading erp_cust_az12
	TRUNCATE TABLE silver.erp_cust_az12;
	Insert into silver.erp_cust_az12 (
		cid,
		bdate,
		gen
	)
	SELECT
		CASE WHEN cid like 'NAS%' THEN SUBSTRING(cid, 4, length(cid))
			ELSE cid
		END as cid,
		CASE
			WHEN bdate > current_date then null
			ELSE bdate
		END as bdate,
		CASE
			WHEN UPPER(TRIM(gen)) in ('F', 'FEMALE') then 'Female'
			WHEN UPPER(TRIM(gen)) in ('M', 'MALE') then 'Male'
			ELSE 'n/a'
		END as gen
	FROM bronze.erp_cust_az12;
	
	-- SILVER loading erp_loc_a101
	TRUNCATE TABLE silver.erp_loc_a101;
	Insert into silver.erp_loc_a101 (
		cid,
		cntry
	)
	select distinct
		REPLACE(cid, '-','') as cid,
		CASE
			WHEN TRIM(cntry) in ('DE', 'Germany') then 'Germany'
			WHEN TRIM(cntry) in ('US', 'USA', 'United States') then 'United States'
			WHEN TRIM(cntry) = '' then NULL
			ELSE TRIM(cntry)
		END as cntry
	from bronze.erp_loc_a101;
	
	-- SILVER loading erp_px_cat_g1v2
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	INSERT INTO silver.erp_px_cat_g1v2 (
		id,
		cat,
		subcat,
		maintenance
	)
	select *
	from bronze.erp_px_cat_g1v2;

EXCEPTION
	WHEN OTHERS THEN
		RAISE NOTICE '==========================================';
        RAISE NOTICE 'ERROR OCCURED DURING LOADING BRONZE LAYER';
        RAISE NOTICE 'Error Message: %', SQLERRM;
        RAISE NOTICE 'Error Code: %', SQLSTATE;
        RAISE NOTICE '==========================================';
END;
$$;
