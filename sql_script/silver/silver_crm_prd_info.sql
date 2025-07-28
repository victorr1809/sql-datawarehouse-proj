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
From bronze.crm_prd_info
