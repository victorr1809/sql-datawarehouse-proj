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
WHERE flag_id = 1
