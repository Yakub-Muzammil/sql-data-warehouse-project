/* 
=========================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
=========================
Script Purpose:
    This stored Procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema table from 'bronze' schema.
Actions Performed:
    - Truncate silver tables.
    - Inserts transformed and cleaned data from Bronze into Silver Tables.

Parameters:
    None.
  This stored Procedure does not accept any parameters or return any values.

Usage Example:
  Call silver.load_silver()
=========================
*/

CREATE OR REPLACE PROCEDURE silver.load_silver()
Language plpgsql AS $$

Declare start_time timestamp; 
		end_time timestamp;
		batch_start_time timestamp;
		batch_end_time timestamp;
		
BEGIN
	RAISE NOTICE '====================';
	batch_start_time := clock_timestamp();
	RAISE NOTICE 'Loading Silver Layer % seconds',batch_start_time;
	RAISE NOTICE '====================';

	RAISE NOTICE 'Loading CRM Tables';

	start_time := clock_timestamp();
	RAISE NOTICE 'Truncating Table: silver.crm_cust_info % seconds',start_time;
	Truncate table silver.crm_cust_info;
	
	RAISE NOTICE 'Inserting Data Into: silver.crm_cust_info';
	Insert into silver.crm_cust_info(
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
	)
	select 
	cst_id,
	cst_key,
	Trim(cst_firstname) as cst_firstname,
	Trim(cst_lastname) as cst_lastname,
	case when Upper(Trim(cst_marital_status)) = 'M' then 'Married'
		 when Upper(Trim(cst_marital_status)) = 'S' then 'Single'
		 else 'N/A'
	end as cst_marital_status,
	case when Upper(Trim(cst_gndr)) = 'M' then 'Male'
		 when Upper(Trim(cst_gndr)) = 'F' then 'Female'
		 else 'N/A'
	end as cst_gndr,
	cst_create_date
	from (select *, row_number() over (partition by cst_id order by cst_create_date Desc) as flag_last
	from bronze.crm_cust_info
	where cst_id is not null) where flag_last = 1;

	end_time := clock_timestamp();
	RAISE NOTICE 'Inserted data in Silver.crm_cust_info at % (duration % seconds)', end_time,
	Extract(EPOCH From (end_time - start_time));
	
	
	start_time := clock_timestamp();
	RAISE NOTICE 'Truncating Table: Silver.crm_prd_info % seconds',start_time;
	truncate table silver.crm_prd_info; 
	
	RAISE NOTICE 'Inserting Data Into: silver.crm_prd_info';
	Insert into silver.crm_prd_info(
		prd_id,
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
		REPLACE(SUBSTRING(prd_key, 1, 5), '-','_') as cat_id,
		SUBSTRING(prd_key, 7, LENGTH(prd_key)) as prd_key,
		prd_nm,
		COALESCE(prd_cost, 0) as prd_cost,
		CASE Upper(Trim(prd_line)) 
			 When'M' Then  'Mountain'
		     When 'R' Then  'Roads'
			 When 'T' Then  'Touring'
			 When 'S' Then  'Other Sales'
			ELSE  'N\A'
		END prd_line,
		prd_start_dt,
		Lead(prd_start_dt) OVER(Partition By prd_key Order by prd_start_dt)-1 as prd_end_dt
	FROM bronze.crm_prd_info;

	end_time := clock_timestamp();
	RAISE NOTICE 'Inserted data in Silver.crm_prd_info at % (duration % seconds)', end_time,
	Extract(EPOCH From (end_time - start_time));
	
	start_time := clock_timestamp();
	RAISE NOTICE 'Truncating Table: Silver.crm_sales_details % seconds',start_time;
	truncate table silver.crm_sales_details;
	
	RAISE NOTICE 'Inserting Data Into: silver.crm_sales_details';
	Insert into silver.crm_sales_details(
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
		case when sls_order_dt != 0 or length(sls_order_dt::text) != 8 then null
			 else cast(cast(sls_order_dt as varchar) as date)
		end as sls_order_dt,
		case when sls_ship_dt != 0 or length(sls_ship_dt::text) != 8 then null
			 else cast(cast(sls_ship_dt as varchar) as date)
		end as sls_ship_dt,
		case when sls_due_dt != 0 or length(sls_due_dt::text) != 8 then null
			 else cast(cast(sls_due_dt as varchar) as date)
		end as sls_due_dt,
		case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * ABS(sls_price) 
				then sls_quantity * abs(sls_price)
			 else sls_sales
		end as sls_sales,
		sls_quantity,
		case when sls_price is null or sls_price <= 0  
				then sls_price / coalesce(sls_quantity, 0)
			 else sls_price
		end as sls_price
	from bronze.crm_sales_details;

	end_time := clock_timestamp();
	RAISE NOTICE 'Inserted data in Silver.crm_sales_details at % (duration % seconds)', end_time,
	Extract(EPOCH From (end_time - start_time));
	
	RAISE NOTICE 'Loading ERP Tables';

	start_time := clock_timestamp();
	RAISE NOTICE 'Truncating Table: Silver.erp_cust_az12 % seconds',start_time;
	truncate table silver.erp_cust_az12; 
	
	RAISE NOTICE 'Inserting Data Into: silver.erp_cust_az12';
	Insert into silver.erp_cust_az12(
		cid,
		bdate,
		gen
	)
	select 
	case when cid like 'NAS%' then substring(cid, 4, length(cid))
		else cid
	end as cid,
	case when bdate > NOW() then null
		else bdate
	end as bdate,
	case when upper(Trim(gen)) in ('F', 'FEMALE') then 'FEMALE'
		 when upper(Trim(gen)) in ('M', 'MALE') then 'MALE'
		 else 'N/A'
	end gen
	from bronze.erp_cust_az12;

	end_time := clock_timestamp();
	RAISE NOTICE 'Inserted data in Silver.erp_cust_az12 at % (duration % seconds)', end_time,
	Extract(EPOCH From (end_time - start_time));
	
	start_time := clock_timestamp();
	RAISE NOTICE 'Truncating Table: Silver.erp_loc_a101 % seconds',start_time;
	truncate table silver.erp_loc_a101; 
	
	RAISE NOTICE 'Inserting Data Into: silver.erp_loc_a101';
	Insert into silver.erp_loc_a101(cid,cntry)
	select 
	replace(cid, '-', '') as cid,
	case when Trim(cntry) = 'DE' Then 'Germany'
		 when Trim(cntry) in ('US', 'USA') Then 'United States'
		 when trim(cntry) = '' or cntry is null then 'N/A'
		 else Trim(cntry)
	end as cntry
	from bronze.erp_loc_a101;

	end_time := clock_timestamp();
	RAISE NOTICE 'Inserted data in Silver.erp_loc_a101 at % (duration % seconds)', end_time,
	Extract(EPOCH From (end_time - start_time));
	
	start_time := clock_timestamp();
	RAISE NOTICE 'Truncating Table: Silver.erp_px_cat_g1v2 % seconds',start_time;
	truncate table silver.erp_px_cat_g1v2; 
	
	RAISE NOTICE 'Inserting Data Into: silver.erp_px_cat_g1v2';
	Insert Into silver.erp_px_cat_g1v2
	(id, cat, subcat, maintenance)
	Select id,
		   cat,
		   subcat,
		   maintainence
	from bronze.erp_px_cat_g1v2;

	end_time := clock_timestamp();
	RAISE NOTICE 'Inserted data in Silver.erp_prd_cat_g1v2 at % (duration % seconds)', end_time,
	Extract(EPOCH From (end_time - start_time));

	batch_end_time := clock_timestamp();
	RAISE NOTICE 'Silver Layer Ended at %s (duration %s seconds)',
    batch_end_time,
    Extract(EPOCH FROM (batch_end_time - batch_start_time));
	
END$$;
