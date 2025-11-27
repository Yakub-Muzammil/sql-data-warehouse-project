/* 
=========================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
=========================
Script Purpose:
    This stored Procedure loads data into the 'bronze' schema from external csv files.
    It Performs the following actions.
    - Truncate the bronze table before Loading Data.
    - Uses the `COPY` command to load data from csv files to bronze tables.

Parameters:
    None.
  This stored Procedure does not accept any parameters or return any values.

Usage Example:
  Call bronze.load_bronze()
=========================
*/
CREATE OR REPLACE PROCEDURE bronze.load_bronze() 
LANGUAGE plpgsql AS $$ 

Declare start_time timestamp; 
		end_time timestamp;
		batch_start_time timestamp;
		batch_end_time timestamp;

BEGIN 

	RAISE NOTICE '====================';
	batch_start_time := clock_timestamp();
	RAISE NOTICE 'Loading Bronze Layer % seconds',batch_start_time;
	RAISE NOTICE '====================';

	RAISE NOTICE 'Loading CRM Tables';

	start_time := clock_timestamp();
	RAISE NOTICE 'Truncate Table: Bronze.crm_cust_info % seconds',start_time;
	truncate table bronze.crm_cust_info; 

	RAISE NOTICE 'Inserting Data in Bronze.crm_cust_info';
	copy bronze.crm_cust_info  
	FROM 'C:\Program Files\PostgreSQL\18\source_crm\cust_info.csv' 
	WITH (FORMAT csv, HEADER);

	end_time := clock_timestamp();
	RAISE NOTICE 'Inserted data in Bronze.crm_cust_info at % (duration % seconds)', end_time,
	Extract(EPOCH From (end_time - start_time));

	start_time := clock_timestamp();
	RAISE NOTICE 'Truncate Table: Bronze.crm_prd_info % seconds',start_time;
	truncate table bronze.crm_prd_info; 

	RAISE NOTICE 'Inserting Data in Bronze.crm_prd_info';
	COPY bronze.crm_prd_info 
	FROM 'C:\Program Files\PostgreSQL\18\source_crm\prd_info.csv' 
	WITH (FORMAT csv, HEADER); 

	end_time := clock_timestamp();
	RAISE NOTICE 'Inserted data in Bronze.crm_prd_info at % (duration % seconds)', end_time,
	Extract(EPOCH From (end_time - start_time));

	start_time := clock_timestamp();
	RAISE NOTICE 'Truncate Table: Bronze.crm_sales_details % seconds',start_time;
	truncate table bronze.crm_sales_details;

	RAISE NOTICE 'Inserting Data in Bronze.crm_sales_details';
	COPY bronze.crm_sales_details 
	FROM 'C:\Program Files\PostgreSQL\18\source_crm\sales_details.csv' 
	WITH (FORMAT csv, HEADER); 

	end_time := clock_timestamp();
	RAISE NOTICE 'Inserted data in Bronze.crm_sales_details at % (duration % seconds)', end_time,
	Extract(EPOCH From (end_time - start_time));

	RAISE NOTICE 'Loading ERP Tables';

	start_time := clock_timestamp();
	RAISE NOTICE 'Truncate Table: Bronze.erp_cust_az12 % seconds',start_time;
	truncate table bronze.erp_cust_az12; 

	RAISE NOTICE 'Inserting Data in Bronze.erp_cust_az12';
	COPY bronze.erp_cust_az12
	FROM 'C:\Program Files\PostgreSQL\18\source_erp\cust_az12.csv' 
	WITH (FORMAT csv, HEADER); 

	end_time := clock_timestamp();
	RAISE NOTICE 'Inserted data in Bronze.erp_cust_az12 at % (duration % seconds)', end_time,
	Extract(EPOCH From (end_time - start_time));

	start_time := clock_timestamp();
	RAISE NOTICE 'Truncate Table: Bronze.erp_loc_a101 % seconds',start_time;
	truncate table bronze.erp_loc_a101; 

	RAISE NOTICE 'Inserting Data in Bronze.erp_loc_a101';
	COPY bronze.erp_loc_a101 
	FROM 'C:\Program Files\PostgreSQL\18\source_erp\loc_a101.csv' 
	WITH (FORMAT csv, HEADER); 

	end_time := clock_timestamp();
	RAISE NOTICE 'Inserted data in Bronze.erp_loc_a101 at % (duration % seconds)', end_time,
	Extract(EPOCH From (end_time - start_time));

	start_time := clock_timestamp();
	RAISE NOTICE 'Truncate Table: Bronze.erp_px_cat_g1v2 % seconds',start_time;
	truncate table bronze.erp_px_cat_g1v2; 

	RAISE NOTICE 'Inserting Data in Bronze.erp_prd_cat_g1v2';
	COPY bronze.erp_px_cat_g1v2 
	FROM 'C:\Program Files\PostgreSQL\18\source_erp\px_cat_g1v2.csv' 
	WITH (FORMAT csv, HEADER); 

	end_time := clock_timestamp();
	RAISE NOTICE 'Inserted data in Bronze.erp_prd_cat_g1v2 at % (duration % seconds)', end_time,
	Extract(EPOCH From (end_time - start_time));

	batch_end_time := clock_timestamp();
	RAISE NOTICE 'Bronze Layer Ended at %s (duration %s seconds)',
    batch_end_time,
    Extract(EPOCH FROM (batch_end_time - batch_start_time));

END; $$;
