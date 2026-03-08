/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Mục đích kịch bản:
    Quy trình được lưu trữ này tải dữ liệu vào lược đồ 'bronze' từ các tệp CSV bên ngoài. 
    Nó thực hiện các hành động sau:
    - Truncate các table trong trong lớp broze trước khi inster vào.
    - Sử dụng lệnh 'BULK INSERT' để tải dữ liệu từ Tệp csv vào table bronze.

Thông số:
    Không. 
	  Thủ tục lưu trữ này không chấp nhận bất kỳ tham số nào hoặc trả về bất kỳ giá trị nào.

Ví dụ sử dụng:
    EXEC đồng.load_bronze;
===============================================================================
*/

create or alter procedure bronze.load_bronze as
begin
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
	begin try
	set @batch_start_time = GETDATE()
	print '======================================';
    print 'Loading Bronze Layer';
	print '======================================';

	print '--------------------------------------';
    print 'Loading CRM Tables';
	print '--------------------------------------';

	set @start_time = GETDATE()
	print '>> Truncating Table: [bronze].[crm_cust_info]'
	truncate table [bronze].[crm_cust_info]

	print '>> Inserting Data Into : [bronze].[crm_cust_info]'
	bulk insert [bronze].[crm_cust_info]
	from 'D:\DataWarehouse\sql-datawarehouse-project\datasets\source_crm\cust_info.csv'
	with(
		firstrow = 2,
		fieldterminator = ',',
		tablock
	);
	set @end_time = GETDATE()
	Print '>> Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) +' seconds'
	Print '-------------'

	set @start_time = GETDATE()
	print 'Truncating Table: [bronze].[crm_prd_info]'
	truncate table [bronze].[crm_prd_info]

	print 'Inserting Data Into : [bronze].[crm_prd_info]'
	bulk insert [bronze].[crm_prd_info]
	from 'D:\DataWarehouse\sql-datawarehouse-project\datasets\source_crm\prd_info.csv'
	with(
		firstrow = 2,
		fieldterminator = ',',
		tablock
	);
	set @end_time = GETDATE()
	Print '>> Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) +' seconds'
	Print '-------------'

	set @start_time = GETDATE()
	print '>> Truncating Table: [bronze].[crm_sales_details]'
	truncate table [bronze].[crm_sales_details]

	print '>> Inserting Data Into : [bronze].[crm_sales_details]'
	bulk insert [bronze].[crm_sales_details]
	from 'D:\DataWarehouse\sql-datawarehouse-project\datasets\source_crm\sales_details.csv'
	with(
		firstrow = 2,
		fieldterminator = ',',
		tablock
	);
	set @end_time = GETDATE()
	Print '>> Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) +' seconds'
	Print '>> -------------'


	print '--------------------------------------';
    print 'Loading ERP Tables';
	print '--------------------------------------';

	set @start_time = GETDATE()
	print '>> Truncating Table: [bronze].[erp_cust_az12]'
	truncate table [bronze].[erp_cust_az12]

	print '>> Inserting Data Into: [bronze].[erp_cust_az12]'
	bulk insert [bronze].[erp_cust_az12]
	from 'D:\DataWarehouse\sql-datawarehouse-project\datasets\source_erp\CUST_AZ12.csv'
	with(
		firstrow = 2,
		fieldterminator = ',',
		tablock
	);

	set @start_time = GETDATE()
	print '>> Truncating Table: [bronze].[erp_loc_a101]'
	truncate table [bronze].[erp_loc_a101]

	print '>> Inserting Data Into : [bronze].[erp_loc_a101]'
	bulk insert [bronze].[erp_loc_a101]
	from 'D:\DataWarehouse\sql-datawarehouse-project\datasets\source_erp\LOC_A101.csv'
	with(
		firstrow = 2,
		fieldterminator = ',',
		tablock
	);
	set @end_time = GETDATE()
	Print '>> Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) +' seconds'
	Print '>> -------------'

	set @start_time = GETDATE()
	print '>> Truncating Table: [bronze].[erp_px_cat_g1v2]'
	truncate table [bronze].[erp_px_cat_g1v2]

	print '>> Inserting Data Into : [bronze].[erp_px_cat_g1v2]'
	bulk insert [bronze].[erp_px_cat_g1v2]
	from 'D:\DataWarehouse\sql-datawarehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
	with(
		firstrow = 2,
		fieldterminator = ',',
		tablock
	);

	set @end_time = GETDATE()
	Print '>> Load Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) +' seconds'
	Print '>> -------------'
	
	set @batch_end_time = GETDATE()
	print '================================'
	print 'Loading Bronze Layer is Completed'
	print '- Total Load Duration: ' + cast( datediff(second, @batch_start_time,@batch_end_time) as nvarchar) + ' seconds'
	print '================================'

	end try
	begin catch
		print '==========================='
		print 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		print 'Error Message' +  error_message();
		print 'Error Message' + cast(error_number() as nvarchar);
		print 'Error Message' + cast(error_state() as nvarchar);
		print '==========================='
	end catch
end
