/*
=============================================================
Tạo Database and Schemas
=============================================================
Mục đích kịch bản:
    Tập lệnh này tạo cơ sở dữ liệu mới có tên 'DataWarehouse' sau khi kiểm tra xem nó đã tồn tại chưa. 
    Nếu cơ sở dữ liệu tồn tại, nó sẽ bị loại bỏ và tạo lại. Ngoài ra, tập lệnh còn thiết lập ba lược đồ 
    trong cơ sở dữ liệu: 'đồng', 'bạc' và 'vàng'.
	
CẢNH BÁO:
    Việc chạy tập lệnh này sẽ loại bỏ toàn bộ cơ sở dữ liệu 'DataWarehouse' nếu nó tồn tại. 
    Tất cả dữ liệu trong cơ sở dữ liệu sẽ bị xóa vĩnh viễn. Tiến hành thận trọng 
    và đảm bảo bạn có bản sao lưu thích hợp trước khi chạy tập lệnh này.
*/

USE master;
GO

-- Xóa and tạo lại  'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

-- Tạo  'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
