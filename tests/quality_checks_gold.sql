/*
===============================================================================
Kiểm tra chất lượng dữ liệu (Quality Checks)
===============================================================================

Mục đích:
    Tập lệnh này thực hiện các kiểm tra chất lượng dữ liệu nhằm xác thực
    tính toàn vẹn, nhất quán và chính xác của dữ liệu trong lớp Gold.
    Các kiểm tra bao gồm:

    - Kiểm tra tính duy nhất của khóa thay thế (Surrogate Key) trong các
      bảng Dimension.
    - Kiểm tra tính toàn vẹn tham chiếu (Referential Integrity) giữa
      các bảng Fact và Dimension.
    - Kiểm tra tính hợp lệ của các mối quan hệ trong mô hình dữ liệu
      Star Schema phục vụ cho phân tích và báo cáo.

Cách sử dụng:
    - Thực thi tập lệnh sau khi hoàn tất việc xây dựng lớp Gold.
    - Kiểm tra và xử lý các lỗi hoặc dữ liệu bất thường được phát hiện
      trước khi sử dụng dữ liệu cho báo cáo, dashboard hoặc các công cụ BI.
===============================================================================
*/

-- ====================================================================
-- Checking 'gold.dim_customers'
-- ====================================================================
-- Check for Uniqueness of Customer Key in gold.dim_customers
-- Expectation: No results 
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.product_key'
-- ====================================================================
-- Check for Uniqueness of Product Key in gold.dim_products
-- Expectation: No results 
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.fact_sales'
-- ====================================================================
-- Check the data model connectivity between fact and dimensions
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL  
