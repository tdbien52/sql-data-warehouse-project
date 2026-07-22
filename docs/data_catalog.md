# Data Catalog

## Overview

This document describes all dimension and fact tables used in the Data Warehouse.

---

# Dimension Tables

## dim_customer

| Column | Data Type | Description |
|---------|-----------|-------------|
| customer_key | INT | Surrogate Key |
| customer_id | INT | Business Key from source system |
| first_name | VARCHAR(50) | Customer first name |
| last_name | VARCHAR(50) | Customer last name |
| country | VARCHAR(50) | Customer country |
| create_date | DATE | Record creation date |

---

## dim_product

| Column | Data Type | Description |
|---------|-----------|-------------|
| product_key | INT | Surrogate Key |
| product_id | INT | Business Key |
| product_name | VARCHAR(100) | Product name |
| category | VARCHAR(50) | Product category |
| brand | VARCHAR(50) | Product brand |

---

## dim_date

| Column | Data Type | Description |
|---------|-----------|-------------|
| date_key | INT | YYYYMMDD |
| full_date | DATE | Calendar date |
| day | INT | Day of month |
| month | INT | Month |
| quarter | INT | Quarter |
| year | INT | Year |

---

# Fact Tables

## fact_sales

| Column | Data Type | Description |
|---------|-----------|-------------|
| sales_key | BIGINT | Fact Primary Key |
| customer_key | INT | FK → dim_customer |
| product_key | INT | FK → dim_product |
| date_key | INT | FK → dim_date |
| quantity | INT | Quantity sold |
| sales_amount | DECIMAL(18,2) | Total sales amount |





