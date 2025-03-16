DROP DATABASE IF EXISTS master_data;
CREATE DATABASE master_data;
USE master_data;

-- Customers Dim
CREATE TABLE Customers (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    gender ENUM('Male', 'Female') NOT NULL
);

-- Products Dim
CREATE TABLE Products (
    productID INT PRIMARY KEY,
    productName VARCHAR(100) NOT NULL,
    productPrice DECIMAL(10, 2) NOT NULL,
    supplierID INT NOT NULL,
    supplierName VARCHAR(100) NOT NULL,
    storeID INT NOT NULL,
    storeName VARCHAR(100) NOT NULL
);

-- Time Dime
CREATE TABLE Timedim (
    timeID INT AUTO_INCREMENT PRIMARY KEY,
    date DATE NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    weekday VARCHAR(10) NOT NULL,
    is_weekend BOOLEAN NOT NULL
);
