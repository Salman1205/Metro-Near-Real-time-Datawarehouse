DROP DATABASE IF EXISTS metro_dwh;
CREATE DATABASE metro_dwh;
USE metro_dwh;

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

-- Fact Table
CREATE TABLE Sales (
    transactionID INT PRIMARY KEY,
    orderDate DATETIME NOT NULL,
    productID INT NOT NULL,
    customerID INT NOT NULL,
    quantity INT NOT NULL CHECK (quantity > 0),
    totalRevenue DECIMAL(15, 2) NOT NULL,
    timeID INT,
    FOREIGN KEY (productID) REFERENCES Products(productID),
    FOREIGN KEY (customerID) REFERENCES Customers(customer_id),
    FOREIGN KEY (timeID) REFERENCES Timedim(timeID)
);

