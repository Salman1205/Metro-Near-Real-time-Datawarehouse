Here's a refined and structured version of your README with improved clarity and consistency:  

---

# Metro Near Real-Time Data Warehouse  

This project sets up a **near real-time data warehouse** using **Java** and **SQL**. It includes Java code for data processing and SQL scripts for database setup and analysis.  

## 📁 Project Structure  

The final submission folder contains:  

1. **Eclipse Project Folder** – Includes the Java code for ETL and data processing.  
2. **SQL Scripts Folder** – Contains all SQL files required for database setup and OLAP analysis:  
   - `master_data.sql` (Master data schema)  
   - `DWH.sql` (Data warehouse schema)  
   - `OLAP.sql` (OLAP queries for analysis)  

## ⚙️ Prerequisites  

- **MySQL Connector/J**: Add the **MySQL Connector/J** JAR file to the project's build path in Eclipse to enable database connectivity.  
- **CSV Data Files**: Ensure `products_data.csv`, `customers_data.csv`, and `transactions.csv` are inside the Eclipse project folder for loading master and transaction data.  

## 🛠 Database Setup  

1. **Create Master Data Tables**:  
   - Run `master_data.sql` to create tables (`Products`, `Customers`, `Timedim`).  
2. **Create Data Warehouse Schema**:  
   - Execute `DWH.sql` to define the **star-schema** structure, including **fact** and **dimension tables**.  

## 🚀 Running the Project  

### 1️⃣ Load Master Data  
- Run `master_data.java` to **import CSV data** into the master tables.  
- Ensure database credentials in the code match your MySQL setup.  

### 2️⃣ Run MESHJOIN Algorithm  
- Execute `meshjoin2.java` to process transactions using **MESHJOIN**.  
- This step enriches transaction data and loads it into the Data Warehouse.  
- Wait for the **"Database connection closed"** message before running OLAP queries.  

### 3️⃣ Execute OLAP Queries  
- Run `OLAP.sql` in **MySQL Workbench** or another MySQL client to analyze revenue trends, supplier contributions, product affinities, and seasonal sales patterns.  

## 🔧 Configuration  

- **Database Credentials**:  
  - The system prompts for database **URL, username, and password** at runtime.  
- **File Paths**:  
  - Ensure `products_data.csv`, `customers_data.csv`, and `transactions.csv` are in the expected directory.  

## 📜 Included Files  

| File | Description |  
|------|------------|  
| `master_data.java` | Loads CSV data into master tables (`Products`, `Customers`, `Timedim`). |  
| `meshjoin2.java` | Implements **MESHJOIN** for transactional data integration. |  
| `master_data.sql` | SQL script to create master data tables. |  
| `DWH.sql` | SQL script to create the **star schema** for the Data Warehouse. |  
| `OLAP.sql` | Contains OLAP queries for sales and customer behavior analysis. |  

## 🔍 Common Issues & Fixes  

❌ **Database Connection Errors**  
✅ Verify **database URL, credentials, and MySQL Connector/J** setup.  

❌ **CSV File Not Found**  
✅ Ensure CSV files are **correctly formatted** and located in the expected directory.  
