 Project Structure
The final submission folder includes:
1. Eclipse Project Folder: Contains the Java code for the project.
2. SQL Scripts Folder: Contains all SQL files ('master_data.sql', 'DWH.sql', 'OLAP.sql') needed for setting up the database.

 Prerequisites
- MySQL Connector/J: Ensure that you add the MySQL Connector/J JAR file to your project's build path in Eclipse to enable database connectivity.
- CSV Data Files: You need to place 'products_data.csv', 'customers_data.csv', and 'transactions.csv' files inside the eclipse project folder for loading master and transaction data.

 1. Database Setup
- Create Master Data: Use the SQL script in 'master_data.sql' to create the master data tables ('Products', 'Customers', 'Timedim').
- Create Data Warehouse Schema: Use 'DWH.sql' to create the star schema for the Data Warehouse, including the fact and dimension tables.

 2. Running the Code
1. Load Master Data: Run the 'master_data.java' code to load data from CSV files into the master data tables ('Products', 'Customers', 'Timedim'). Ensure that the database credentials in the code match your setup.
2. Run MESHJOIN: Execute 'meshjoin2.java' to process transactions using the MESHJOIN algorithm. This will enrich the transaction data and load it into the Data Warehouse. Wait until it says database connection closed before running olap queries.
3. Execute OLAP Queries: Use 'OLAP.sql' to run the OLAP queries on the DW for analysis.

 3. Configuration
- Database Credentials: You will be prompted to enter the database URLs, username, and password during runtime for both 'master_data.java' and 'meshjoin2.java'.
- File Paths: Ensure that the CSV files ('products_data.csv', 'customers_data.csv', 'transactions.csv') are in the correct directory as expected by the Java code.

 Running the Project
1. Compile and Run: Open the project in Eclipse and ensure that the MySQL Connector/J JAR file is added to the project's build path. Then run 'master_data.java' to populate the master tables.
2. MESHJOIN Processing: Run 'meshjoin2.java' to apply MESHJOIN, which will take transactions from the CSV and join them with the master data.
3. Analyze with OLAP Queries: Run the OLAP queries in 'OLAP.sql' using MySQL Workbench or any other MySQL client to get insights.

 Files Included
- master_data.java: Loads data into master tables ('Products', 'Customers', 'Timedim').
- meshjoin2.java: Implements MESHJOIN to join customer transactions with master data and loads them into the Data Warehouse.
- master_data.sql: SQL script to create master data tables.
- DWH.sql: SQL script to create the Data Warehouse star schema.
- OLAP.sql: Contains the OLAP queries for analyzing the Data Warehouse.

 Common Issues
- Database Connection Errors: Ensure database URLs, credentials, and MySQL Connector/J setup are correct.
- CSV File Errors: Make sure the CSV files are correctly formatted and located in the expected directory.
