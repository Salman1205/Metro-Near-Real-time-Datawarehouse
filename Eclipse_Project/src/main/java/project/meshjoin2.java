package project;

import java.sql.*;
import java.util.*;
import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.math.BigDecimal;

public class meshjoin2 {

    private static final int DISK_BUFFER_SIZE = 10; 
    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final String TRANSACTIONS_CSV = "transactions.csv";

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        // Prompt user for database credentials
        System.out.println("Enter database URL for Master_data (e.g., jdbc:mysql://localhost:3306/master_data): ");
        String masterDbUrl = scanner.nextLine();
        System.out.println("Enter database URL for METRO_DWH (e.g., jdbc:mysql://localhost:3306/metro_dwh): ");
        String metroDbUrl = scanner.nextLine();
        System.out.println("Enter database username: ");
        String dbUser = scanner.nextLine();
        System.out.println("Enter database password: ");
        String dbPassword = scanner.nextLine();

        try {
            Connection masterConn = DriverManager.getConnection(masterDbUrl, dbUser, dbPassword);
            Connection metroConn = DriverManager.getConnection(metroDbUrl, dbUser, dbPassword);

            System.out.println("Connected to the databases.");

            populateDimensionTables(metroConn, masterConn);

            meshJoin(metroConn, masterConn);

            masterConn.close();
            metroConn.close();
            System.out.println("Database connections closed.");
        } catch (SQLException e) {
            System.err.println("Database connection error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void populateDimensionTables(Connection metroConn, Connection masterConn) throws SQLException {
        copyTable(masterConn, metroConn, "Customers", "customer_id");

        copyTable(masterConn, metroConn, "Products", "productID");

        populateTimeDimension(metroConn);
    }

    private static void copyTable(Connection sourceConn, Connection targetConn, String tableName, String primaryKey) throws SQLException {
        String query = "SELECT * FROM " + tableName;
        try (Statement stmt = sourceConn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            StringBuilder insertQuery = new StringBuilder("INSERT INTO " + tableName + " VALUES (");
            for (int i = 0; i < columnCount; i++) {
                insertQuery.append("?");
                if (i < columnCount - 1) insertQuery.append(", ");
            }
            insertQuery.append(") ON DUPLICATE KEY UPDATE ");

            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnName(i);
                if (!columnName.equalsIgnoreCase(primaryKey)) {
                    insertQuery.append(columnName).append(" = VALUES(").append(columnName).append(")");
                    if (i < columnCount) insertQuery.append(", ");
                }
            }
            if (insertQuery.toString().endsWith(", ")) {
                insertQuery.setLength(insertQuery.length() - 2);
            }

            try (PreparedStatement pstmt = targetConn.prepareStatement(insertQuery.toString())) {
                while (rs.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        pstmt.setObject(i, rs.getObject(i));
                    }
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }
            System.out.println("Data successfully copied to " + tableName + " table.");
        }
    }

    private static void populateTimeDimension(Connection metroConn) throws SQLException {
        String insertQuery = "INSERT INTO Timedim (date, year, month, day, weekday, is_weekend) " +
                "VALUES (?, ?, ?, ?, ?, ?)";

        try (PreparedStatement pstmt = metroConn.prepareStatement(insertQuery)) {
            Calendar start = Calendar.getInstance();
            start.set(2019, Calendar.JANUARY, 1);
            Calendar end = Calendar.getInstance();
            end.set(2024, Calendar.DECEMBER, 31);

            while (!start.after(end)) {
                java.sql.Date sqlDate = new java.sql.Date(start.getTimeInMillis());
                int year = start.get(Calendar.YEAR);
                int month = start.get(Calendar.MONTH) + 1;
                int day = start.get(Calendar.DAY_OF_MONTH);
                String weekday = new SimpleDateFormat("EEEE").format(start.getTime());
                boolean isWeekend = start.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY ||
                        start.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY;

                pstmt.setDate(1, sqlDate);
                pstmt.setInt(2, year);
                pstmt.setInt(3, month);
                pstmt.setInt(4, day);
                pstmt.setString(5, weekday);
                pstmt.setBoolean(6, isWeekend);
                pstmt.addBatch();

                start.add(Calendar.DAY_OF_MONTH, 1);
            }

            pstmt.executeBatch();
            System.out.println("Timedim table successfully populated.");
        }
    }

    private static void meshJoin(Connection metroConn, Connection masterConn) throws SQLException {
        Queue<Map<String, Object>> transactionBuffer = new LinkedList<>();
        Map<String, Map<Integer, Map<String, Object>>> masterDataMap = new HashMap<>();

        Set<Integer> insertedTransactionIds = getExistingTransactionIds(metroConn);
        loadMasterData(masterConn, masterDataMap);

        try (BufferedReader br = new BufferedReader(new FileReader(TRANSACTIONS_CSV))) {
            String line;
            br.readLine(); 

            SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);

            while ((line = br.readLine()) != null) {
                try {
                    String[] values = line.split(",");
                    Map<String, Object> transaction = new HashMap<>();
                    int transactionID = Integer.parseInt(values[0].trim());

                    if (insertedTransactionIds.contains(transactionID)) {
                        continue;
                    }

                    transaction.put("transactionID", transactionID);
                    transaction.put("orderDate", new Timestamp(sdf.parse(values[1].trim()).getTime()));
                    transaction.put("productID", Integer.parseInt(values[2].trim()));
                    transaction.put("quantity", Integer.parseInt(values[3].trim()));
                    transaction.put("customerID", Integer.parseInt(values[4].trim()));
                    transactionBuffer.add(transaction);

                    if (transactionBuffer.size() >= DISK_BUFFER_SIZE) {
                        processTransactions(metroConn, transactionBuffer, masterDataMap, insertedTransactionIds);
                    }
                } catch (ParseException e) {
                    System.err.println("Error parsing date: " + e.getMessage());
                }
            }

            if (!transactionBuffer.isEmpty()) {
                processTransactions(metroConn, transactionBuffer, masterDataMap, insertedTransactionIds);
            }
        } catch (IOException e) {
            System.err.println("Error reading transactions CSV file.");
        }
    }

    private static void processTransactions(
            Connection metroConn,
            Queue<Map<String, Object>> transactionBuffer,
            Map<String, Map<Integer, Map<String, Object>>> masterDataMap,
            Set<Integer> insertedTransactionIds) throws SQLException {

        String insertFactSQL = "INSERT INTO Sales (transactionID, orderDate, productID, customerID, quantity, totalRevenue) " +
                "VALUES (?, ?, ?, ?, ?, ?)";

        try (PreparedStatement factStmt = metroConn.prepareStatement(insertFactSQL)) {
            while (!transactionBuffer.isEmpty()) {
                Map<String, Object> transaction = transactionBuffer.poll();
                int transactionID = (int) transaction.get("transactionID");

                if (insertedTransactionIds.contains(transactionID)) {
                    continue;
                }

                int productID = (int) transaction.get("productID");
                int customerID = (int) transaction.get("customerID");

                Map<String, Object> product = masterDataMap.get("Products").get(productID);
                if (product != null) {
                    transaction.put("productPrice", product.get("productPrice"));
                    transaction.put("supplierName", product.get("supplierName"));

                    Map<String, Object> customer = masterDataMap.get("Customers").get(customerID);
                    if (customer != null) {
                        transaction.put("customerName", customer.get("customer_name"));
                        transaction.put("gender", customer.get("gender"));
                    }

                    BigDecimal productPrice = (BigDecimal) product.get("productPrice");
                    int quantity = (int) transaction.get("quantity");
                    double totalRevenue = productPrice.doubleValue() * quantity;

                    factStmt.setInt(1, transactionID);
                    factStmt.setTimestamp(2, (Timestamp) transaction.get("orderDate"));
                    factStmt.setInt(3, productID);
                    factStmt.setInt(4, customerID);
                    factStmt.setInt(5, quantity);
                    factStmt.setDouble(6, totalRevenue);
                   

                    factStmt.addBatch();

                    insertedTransactionIds.add(transactionID);
                } else {
                    System.err.println("Skipping transaction ID: " + transactionID + " due to missing product data for productID: " + productID);
                }
            }
            factStmt.executeBatch();
        }
    }

    private static Set<Integer> getExistingTransactionIds(Connection metroConn) throws SQLException {
        Set<Integer> transactionIds = new HashSet<>();
        String sql = "SELECT transactionID FROM Sales";

        try (Statement stmt = metroConn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                transactionIds.add(rs.getInt("transactionID"));
            }
        }
        return transactionIds;
    }

    private static void loadMasterData(Connection masterConn, Map<String, Map<Integer, Map<String, Object>>> masterDataMap) throws SQLException {
        masterDataMap.put("Products", new HashMap<>());
        masterDataMap.put("Customers", new HashMap<>());

        loadTableIntoMap(masterConn, "Products", "productID", masterDataMap.get("Products"));
        loadTableIntoMap(masterConn, "Customers", "customer_id", masterDataMap.get("Customers"));
    }

    private static void loadTableIntoMap(Connection masterConn, String tableName, String keyColumn, Map<Integer, Map<String, Object>> map) throws SQLException {
        String query = "SELECT * FROM " + tableName;

        try (Statement stmt = masterConn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            ResultSetMetaData metaData = rs.getMetaData();
            while (rs.next()) {
                Map<String, Object> record = new HashMap<>();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    record.put(metaData.getColumnName(i), rs.getObject(i));
                }
                map.put((Integer) record.get(keyColumn), record);
            }
        }
    }
}