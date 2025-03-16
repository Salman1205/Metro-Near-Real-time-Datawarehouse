package project;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.TextStyle;
import java.util.Locale;
import java.util.Scanner;

public class master_data {

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter database URL (e.g., jdbc:mysql://localhost:3306/master_data): ");
        String url = scanner.nextLine();
        System.out.println("Enter database username: ");
        String user = scanner.nextLine();
        System.out.println("Enter database password: ");
        String password = scanner.nextLine();

        String productsFile = "products_data.csv";
        String customersFile = "customers_data.csv";

        checkFileExists(productsFile);
        checkFileExists(customersFile);

        loadDataToDimProducts(url, user, password, productsFile);
        loadDataToDimCustomers(url, user, password, customersFile);

        populateDimTime(url, user, password, LocalDate.of(2019, 1, 1), LocalDate.of(2024, 12, 31));

        validateDataLoading(url, user, password);
    }

    private static void checkFileExists(String filePath) {
        File file = new File(filePath);
        if (!file.exists()) {
            System.err.println("Error: File not found -> " + filePath);
            System.exit(1);
        }
    }

    private static void loadDataToDimProducts(String url, String user, String password, String filePath) {
        String insertQuery = "INSERT INTO Products (productID, productName, productPrice, supplierID, supplierName, storeID, storeName) " +
                             "VALUES (?, ?, ?, ?, ?, ?, ?) " +
                             "ON DUPLICATE KEY UPDATE " +
                             "productName = VALUES(productName), " +
                             "productPrice = VALUES(productPrice), " +
                             "supplierName = VALUES(supplierName), " +
                             "storeName = VALUES(storeName)";
        try (Connection connection = DriverManager.getConnection(url, user, password);
             BufferedReader br = new BufferedReader(new FileReader(filePath))) {

            String line;
            boolean isFirstLine = true;
            try (PreparedStatement preparedStatement = connection.prepareStatement(insertQuery)) {
                int batchCount = 0;
                int skippedCount = 0;
                while ((line = br.readLine()) != null) {
                    if (isFirstLine) {
                        isFirstLine = false;
                        continue;
                    }
                    try {
                        String[] values = line.split(",", -1);
                        if (values.length < 7) {
                            System.err.println("Skipping malformed line: " + line);
                            skippedCount++;
                            continue;
                        }
                        preparedStatement.setInt(1, parseInteger(values[0]));
                        preparedStatement.setString(2, values[1].trim()); 
                        preparedStatement.setDouble(3, parseDouble(values[2])); 
                        preparedStatement.setInt(4, parseInteger(values[3]));
                        preparedStatement.setString(5, values[4].trim());
                        preparedStatement.setInt(6, parseInteger(values[5])); 
                        preparedStatement.setString(7, values[6].trim()); 
                        preparedStatement.addBatch();
                        batchCount++;

                        if (batchCount % 100 == 0) {
                            preparedStatement.executeBatch();
                        }
                    } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
                        System.err.println("Skipping malformed line: " + line);
                        skippedCount++;
                    }
                }
                if (batchCount > 0) {
                    preparedStatement.executeBatch();
                }
                System.out.println("Data successfully loaded into Products table. Skipped lines: " + skippedCount);
            }

        } catch (IOException | SQLException e) {
            System.err.println("Error loading data into Products table: " + e.getMessage());
        }
    }

    private static void loadDataToDimCustomers(String url, String user, String password, String filePath) {
        String insertQuery = "INSERT INTO Customers (customer_id, customer_name, gender) " +
                             "VALUES (?, ?, ?) " +
                             "ON DUPLICATE KEY UPDATE " +
                             "customer_name = VALUES(customer_name), " +
                             "gender = VALUES(gender)";
        try (Connection connection = DriverManager.getConnection(url, user, password);
             BufferedReader br = new BufferedReader(new FileReader(filePath))) {

            String line;
            boolean isFirstLine = true;
            try (PreparedStatement preparedStatement = connection.prepareStatement(insertQuery)) {
                int batchCount = 0;
                int skippedCount = 0;
                while ((line = br.readLine()) != null) {
                    if (isFirstLine) {
                        isFirstLine = false;
                        continue;
                    }
                    try {
                        String[] values = line.split(",", -1);
                        if (values.length < 3) {
                            System.err.println("Skipping malformed line: " + line);
                            skippedCount++;
                            continue;
                        }
                        preparedStatement.setInt(1, parseInteger(values[0]));
                        preparedStatement.setString(2, values[1].trim());
                        preparedStatement.setString(3, values[2].trim()); 
                        preparedStatement.addBatch();
                        batchCount++;

                        if (batchCount % 100 == 0) {
                            preparedStatement.executeBatch();
                        }
                    } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
                        System.err.println("Skipping malformed line: " + line);
                        skippedCount++;
                    }
                }
                if (batchCount > 0) {
                    preparedStatement.executeBatch();
                }
                System.out.println("Data successfully loaded into Customers table. Skipped lines: " + skippedCount);
            }

        } catch (IOException | SQLException e) {
            System.err.println("Error loading data into Customers table: " + e.getMessage());
        }
    }

    private static void populateDimTime(String url, String user, String password, LocalDate startDate, LocalDate endDate) {
        String insertQuery = "INSERT INTO Timedim (date, year, month, day, weekday, is_weekend) " +
                             "VALUES (?, ?, ?, ?, ?, ?) " +
                             "ON DUPLICATE KEY UPDATE " +
                             "year = VALUES(year), " +
                             "month = VALUES(month), " +
                             "day = VALUES(day), " +
                             "weekday = VALUES(weekday), " +
                             "is_weekend = VALUES(is_weekend)";

        try (Connection connection = DriverManager.getConnection(url, user, password);
             PreparedStatement preparedStatement = connection.prepareStatement(insertQuery)) {

            LocalDate currentDate = startDate;

            while (!currentDate.isAfter(endDate)) {
                try {
                    preparedStatement.setDate(1, java.sql.Date.valueOf(currentDate)); 
                    preparedStatement.setInt(2, currentDate.getYear()); 
                    preparedStatement.setInt(3, currentDate.getMonthValue()); 
                    preparedStatement.setInt(4, currentDate.getDayOfMonth()); 
                    preparedStatement.setString(5, currentDate.getDayOfWeek()
                                                               .getDisplayName(TextStyle.FULL, Locale.ENGLISH));
                    preparedStatement.setBoolean(6, isWeekend(currentDate)); 

                    preparedStatement.addBatch();
                } catch (SQLException e) {
                    System.err.println("Error processing date: " + currentDate + " - " + e.getMessage());
                }

                currentDate = currentDate.plusDays(1);
            }

            preparedStatement.executeBatch();
            System.out.println("Time dimension successfully populated.");

        } catch (SQLException e) {
            System.err.println("Error populating Timedim table: " + e.getMessage());
        }
    }

    private static boolean isWeekend(LocalDate date) {
        return date.getDayOfWeek().getValue() == 6 || date.getDayOfWeek().getValue() == 7;
    }

    private static void validateDataLoading(String url, String user, String password) {
        try (Connection connection = DriverManager.getConnection(url, user, password)) {
            String[] tables = {"Products", "Customers", "Timedim"};

            for (String table : tables) {
                String query = "SELECT COUNT(*) FROM " + table;
                try (PreparedStatement pstmt = connection.prepareStatement(query);
                     ResultSet rs = pstmt.executeQuery()) {
                    if (rs.next()) {
                        int rowCount = rs.getInt(1);
                        System.out.println("Row count for " + table + ": " + rowCount);
                    }
                }
            }

        } catch (SQLException e) {
            System.err.println("Error validating master data loading: " + e.getMessage());
        }
    }

    private static int parseInteger(String value) {
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private static double parseDouble(String value) {
        try {
            return Double.parseDouble(value.trim().replace("$", ""));
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }
}