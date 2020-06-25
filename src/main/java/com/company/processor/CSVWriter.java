package com.company.processor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.company.processor.CSVFileProcessor.ERROR_COUNT;

public class CSVWriter implements Runnable {

    private static final String SQL_STATEMENT = "insert into `product` (name, sku, description)" +
            " values (?,?,?)";
    private static final String UPSERT_STATEMENT = "INSERT INTO product (`name`,sku, description) " +
            "VALUES (?,?,?) ON DUPLICATE KEY UPDATE `name`=VALUES(name), " +
            "`description`=VALUES(description);";
    List<String[]> lines;
    String splitBy;
    Connection connection;

    public CSVWriter(final List<String[]> lines, String splitBy, Connection connection) {
        this.lines = lines;
        this.splitBy = splitBy;
        this.connection = connection;
    }

    @Override
    public void run() {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(UPSERT_STATEMENT);
            for (String[] data : lines) {
                preparedStatement.setString(1, data[0]);
                preparedStatement.setString(2, data[1]);
                preparedStatement.setString(3, data[2]);
                preparedStatement.addBatch();
            }

            int[] rows = preparedStatement.executeBatch();
//            System.out.println("Batch size = "+preparedStatement.getLargeUpdateCount());
        } catch (SQLException throwables) {
            ERROR_COUNT++;
            throwables.printStackTrace();
        }
    }
}
