package com.company;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBConnector {
    private static final String CONNECTION_STRING = "jdbc:mysql://localhost:3306/file_processor?rewriteBatchedStatements=true";
    private static final String USER_NAME = "root";
    private static final String PASSWORD = "root";
    static Connection connection;

    public static Connection getConnection() {
        if (null != connection) {
            return connection;
        } else {
            try {
                //JDBC drive check...`com.mysql.jdbc.Driver` -- this has been deprecated
                Class.forName("com.mysql.cj.jdbc.Driver");
                connection = DriverManager.getConnection(CONNECTION_STRING, USER_NAME, PASSWORD);
//                connection.setAutoCommit(false);
            } catch (ClassNotFoundException| SQLException e) {
                e.printStackTrace();
            }
        }
        return connection;
    }

    public static void close(){
        try {
            connection.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

}
