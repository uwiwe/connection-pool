package com.example;

import java.sql.*;

public class MySQLAdapter implements IAdapter {

    private final String url;
    private final String user;
    private final String password;

    public MySQLAdapter(String url, String user, String password) {
        this.url = url;
        this.user = user;
        this.password = password;
    }

    @Override
    public Connection openConnection() throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }
}
