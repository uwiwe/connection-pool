package com.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class H2Adapter implements IAdapter {

    private final String url;
    private final String user;
    private final String password;

    public H2Adapter(String url, String user, String password) {
        this.url      = url;
        this.user     = user;
        this.password = password;
    }

    @Override
    public Connection openConnection() throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }
}
