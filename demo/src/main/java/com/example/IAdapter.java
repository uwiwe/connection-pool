package com.example;

import java.sql.Connection;
import java.sql.SQLException;

public interface IAdapter {
    Connection openConnection() throws SQLException;
}
