package com.example;

import java.sql.*;

public interface IAdapter {
    Connection openConnection() throws SQLException;
}
