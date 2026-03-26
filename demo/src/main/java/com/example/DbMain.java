package com.example;

import java.io.*;
import java.sql.*;
import java.util.Properties;

// cd demo
// mvn exec:java

public class DbMain {

    public static void main(String[] args) throws Exception {

        // leer config
        Properties cfg = new Properties();
        InputStream in = DbMain.class.getClassLoader().getResourceAsStream("config.properties");
        cfg.load(in);

        String url = cfg.getProperty("db.url");
        String user = cfg.getProperty("db.user");
        String password = cfg.getProperty("db.password");

        String mysqlUrl = cfg.getProperty("mysql.url");
        String mysqlUser = cfg.getProperty("mysql.user");
        String mysqlPassword = cfg.getProperty("mysql.password");

        int poolSize = Integer.parseInt(cfg.getProperty("poolSize"));
        long timeoutMs = Long.parseLong(cfg.getProperty("poolConnectionTimeoutMs"));

        // --- postgres ---
        System.out.println("=== DbComponent - Postgres ===");

        PostgresAdapter pg = new PostgresAdapter(url, user, password);
        DbComponent db = new DbComponent(pg, poolSize, timeoutMs);

        // limpiar y crear tabla
        db.transaction(new String[]{"drop.table", "create.table"}, null);
        System.out.println("tabla creada");

        // insertar los 3 usuarios en una sola transaccion
        db.transaction(
            new String[]{"insert.user", "insert.user", "insert.user"},
            new Object[][]{{"Alice"}, {"Bob"}, {"Carlos"}}
        );
        System.out.println("insertados: Alice, Bob, Carlos");

        // ver todos
        ResultSet rs = db.query("find.all.users");
        System.out.println("\nusuarios en la bd:");
        while (rs.next()) {
            System.out.println("  id=" + rs.getInt("id") + "  nombre=" + rs.getString("name"));
        }

        // buscar por id
        ResultSet rs2 = db.query("find.user.by.id", 1);
        if (rs2.next()) {
            System.out.println("\nbusqueda id=1: " + rs2.getString("name"));
        }

        // ping
        db.query("ping");
        System.out.println("ping ok");

        db.close();

        // --- mysql ---
        System.out.println("\n=== DbComponent - MySQL ===");

        MySQLAdapter mysql = new MySQLAdapter(mysqlUrl, mysqlUser, mysqlPassword);
        DbComponent db2 = new DbComponent(mysql, poolSize, timeoutMs);

        // limpiar y crear tabla
        db2.transaction(new String[]{"drop.table", "create.table.mysql"}, null);
        System.out.println("tabla creada");

        // insertar los 3 usuarios en una sola transaccion
        db2.transaction(
            new String[]{"insert.user", "insert.user", "insert.user"},
            new Object[][]{{"Alice"}, {"Bob"}, {"Carlos"}}
        );
        System.out.println("insertados: Alice, Bob, Carlos");

        // ver todos
        ResultSet rs3 = db2.query("find.all.users");
        System.out.println("\nusuarios en la bd:");
        while (rs3.next()) {
            System.out.println("  id=" + rs3.getInt("id") + "  nombre=" + rs3.getString("name"));
        }

        // buscar por id
        ResultSet rs4 = db2.query("find.user.by.id", 1);
        if (rs4.next()) {
            System.out.println("\nbusqueda id=1: " + rs4.getString("name"));
        }

        // ping
        db2.query("ping");
        System.out.println("ping ok");

        db2.close();
    }
}
