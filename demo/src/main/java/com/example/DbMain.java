package com.example;

import java.io.*;
import java.sql.*;
import java.util.Properties;

public class DbMain {

    public static void main(String[] args) throws Exception {

        // leer config
        Properties cfg = new Properties();
        try (InputStream in = DbMain.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (in == null) throw new RuntimeException("No se encontro config.properties");
            cfg.load(in);
        }

        String url = cfg.getProperty("db.url");
        String user = cfg.getProperty("db.user");
        String password = cfg.getProperty("db.password");

        String mysqlUrl = cfg.getProperty("mysql.url");
        String mysqlUser = cfg.getProperty("mysql.user");
        String mysqlPassword = cfg.getProperty("mysql.password");

        System.out.println("=== DbComponent - Postgres ===");
        demoPostgres(url, user, password);

        System.out.println("\n=== DbComponent - MySQL ===");
        demoMysql(mysqlUrl, mysqlUser, mysqlPassword);
    }

    static void demoPostgres(String url, String user, String password) throws Exception {
        PostgresAdapter pg = new PostgresAdapter(url, user, password);

        try (DbComponent<PostgresAdapter> db = new DbComponent<>(pg, 5, 5000)) {

            // limpiar de corridas anteriores
            Connection drop = db.transaction("drop.table");
            drop.commit();
            db.endTransaction(drop);

            // 1. crear tabla
            Connection c = db.transaction("create.table");
            c.commit();
            db.endTransaction(c);
            System.out.println("tabla creada");

            // 2. insertar datos
            for (String nombre : new String[]{"Alice", "Bob", "Carlos"}) {
                Connection cx = db.transaction("insert.user", nombre);
                cx.commit();
                db.endTransaction(cx);
                System.out.println("insertado: " + nombre);
            }

            // 3. ver todos
            ResultSet rs = db.query("find.all.users");
            System.out.println("\nusuarios en la bd:");
            while (rs.next()) {
                System.out.println("  id=" + rs.getInt("id") + "  nombre=" + rs.getString("name"));
            }

            // 4. buscar por id
            ResultSet rs2 = db.query("find.user.by.id", 1);
            if (rs2.next()) {
                System.out.println("\nbusqueda id=1: " + rs2.getString("name"));
            }

            // 5. ping
            db.query("ping");
            System.out.println("ping ok");
        }
    }

    static void demoMysql(String url, String user, String password) throws Exception {
        MySQLAdapter mysql = new MySQLAdapter(url, user, password);

        try (DbComponent<MySQLAdapter> db = new DbComponent<>(mysql, 5, 5000)) {

            // limpiar de corridas anteriores
            Connection drop = db.transaction("drop.table");
            drop.commit();
            db.endTransaction(drop);

            // 1. crear tabla
            Connection c = db.transaction("create.table.mysql");
            c.commit();
            db.endTransaction(c);
            System.out.println("tabla creada");

            // 2. insertar datos
            for (String nombre : new String[]{"Alice", "Bob", "Carlos"}) {
                Connection cx = db.transaction("insert.user", nombre);
                cx.commit();
                db.endTransaction(cx);
                System.out.println("insertado: " + nombre);
            }

            // 3. ver todos
            ResultSet rs = db.query("find.all.users");
            System.out.println("\nusuarios en la bd:");
            while (rs.next()) {
                System.out.println("  id=" + rs.getInt("id") + "  nombre=" + rs.getString("name"));
            }

            // 4. buscar por id
            ResultSet rs2 = db.query("find.user.by.id", 1);
            if (rs2.next()) {
                System.out.println("\nbusqueda id=1: " + rs2.getString("name"));
            }

            // 5. ping
            db.query("ping");
            System.out.println("ping ok");
        }
    }
}
