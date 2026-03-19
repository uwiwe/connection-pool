package com.example;

import java.sql.Connection;
import java.sql.ResultSet;

public class DbMain {

    public static void main(String[] args) throws Exception {

        System.out.println("=== DbComponent Demo — H2 (in-memory) ===");
        demoH2();

        // Descomenta para probar con Postgres real:
        // System.out.println("\n=== DbComponent Demo — Postgres ===");
        // demoPostgres();
    }

    static void demoH2() throws Exception {
        H2Adapter h2 = new H2Adapter(
                "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1", "sa", "");

        try (DbComponent<H2Adapter> db = new DbComponent<>(h2, 3, 2000)) {

            // 1. Crear tabla (transacción)
            Connection c = db.transaction("create.table");
            c.commit();
            db.endTransaction(c);
            System.out.println("Tabla creada.");

            // 2. Insertar filas (transacciones)
            for (String nombre : new String[]{"Alice", "Bob", "Carlos"}) {
                Connection cx = db.transaction("insert.user", nombre);
                cx.commit();
                db.endTransaction(cx);
                System.out.println("Insertado: " + nombre);
            }

            // 3. Consultar todos (query)
            ResultSet rs = db.query("find.all.users");
            System.out.println("\nUsuarios en la BD:");
            while (rs.next()) {
                System.out.println("  id=" + rs.getInt("id")
                        + "  nombre=" + rs.getString("name"));
            }

            // 4. Buscar por ID (query con parámetro)
            ResultSet rs2 = db.query("find.user.by.id", 1);
            if (rs2.next()) {
                System.out.println("\nBúsqueda por id=1: " + rs2.getString("name"));
            }

            // 5. Ping
            db.query("ping");
            System.out.println("\nPing OK.");
        }
    }

    static void demoPostgres() throws Exception {
        PostgresAdapter pg = new PostgresAdapter(
                "jdbc:postgresql://localhost:5432/proyectovisual",
                "postgres", "miki/52511");

        try (DbComponent<PostgresAdapter> db = new DbComponent<>(pg, 5, 5000)) {
            ResultSet rs = db.query("ping");
            if (rs.next()) System.out.println("Postgres ping: " + rs.getInt(1));
        }
    }
}
