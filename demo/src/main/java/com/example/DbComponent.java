package com.example;

import java.io.*;
import java.sql.*;
import java.util.Properties;
import java.util.concurrent.*;

public class DbComponent<A extends IAdapter> implements AutoCloseable {

    private final A adapter;
    private final Properties queries;
    private final ArrayBlockingQueue<Connection> pool;
    private final long timeoutMs;

    public DbComponent(A adapter, int poolSize, long timeoutMs) throws Exception {
        this.adapter = adapter;
        this.timeoutMs = timeoutMs;

        // cargar queries del archivo
        Properties p = new Properties();
        try (InputStream in = DbComponent.class.getClassLoader().getResourceAsStream("queries.properties")) {
            if (in == null) throw new RuntimeException("No se encontro queries.properties");
            p.load(in);
        }
        this.queries = p;

        // inicializar el pool
        this.pool = new ArrayBlockingQueue<>(poolSize);
        for (int i = 0; i < poolSize; i++) {
            pool.offer(adapter.openConnection());
        }
    }

    // ejecuta una query del archivo, no acepta sql directo
    public ResultSet query(String nombre, Object... params) throws Exception {
        String sql = getQuery(nombre);
        Connection c = pool.poll(timeoutMs, TimeUnit.MILLISECONDS);
        if (c == null) throw new RuntimeException("no habia conexiones en el pool");
        try {
            PreparedStatement ps = c.prepareStatement(sql);
            for (int i = 0; i < params.length; i++) {
                ps.setObject(i + 1, params[i]);
            }
            return ps.executeQuery();
        } finally {
            pool.offer(c);
        }
    }

    // ejecuta en transaccion, devuelve la conexion para hacer commit/rollback afuera
    public Connection transaction(String nombre, Object... params) throws Exception {
        String sql = getQuery(nombre);
        Connection c = pool.poll(timeoutMs, TimeUnit.MILLISECONDS);
        if (c == null) throw new RuntimeException("no habia conexiones en el pool");
        c.setAutoCommit(false);
        try {
            PreparedStatement ps = c.prepareStatement(sql);
            for (int i = 0; i < params.length; i++) {
                ps.setObject(i + 1, params[i]);
            }
            ps.executeUpdate();
            return c;
        } catch (Exception e) {
            c.rollback();
            c.setAutoCommit(true);
            pool.offer(c);
            throw e;
        }
    }

    // hay que llamar esto despues del commit/rollback para liberar la conexion
    public void endTransaction(Connection c) throws SQLException {
        c.setAutoCommit(true);
        pool.offer(c);
    }

    @Override
    public void close() throws Exception {
        for (Connection c : pool) {
            try { c.close(); } catch (Exception e) {}
        }
    }

    private String getQuery(String nombre) {
        String sql = queries.getProperty(nombre);
        if (sql == null) throw new IllegalArgumentException("no existe esa query: " + nombre);
        return sql;
    }
}
