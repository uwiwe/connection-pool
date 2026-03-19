package com.example;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class DbComponent<A extends IAdapter> implements AutoCloseable {

    private final A adapter;
    private final Properties queries;
    private final BlockingQueue<Connection> pool;
    private final long timeoutMs;

    /**
     * @param adapter   el adaptador concreto (Postgres o H2)
     * @param poolSize  cantidad de conexiones a pre-abrir
     * @param timeoutMs tiempo máximo de espera para obtener una conexión (ms)
     */
    public DbComponent(A adapter, int poolSize, long timeoutMs) throws Exception {
        this.adapter   = adapter;
        this.timeoutMs = timeoutMs;
        this.queries   = loadQueries();
        this.pool      = new ArrayBlockingQueue<>(poolSize);

        for (int i = 0; i < poolSize; i++) {
            pool.offer(adapter.openConnection());
        }
    }

    /**
     * Ejecuta una query predefinida (SELECT). Pide conexión del pool,
     * ejecuta y la devuelve al pool.
     */
    public ResultSet query(String queryName, Object... params) throws Exception {
        String sql = requireQuery(queryName);
        Connection c = borrow();
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

    /**
     * Ejecuta una query predefinida dentro de una transacción (INSERT/UPDATE/DELETE).
     * Retorna la conexión con autoCommit=false para que el caller haga commit/rollback.
     * Siempre llamar endTransaction(c) al terminar.
     */
    public Connection transaction(String queryName, Object... params) throws Exception {
        String sql = requireQuery(queryName);
        Connection c = borrow();
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

    /**
     * Devuelve la conexión transaccional al pool (llamar después de commit/rollback).
     */
    public void endTransaction(Connection c) throws SQLException {
        c.setAutoCommit(true);
        pool.offer(c);
    }

    @Override
    public void close() throws Exception {
        for (Connection c : pool) {
            try { c.close(); } catch (Exception ignored) {}
        }
    }

    // --- privados ---

    private Connection borrow() throws Exception {
        Connection c = pool.poll(timeoutMs, TimeUnit.MILLISECONDS);
        if (c == null) throw new RuntimeException("Pool timeout: no hay conexiones disponibles");
        return c;
    }

    private String requireQuery(String name) {
        String sql = queries.getProperty(name);
        if (sql == null) throw new IllegalArgumentException("Query no encontrada: " + name);
        return sql.trim();
    }

    private static Properties loadQueries() throws Exception {
        Properties p = new Properties();
        try (InputStream in =
                DbComponent.class.getClassLoader().getResourceAsStream("queries.properties")) {
            if (in == null) throw new RuntimeException("queries.properties no encontrado en classpath");
            p.load(in);
        }
        return p;
    }
}
