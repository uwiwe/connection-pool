package com.example;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.moandjiezana.toml.Toml;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;

public class DbComponent<A extends IAdapter> {

    private A adapter;
    private Properties queries;
    private ArrayBlockingQueue<Connection> pool;
    private long timeoutMs;

    public DbComponent(A adapter, int poolSize, long timeoutMs) throws Exception {
        this.adapter = adapter;
        this.timeoutMs = timeoutMs;
        this.queries = loadQueries();

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

    // ejecuta varias queries en una sola transaccion, hace commit si todo sale bien o rollback si algo falla
    public void transaction(String[] nombres, Object[][] params) throws Exception {
        Connection c = pool.poll(timeoutMs, TimeUnit.MILLISECONDS);
        if (c == null) throw new RuntimeException("no habia conexiones en el pool");
        c.setAutoCommit(false);
        try {
            for (int i = 0; i < nombres.length; i++) {
                String sql = getQuery(nombres[i]);
                PreparedStatement ps = c.prepareStatement(sql);
                Object[] p = (params != null && i < params.length && params[i] != null) ? params[i] : new Object[0];
                for (int j = 0; j < p.length; j++) {
                    ps.setObject(j + 1, p[j]);
                }
                ps.executeUpdate();
            }
            c.commit();
        } catch (Exception e) {
            c.rollback();
            throw e;
        } finally {
            c.setAutoCommit(true);
            pool.offer(c);
        }
    }

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

    // busca el archivo de queries en los formatos soportados (json, yaml, toml, properties)
    private static Properties loadQueries() throws Exception {
        ClassLoader cl = DbComponent.class.getClassLoader();

        InputStream json = cl.getResourceAsStream("queries.json");
        if (json != null) {
            ObjectMapper mapper = new ObjectMapper();
            Map<String, String> mapa = mapper.readValue(json, new TypeReference<Map<String, String>>() {});
            Properties p = new Properties();
            p.putAll(mapa);
            return p;
        }

        InputStream yaml = cl.getResourceAsStream("queries.yaml");
        if (yaml != null) {
            Map<String, String> mapa = new Yaml().load(yaml);
            Properties p = new Properties();
            p.putAll(mapa);
            return p;
        }

        InputStream toml = cl.getResourceAsStream("queries.toml");
        if (toml != null) {
            Properties p = new Properties();
            new Toml().read(toml).toMap().forEach((k, v) -> p.setProperty(k, v.toString()));
            return p;
        }

        InputStream props = cl.getResourceAsStream("queries.properties");
        if (props != null) {
            Properties p = new Properties();
            p.load(props);
            return p;
        }

        throw new RuntimeException("no se encontro archivo de queries (json/yaml/toml/properties)");
    }
}
