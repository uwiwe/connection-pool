package com.example;

import java.io.*;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class PoolSimApp {

    // =============================
    // CONFIG
    // =============================
    static class Config {
        final String url, user, password, query, logFile;
        final int samples, maxRetries;
        final int poolMaxSize;
        final int poolMinIdle;
        final long poolTimeout;

        Config(Properties p) {
            url = req(p, "db.url");
            user = req(p, "db.user");
            password = req(p, "db.password");
            query = req(p, "db.query");

            samples = Integer.parseInt(p.getProperty("samples", "20").trim());
            maxRetries = Integer.parseInt(p.getProperty("maxRetries", "1").trim());
            logFile = p.getProperty("log.file", "simulator.log").trim();

            poolMaxSize = Integer.parseInt(p.getProperty("pool.maxSize", "10"));
            poolMinIdle = Integer.parseInt(p.getProperty("pool.minIdle", "2"));
            poolTimeout = Long.parseLong(p.getProperty("pool.connectionTimeoutMs", "30000"));
        }

        static String req(Properties p, String k) {
            String v = p.getProperty(k);
            if (v == null || v.isBlank())
                throw new IllegalArgumentException("Falta config: " + k);
            return v.trim();
        }
    }

    // =============================
    // LOGGER
    // =============================
    static class SimpleLogger implements Closeable {
        private final BufferedWriter out;
        private final DateTimeFormatter fmt =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

        SimpleLogger(String file) throws IOException {
            out = new BufferedWriter(new FileWriter(file, false));
        }

        synchronized void log(String line) {
            try {
                out.write(line);
                out.newLine();
                out.flush();
            } catch (IOException e) {
                System.err.println("ERROR log: " + e.getMessage());
            }
        }

        String ts() { return LocalDateTime.now().format(fmt); }

        @Override public void close() throws IOException { out.close(); }
    }

    // =============================
    // METRICS
    // =============================
    static class Metrics {
        final LongAdder ok = new LongAdder();
        final LongAdder fail = new LongAdder();
        final LongAdder retries = new LongAdder();

        void record(boolean success, int retriesUsed) {
            if (success) ok.increment();
            else fail.increment();
            retries.add(retriesUsed);
        }
    }

    // =============================
    // WORKER RAW
    // =============================
    static class WorkerRaw implements Runnable {
        private final int id;
        private final Config cfg;
        private final CountDownLatch ready;
        private final CountDownLatch start;
        private final SimpleLogger logger;
        private final Metrics metrics;

        WorkerRaw(int id, Config cfg, CountDownLatch ready, CountDownLatch start,
                  SimpleLogger logger, Metrics metrics) {
            this.id = id;
            this.cfg = cfg;
            this.ready = ready;
            this.start = start;
            this.logger = logger;
            this.metrics = metrics;
        }

        @Override
        public void run() {
            ready.countDown();
            try { start.await(); } catch (InterruptedException e) { return; }

            boolean success = false;
            int retriesUsed = 0;
            long t0 = System.nanoTime();

            for (int attempt = 1; attempt <= (cfg.maxRetries + 1); attempt++) {
                try (Connection c = DriverManager.getConnection(cfg.url, cfg.user, cfg.password);
                     PreparedStatement ps = c.prepareStatement(cfg.query)) {

                    ps.execute();
                    success = true;
                    retriesUsed = attempt - 1;
                    break;

                } catch (Exception ex) {
                    if (attempt == cfg.maxRetries + 1)
                        retriesUsed = attempt - 1;
                }
            }

            long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
            metrics.record(success, retriesUsed);

            logger.log("%s - raw - id=%d - %s - reintentos=%d - tiempo=%d ms"
                    .formatted(logger.ts(), id,
                            success ? "exitoso" : "fallido",
                            retriesUsed, ms));
        }
    }

    // =============================
    // WORKER POOLED
    // =============================
    static class WorkerPooled implements Runnable {
        private final int id;
        private final Config cfg;
        private final CountDownLatch ready;
        private final CountDownLatch start;
        private final SimpleLogger logger;
        private final Metrics metrics;
        private final HikariDataSource ds;

        WorkerPooled(int id, Config cfg, CountDownLatch ready, CountDownLatch start,
                     SimpleLogger logger, Metrics metrics, HikariDataSource ds) {
            this.id = id;
            this.cfg = cfg;
            this.ready = ready;
            this.start = start;
            this.logger = logger;
            this.metrics = metrics;
            this.ds = ds;
        }

        @Override
        public void run() {
            ready.countDown();
            try { start.await(); } catch (InterruptedException e) { return; }

            boolean success = false;
            int retriesUsed = 0;
            long t0 = System.nanoTime();

            for (int attempt = 1; attempt <= (cfg.maxRetries + 1); attempt++) {
                try (Connection c = ds.getConnection();
                     PreparedStatement ps = c.prepareStatement(cfg.query)) {

                    ps.execute();
                    success = true;
                    retriesUsed = attempt - 1;
                    break;

                } catch (Exception ex) {
                    if (attempt == cfg.maxRetries + 1)
                        retriesUsed = attempt - 1;
                }
            }

            long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
            metrics.record(success, retriesUsed);

            logger.log("%s - pooled - id=%d - %s - reintentos=%d - tiempo=%d ms"
                    .formatted(logger.ts(), id,
                            success ? "exitoso" : "fallido",
                            retriesUsed, ms));
        }
    }

    static HikariDataSource createPool(Config cfg) {
        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl(cfg.url);
        hc.setUsername(cfg.user);
        hc.setPassword(cfg.password);
        hc.setMaximumPoolSize(cfg.poolMaxSize);
        hc.setMinimumIdle(cfg.poolMinIdle);
        hc.setConnectionTimeout(cfg.poolTimeout);
        return new HikariDataSource(hc);
    }

    // =============================
    // MAIN CON MENÚ
    // =============================
    public static void main(String[] args) throws Exception {

        Properties p = new Properties();
        try (InputStream is = PoolSimApp.class
                .getClassLoader()
                .getResourceAsStream("config.properties")) {
            p.load(is);
        }

        Config cfg = new Config(p);

        Scanner sc = new Scanner(System.in);

        System.out.println("Seleccione modo:");
        System.out.println("1 - RAW");
        System.out.println("2 - POOLED");
        System.out.println("3 - Ambos");
        System.out.print("Opción: ");

        int opcion = sc.nextInt();

        try (SimpleLogger logger = new SimpleLogger(cfg.logFile)) {

            if (opcion == 1 || opcion == 3) {
                ejecutarRaw(cfg, logger);
            }

            if (opcion == 2 || opcion == 3) {
                ejecutarPooled(cfg, logger);
            }
        }
    }

    static void ejecutarRaw(Config cfg, SimpleLogger logger) throws InterruptedException {

        Metrics metrics = new Metrics();
        ExecutorService exec = Executors.newFixedThreadPool(cfg.samples);
        CountDownLatch ready = new CountDownLatch(cfg.samples);
        CountDownLatch start = new CountDownLatch(1);

        long t0 = System.nanoTime();

        for (int i = 1; i <= cfg.samples; i++) {
            exec.submit(new WorkerRaw(i, cfg, ready, start, logger, metrics));
        }

        ready.await();
        start.countDown();

        exec.shutdown();
        exec.awaitTermination(2, TimeUnit.MINUTES);

        long total = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);

        System.out.println("\n===== RAW =====");
        System.out.println("Tiempo total: " + total + " ms");
        System.out.println("Exitosas: " + metrics.ok.sum());
        System.out.println("Fallidas: " + metrics.fail.sum());
    }

    static void ejecutarPooled(Config cfg, SimpleLogger logger) throws InterruptedException {

        Metrics metrics = new Metrics();
        HikariDataSource ds = createPool(cfg);

        ExecutorService exec = Executors.newFixedThreadPool(cfg.samples);
        CountDownLatch ready = new CountDownLatch(cfg.samples);
        CountDownLatch start = new CountDownLatch(1);

        long t0 = System.nanoTime();

        for (int i = 1; i <= cfg.samples; i++) {
            exec.submit(new WorkerPooled(i, cfg, ready, start, logger, metrics, ds));
        }

        ready.await();
        start.countDown();

        exec.shutdown();
        exec.awaitTermination(2, TimeUnit.MINUTES);

        long total = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);

        System.out.println("\n===== POOLED =====");
        System.out.println("Tiempo total: " + total + " ms");
        System.out.println("Exitosas: " + metrics.ok.sum());
        System.out.println("Fallidas: " + metrics.fail.sum());

        ds.close();
    }
}