package com.example;

import java.io.*;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;

public class PoolSimApp {

    public static void main(String[] args) throws Exception {

        Properties p = new Properties();
        try (InputStream in =
                     PoolSimApp.class.getClassLoader()
                             .getResourceAsStream("config.properties")) {
            if (in == null)
                throw new RuntimeException("No se encontró config.properties");
            p.load(in);
        }

        Config cfg = new Config(p);

        try (SimpleLogger logger = new SimpleLogger(cfg.logFile)) {

            System.out.println("=== INICIANDO RAW ===");
            runRaw(cfg, logger);

            System.out.println("\n=== INICIANDO POOLED ===");
            runPooled(cfg, logger);
        }
    }


    static void runRaw(Config cfg, SimpleLogger logger) throws Exception {

        ExecutorService exec = Executors.newFixedThreadPool(cfg.samples);
        CountDownLatch ready = new CountDownLatch(cfg.samples);
        CountDownLatch start = new CountDownLatch(1);
        Metrics metrics = new Metrics();

        for (int i = 0; i < cfg.samples; i++) {
            exec.submit(new WorkerRaw(i, cfg, ready, start, logger, metrics));
        }

        ready.await();
        long t0 = System.currentTimeMillis();
        start.countDown();

        exec.shutdown();
        exec.awaitTermination(10, TimeUnit.MINUTES);

        long total = System.currentTimeMillis() - t0;

        System.out.println("Conexiones exitosas : " + metrics.ok.sum());
        System.out.println("Conexiones fallidas : " + metrics.fail.sum());
        System.out.println("Tiempo total (ms)   : " + total);
        System.out.println("====================================");
    }


    static void runPooled(Config cfg, SimpleLogger logger) throws Exception {

        ExecutorService exec = Executors.newFixedThreadPool(cfg.samples);
        CountDownLatch ready = new CountDownLatch(cfg.samples);
        CountDownLatch start = new CountDownLatch(1);
        Metrics metrics = new Metrics();

        BlockingQueue<Connection> pool =
                new ArrayBlockingQueue<>(cfg.poolMaxSize);

        for (int i = 0; i < cfg.poolMaxSize; i++) {
            pool.offer(DriverManager.getConnection(
                    cfg.url, cfg.user, cfg.password));
        }

        for (int i = 0; i < cfg.samples; i++) {
            exec.submit(new WorkerPooled(
                    i, cfg, pool, ready, start, logger, metrics));
        }

        ready.await();
        long t0 = System.currentTimeMillis();
        start.countDown();

        exec.shutdown();
        exec.awaitTermination(10, TimeUnit.MINUTES);

        long total = System.currentTimeMillis() - t0;

        System.out.println("Conexiones exitosas : " + metrics.ok.sum());
        System.out.println("Conexiones fallidas : " + metrics.fail.sum());
        System.out.println("Tiempo total (ms)   : " + total);
        System.out.println("=======================================");
    }

    static class Config {
        final String url, user, password, query, logFile;
        final int samples, maxRetries;
        final int poolMaxSize;
        final int poolMinIdle;
        final long poolTimeout;

        Config(Properties p) {
            url = req(p, "db.url");
            user = req(p, "db.user");
            password = p.getProperty("db.password", "").trim();
            query = req(p, "db.query");

            samples = Integer.parseInt(p.getProperty("samples", "20").trim());
            maxRetries = Integer.parseInt(p.getProperty("maxRetries", "1").trim());
            logFile = p.getProperty("log.file", "simulator.log").trim();

            poolMaxSize = Integer.parseInt(p.getProperty("pool.maxSize", "10"));
            poolMinIdle = Integer.parseInt(p.getProperty("pool.minIdle", "2"));
            poolTimeout = Long.parseLong(
                    p.getProperty("pool.connectionTimeoutMs", "30000"));
        }

        static String req(Properties p, String k) {
            String v = p.getProperty(k);
            if (v == null || v.isBlank())
                throw new IllegalArgumentException("Falta config: " + k);
            return v.trim();
        }
    }

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

        @Override
        public void close() throws IOException { out.close(); }
    }

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

    static class WorkerRaw implements Runnable {
        private final int id;
        private final Config cfg;
        private final CountDownLatch ready;
        private final CountDownLatch start;
        private final SimpleLogger logger;
        private final Metrics metrics;

        WorkerRaw(int id, Config cfg,
                  CountDownLatch ready, CountDownLatch start,
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
                try (Connection c =
                             DriverManager.getConnection(cfg.url, cfg.user, cfg.password);
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

            long ms = TimeUnit.NANOSECONDS.toMillis(
                    System.nanoTime() - t0);
            metrics.record(success, retriesUsed);

            logger.log("%s - raw - id=%d - %s - reintentos=%d - tiempo=%d ms"
                    .formatted(logger.ts(), id,
                            success ? "exitoso" : "fallido",
                            retriesUsed, ms));
        }
    }

    static class WorkerPooled implements Runnable {
        private final int id;
        private final Config cfg;
        private final BlockingQueue<Connection> pool;
        private final CountDownLatch ready;
        private final CountDownLatch start;
        private final SimpleLogger logger;
        private final Metrics metrics;

        WorkerPooled(int id, Config cfg,
                     BlockingQueue<Connection> pool,
                     CountDownLatch ready,
                     CountDownLatch start,
                     SimpleLogger logger,
                     Metrics metrics) {
            this.id = id;
            this.cfg = cfg;
            this.pool = pool;
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

                Connection c = null;

                try {
                    c = pool.poll(cfg.poolTimeout, TimeUnit.MILLISECONDS);

                    if (c == null)
                        throw new TimeoutException("Timeout pool");

                    try (PreparedStatement ps = c.prepareStatement(cfg.query)) {
                        ps.execute();
                    }

                    success = true;
                    retriesUsed = attempt - 1;
                    pool.offer(c);
                    break;

                } catch (Exception ex) {

                    if (c != null)
                        pool.offer(c);

                    if (attempt == cfg.maxRetries + 1)
                        retriesUsed = attempt - 1;
                }
            }

            long ms = TimeUnit.NANOSECONDS.toMillis(
                    System.nanoTime() - t0);
            metrics.record(success, retriesUsed);

            logger.log("%s - pooled - id=%d - %s - reintentos=%d - tiempo=%d ms"
                    .formatted(logger.ts(), id,
                            success ? "exitoso" : "fallido",
                            retriesUsed, ms));
        }
    }
}