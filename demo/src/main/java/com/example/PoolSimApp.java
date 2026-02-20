package com.example;

import java.io.*;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;

public class PoolSimApp {

    static class Config {
        final String url, user, password, query, logFile;
        final int samples, maxRetries;

        Config(Properties p) {
            url = req(p, "db.url");
            user = req(p, "db.user");
            password = req(p, "db.password");
            query = req(p, "db.query");
            samples = Integer.parseInt(p.getProperty("samples", "20").trim());
            maxRetries = Integer.parseInt(p.getProperty("maxRetries", "1").trim());
            logFile = p.getProperty("log.file", "simulator.log").trim();
        }

        static String req(Properties p, String k) {
            String v = p.getProperty(k);
            if (v == null || v.isBlank()) throw new IllegalArgumentException("Falta config: " + k);
            return v.trim();
        }
    }

    static class SimpleLogger implements Closeable {
        private final BufferedWriter out;
        private final DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

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

    static class Metrics {
        final LongAdder ok = new LongAdder();
        final LongAdder fail = new LongAdder();
        final LongAdder retries = new LongAdder();

        void record(boolean success, int retriesUsed) {
            if (success) ok.increment(); else fail.increment();
            retries.add(retriesUsed);
        }
    }

    static class Worker implements Runnable {
        private final int id;
        private final Config cfg;
        private final CountDownLatch ready;
        private final CountDownLatch start;
        private final SimpleLogger logger;
        private final Metrics metrics;

        Worker(int id, Config cfg, CountDownLatch ready, CountDownLatch start,
               SimpleLogger logger, Metrics metrics) {
            this.id = id; this.cfg = cfg; this.ready = ready; this.start = start;
            this.logger = logger; this.metrics = metrics;
        }

        @Override public void run() {
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
                    if (attempt == cfg.maxRetries + 1) retriesUsed = attempt - 1;
                }
            }

            long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
            metrics.record(success, retriesUsed);

            String status = success ? "exitoso" : "fallido";
            logger.log("%s - raw - id=%d - %s - reintentos=%d - tiempo en ms=%d"
                    .formatted(logger.ts(), id, status, retriesUsed, ms));
        }
    }

    public static void main(String[] args) throws Exception {
        Properties p = new Properties();
        try (FileInputStream fis = new FileInputStream("config.properties")) {
            p.load(fis);
        }
        Config cfg = new Config(p);

        Metrics metrics = new Metrics();

        try (SimpleLogger logger = new SimpleLogger(cfg.logFile)) {
            // test de conexión rápido
            try (Connection c = DriverManager.getConnection(cfg.url, cfg.user, cfg.password)) {
                logger.log(logger.ts() + " - SYSTEM - db_ok");
            } catch (SQLException e) {
                logger.log(logger.ts() + " - SYSTEM - db_error - " + e.getMessage());
                System.err.println("No conecta a la db. Revisar config.properties");
                return;
            }

            ExecutorService exec = Executors.newFixedThreadPool(cfg.samples);
            CountDownLatch ready = new CountDownLatch(cfg.samples);
            CountDownLatch start = new CountDownLatch(1);

            long simStart = System.nanoTime();

            for (int i = 1; i <= cfg.samples; i++) {
                exec.submit(new Worker(i, cfg, ready, start, logger, metrics));
            }

            // Arranque concurrente real
            ready.await();
            logger.log(logger.ts() + " - SYSTEM - inicio_concurrente samples=" + cfg.samples);
            start.countDown();

            exec.shutdown();
            exec.awaitTermination(2, TimeUnit.MINUTES);

            long totalMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - simStart);

            long ok = metrics.ok.sum();
            long fail = metrics.fail.sum();
            long n = ok + fail;
            double okPct = n == 0 ? 0 : (100.0 * ok / n);
            double avgRetries = n == 0 ? 0 : (1.0 * metrics.retries.sum() / n);

            String resumen = """
                    RAW
                    Tiempo total: %d ms
                    Muestras: %d
                    Exitosas: %d (%.2f%%)
                    Fallidas: %d
                    Reintentospromedio: %.2f
                    """.formatted(totalMs, n, ok, okPct, fail, avgRetries);

            System.out.println(resumen);
            logger.log(logger.ts() + " | SYSTEM | resumen | " + resumen.replace("\n", " | "));
        }
    }
}