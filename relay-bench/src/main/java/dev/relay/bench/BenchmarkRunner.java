package dev.relay.bench;

import dev.relay.worker.ClaimedJob;
import dev.relay.worker.RelayClient;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class BenchmarkRunner {

    private final String baseUrl;
    private final RelayClient client;

    public BenchmarkRunner(String baseUrl) {
        this.baseUrl = baseUrl;
        this.client = new RelayClient(baseUrl);
    }

    public record BenchmarkResult(
            String experiment,
            int workers,
            int batchSize,
            int totalJobs,
            double elapsedSeconds,
            double throughputJobsPerSec,
            long p50Ms,
            long p95Ms,
            long p99Ms
    ) {}

    public BenchmarkResult runThroughputBenchmark(int workerCount, int batchSize, int jobCount) throws Exception {
        String queue = "bench-w" + workerCount + "-b" + batchSize + "-" + UUID.randomUUID().toString().substring(0, 8);

        // Pre-populate queue
        for (int i = 0; i < jobCount; i++) {
            client.submit(queue, "{\"index\": " + i + "}", null, 0, null);
        }

        ExecutorService executor = Executors.newFixedThreadPool(workerCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(workerCount);
        AtomicInteger completedCount = new AtomicInteger(0);
        List<Long> latencies = new CopyOnWriteArrayList<>();

        Instant start = Instant.now();

        for (int w = 0; w < workerCount; w++) {
            final String workerId = "bench-worker-" + w;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    while (completedCount.get() < jobCount) {
                        long t0 = System.nanoTime();
                        List<ClaimedJob> claimed = client.claim(queue, workerId, batchSize, null);
                        if (claimed.isEmpty()) {
                            if (completedCount.get() >= jobCount) break;
                            Thread.sleep(10);
                            continue;
                        }

                        for (ClaimedJob job : claimed) {
                            boolean ok = client.complete(job.id(), workerId, job.fencing_token());
                            if (ok) {
                                long latencyMs = (System.nanoTime() - t0) / 1_000_000;
                                latencies.add(latencyMs);
                                completedCount.incrementAndGet();
                            }
                        }
                    }
                } catch (Exception e) {
                    // ignore
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        doneLatch.await(60, TimeUnit.SECONDS);
        Duration elapsed = Duration.between(start, Instant.now());
        executor.shutdown();

        Collections.sort(latencies);
        long p50 = latencies.isEmpty() ? 0 : latencies.get((int) (latencies.size() * 0.50));
        long p95 = latencies.isEmpty() ? 0 : latencies.get((int) (latencies.size() * 0.95));
        long p99 = latencies.isEmpty() ? 0 : latencies.get((int) (latencies.size() * 0.99));

        double seconds = elapsed.toMillis() / 1000.0;
        double throughput = seconds > 0 ? (completedCount.get() / seconds) : 0;

        return new BenchmarkResult(
                "Throughput & Latency",
                workerCount,
                batchSize,
                completedCount.get(),
                seconds,
                throughput,
                p50,
                p95,
                p99
        );
    }

    public double measureRecoveryTime(int leaseSeconds) throws Exception {
        String queue = "bench-recovery-" + UUID.randomUUID().toString().substring(0, 8);
        UUID jobId = client.submit(queue, "{\"work\": \"failover\"}", null, 0, null);

        // Worker 1 claims with short lease and crashes
        List<ClaimedJob> claimed = client.claim(queue, "worker-dying", 1, leaseSeconds);
        if (claimed.isEmpty()) {
            throw new IllegalStateException("Failed to claim initial job for recovery test");
        }
        Instant leaseExpiredAt = claimed.get(0).lease_expires_at();

        // Standby worker waits and measures time to claim after lease expiry
        Instant pollStart = Instant.now();
        while (Duration.between(pollStart, Instant.now()).getSeconds() < 30) {
            List<ClaimedJob> recovered = client.claim(queue, "worker-standby", 1, null);
            if (!recovered.isEmpty()) {
                Instant recoveredAt = Instant.now();
                client.complete(jobId, "worker-standby", recovered.get(0).fencing_token());
                long diffMs = Math.max(0, Duration.between(leaseExpiredAt, recoveredAt).toMillis());
                return diffMs / 1000.0;
            }
            Thread.sleep(200);
        }
        return -1.0;
    }

    public static void main(String[] args) throws Exception {
        String baseUrl = args.length > 0 ? args[0] : "http://localhost:8080";
        BenchmarkRunner runner = new BenchmarkRunner(baseUrl);

        System.out.println("=================================================");
        System.out.println("Relay Benchmark Suite (B1, B2, B5)");
        System.out.println("Target: " + baseUrl);
        System.out.println("=================================================");

        // B1: Throughput vs Workers (batch size = 5, 100 jobs)
        System.out.println("\n--- B1: Throughput vs Workers (batch = 5, jobs = 100) ---");
        System.out.printf("%-10s %-12s %-14s %-10s %-10s %-10s%n", "Workers", "Time (s)", "Throughput (j/s)", "p50 (ms)", "p95 (ms)", "p99 (ms)");
        int[] workerCounts = {1, 2, 4, 8, 16};
        for (int w : workerCounts) {
            BenchmarkResult r = runner.runThroughputBenchmark(w, 5, 100);
            System.out.printf("%-10d %-12.2f %-14.1f %-10d %-10d %-10d%n",
                    r.workers(), r.elapsedSeconds(), r.throughputJobsPerSec(), r.p50Ms(), r.p95Ms(), r.p99Ms());
        }

        // B2: Batch Size Effect (workers = 4, 100 jobs)
        System.out.println("\n--- B2: Effect of Claim Batch Size (workers = 4, jobs = 100) ---");
        System.out.printf("%-10s %-12s %-14s %-10s %-10s %-10s%n", "Batch Size", "Time (s)", "Throughput (j/s)", "p50 (ms)", "p95 (ms)", "p99 (ms)");
        int[] batchSizes = {1, 5, 10, 50};
        for (int b : batchSizes) {
            BenchmarkResult r = runner.runThroughputBenchmark(4, b, 100);
            System.out.printf("%-10d %-12.2f %-14.1f %-10d %-10d %-10d%n",
                    r.batchSize(), r.elapsedSeconds(), r.throughputJobsPerSec(), r.p50Ms(), r.p95Ms(), r.p99Ms());
        }

        // B5: Recovery Time
        System.out.println("\n--- B5: Lease Expiration Recovery Time ---");
        double recoverySec = runner.measureRecoveryTime(2);
        System.out.printf("Time from lease expiry to claim by standby worker: %.2f seconds%n", recoverySec);
    }
}
