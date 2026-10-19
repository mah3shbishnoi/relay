package dev.relay.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class RelayWorker {

    private static final Logger log = LoggerFactory.getLogger(RelayWorker.class);

    private final RelayClient client;
    private final String queue;
    private final String workerId;
    private final JobHandler handler;
    private final int concurrency;
    private final int leaseSeconds;
    private final int heartbeatIntervalSeconds;
    private final long pollIntervalMs;

    private final ExecutorService jobExecutor;
    private final ScheduledExecutorService heartbeatExecutor;
    private final ExecutorService pollExecutor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ConcurrentHashMap<UUID, Future<?>> inFlight = new ConcurrentHashMap<>();

    public RelayWorker(
            RelayClient client,
            String queue,
            String workerId,
            JobHandler handler,
            int concurrency,
            int leaseSeconds,
            int heartbeatIntervalSeconds,
            long pollIntervalMs
    ) {
        this.client = client;
        this.queue = queue;
        this.workerId = workerId;
        this.handler = handler;
        this.concurrency = concurrency;
        this.leaseSeconds = leaseSeconds;
        this.heartbeatIntervalSeconds = heartbeatIntervalSeconds;
        this.pollIntervalMs = pollIntervalMs;

        this.jobExecutor = Executors.newFixedThreadPool(concurrency);
        this.heartbeatExecutor = Executors.newScheduledThreadPool(Math.max(1, concurrency / 2));
        this.pollExecutor = Executors.newSingleThreadExecutor();
    }

    public synchronized void start() {
        if (running.compareAndSet(false, true)) {
            pollExecutor.submit(this::pollLoop);
            log.info("RelayWorker {} started on queue {}", workerId, queue);
        }
    }

    public synchronized void stop(Duration timeout) {
        if (running.compareAndSet(true, false)) {
            log.info("RelayWorker {} stopping...", workerId);
            pollExecutor.shutdownNow();
            jobExecutor.shutdown();
            heartbeatExecutor.shutdownNow();

            try {
                if (!jobExecutor.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                    jobExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                jobExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            log.info("RelayWorker {} stopped", workerId);
        }
    }

    private void pollLoop() {
        while (running.get()) {
            try {
                int capacity = concurrency - inFlight.size();
                if (capacity > 0) {
                    List<ClaimedJob> claimed = client.claim(queue, workerId, capacity, leaseSeconds);
                    for (ClaimedJob job : claimed) {
                        dispatchJob(job);
                    }
                }
                Thread.sleep(pollIntervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.warn("Error in poll loop for worker {}: {}", workerId, e.getMessage());
                try {
                    Thread.sleep(pollIntervalMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    private void dispatchJob(ClaimedJob job) {
        AtomicBoolean lostLease = new AtomicBoolean(false);

        ScheduledFuture<?> heartbeatTask = heartbeatExecutor.scheduleAtFixedRate(() -> {
            boolean ok = client.heartbeat(job.id(), workerId, job.fencing_token(), leaseSeconds);
            if (!ok) {
                log.warn("Lost lease on job {} during heartbeat, cancelling handler", job.id());
                lostLease.set(true);
                Future<?> task = inFlight.get(job.id());
                if (task != null) {
                    task.cancel(true);
                }
            }
        }, heartbeatIntervalSeconds, heartbeatIntervalSeconds, TimeUnit.SECONDS);

        Future<?> jobTask = jobExecutor.submit(() -> {
            JobContext ctx = new JobContext(job.id(), job.payload(), job.attempt(), job.fencing_token());
            try {
                handler.handle(ctx);
                heartbeatTask.cancel(false);
                if (!lostLease.get()) {
                    client.complete(job.id(), workerId, job.fencing_token());
                }
            } catch (InterruptedException ie) {
                heartbeatTask.cancel(true);
                log.info("Job {} was interrupted", job.id());
            } catch (Exception ex) {
                heartbeatTask.cancel(false);
                if (!lostLease.get()) {
                    client.fail(job.id(), workerId, job.fencing_token(), ex.getMessage(), true);
                }
            } finally {
                inFlight.remove(job.id());
            }
        });

        inFlight.put(job.id(), jobTask);
    }
}
