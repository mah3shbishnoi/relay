package dev.relay.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class RelayMetrics {

    private final MeterRegistry registry;

    public RelayMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    public void recordJobSubmitted(String queue) {
        Counter.builder("relay_jobs_submitted_total")
                .tag("queue", queue)
                .register(registry)
                .increment();
    }

    public void recordJobClaimed(String queue) {
        Counter.builder("relay_jobs_claimed_total")
                .tag("queue", queue)
                .register(registry)
                .increment();
    }

    public void recordJobCompleted(String queue) {
        Counter.builder("relay_jobs_completed_total")
                .tag("queue", queue)
                .register(registry)
                .increment();
    }

    public void recordJobFailed(String queue, boolean retryable) {
        Counter.builder("relay_jobs_failed_total")
                .tag("queue", queue)
                .tag("retryable", String.valueOf(retryable))
                .register(registry)
                .increment();
    }

    public void recordJobDead(String queue) {
        Counter.builder("relay_jobs_dead_total")
                .tag("queue", queue)
                .register(registry)
                .increment();
    }

    public void recordLeaseExpired(String queue) {
        Counter.builder("relay_lease_expired_total")
                .tag("queue", queue)
                .register(registry)
                .increment();
    }

    public void recordStaleRejection() {
        Counter.builder("relay_stale_rejections_total")
                .register(registry)
                .increment();
    }

    public void recordClaimDuration(Duration duration) {
        Timer.builder("relay_claim_duration_seconds")
                .register(registry)
                .record(duration);
    }

    public void recordReaperPassDuration(Duration duration) {
        Timer.builder("relay_reaper_pass_seconds")
                .register(registry)
                .record(duration);
    }
}
