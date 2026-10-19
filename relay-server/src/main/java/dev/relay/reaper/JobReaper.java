package dev.relay.reaper;

import dev.relay.config.RelayProperties;
import dev.relay.store.JobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

import dev.relay.metrics.RelayMetrics;
import java.time.Duration;
import java.time.Instant;

@Component
public class JobReaper {

    private static final Logger log = LoggerFactory.getLogger(JobReaper.class);

    private final JobStore jobStore;
    private final RelayProperties relayProperties;
    private final RelayMetrics metrics;

    public JobReaper(JobStore jobStore, RelayProperties relayProperties, RelayMetrics metrics) {
        this.jobStore = jobStore;
        this.relayProperties = relayProperties;
        this.metrics = metrics;
    }

    @Scheduled(fixedDelayString = "${relay.reaper-interval-seconds:5}", timeUnit = TimeUnit.SECONDS)
    public int reap() {
        Instant start = Instant.now();
        int reaped = 0;
        try {
            reaped = jobStore.reapExpiredLeases(relayProperties.getReaperBatchSize());
            if (reaped > 0) {
                log.info("Reaper recovered {} expired job lease(s)", reaped);
            }
        } catch (Exception e) {
            log.error("Error during reaper execution", e);
        } finally {
            metrics.recordReaperPassDuration(Duration.between(start, Instant.now()));
        }
        return reaped;
    }
}
