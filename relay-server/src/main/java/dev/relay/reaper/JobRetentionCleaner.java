package dev.relay.reaper;

import dev.relay.config.RelayProperties;
import dev.relay.store.JobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class JobRetentionCleaner {

    private static final Logger log = LoggerFactory.getLogger(JobRetentionCleaner.class);

    private final JobStore jobStore;
    private final RelayProperties relayProperties;

    public JobRetentionCleaner(JobStore jobStore, RelayProperties relayProperties) {
        this.jobStore = jobStore;
        this.relayProperties = relayProperties;
    }

    @Scheduled(fixedDelayString = "${relay.retention-interval-ms:3600000}")
    public int clean() {
        if (relayProperties.getRetentionDays() <= 0) {
            return 0;
        }
        Duration retention = Duration.ofDays(relayProperties.getRetentionDays());
        int deleted = jobStore.deleteSucceededOlderThan(retention, relayProperties.getRetentionBatchSize());
        if (deleted > 0) {
            log.info("Retention cleaner deleted {} old succeeded jobs", deleted);
        }
        return deleted;
    }
}
