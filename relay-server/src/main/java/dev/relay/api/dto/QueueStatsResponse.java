package dev.relay.api.dto;

import dev.relay.store.QueueStats;

public record QueueStatsResponse(
        long scheduled_count,
        long running_count,
        long succeeded_count,
        long dead_count,
        double oldest_scheduled_age_seconds
) {
    public static QueueStatsResponse from(QueueStats stats) {
        return new QueueStatsResponse(
                stats.scheduledCount(),
                stats.runningCount(),
                stats.succeededCount(),
                stats.deadCount(),
                stats.oldestScheduledAgeSeconds()
        );
    }
}
