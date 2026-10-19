package dev.relay.store;

public record QueueStats(
        long scheduledCount,
        long runningCount,
        long succeededCount,
        long deadCount,
        double oldestScheduledAgeSeconds
) {}
