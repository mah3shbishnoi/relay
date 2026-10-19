package dev.relay.core.model;

import java.time.Instant;
import java.util.UUID;

public record Job(
        UUID id,
        String queue,
        String payload,
        String payloadHash,
        String idempotencyKey,
        JobState state,
        int priority,
        Instant runAt,
        int attempts,
        int maxAttempts,
        String leaseOwner,
        Instant leaseExpiresAt,
        long fencingToken,
        String lastError,
        Instant createdAt,
        Instant updatedAt,
        Instant finishedAt
) {}
