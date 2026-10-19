package dev.relay.worker;

import java.time.Instant;
import java.util.UUID;

public record ClaimedJob(
        UUID id,
        String payload,
        int attempt,
        long fencing_token,
        Instant lease_expires_at
) {}
