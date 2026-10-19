package dev.relay.worker;

import java.util.UUID;

public record JobContext(
        UUID jobId,
        String payload,
        int attempt,
        long fencingToken
) {}
