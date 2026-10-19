package dev.relay.api.dto;

import java.time.Instant;
import java.util.UUID;

public record ClaimedJobResponse(
        UUID id,
        Object payload,
        int attempt,
        long fencing_token,
        Instant lease_expires_at
) {}
