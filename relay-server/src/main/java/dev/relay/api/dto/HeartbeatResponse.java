package dev.relay.api.dto;

import java.time.Instant;

public record HeartbeatResponse(
        Instant lease_expires_at
) {}
