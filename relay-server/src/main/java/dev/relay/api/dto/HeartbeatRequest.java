package dev.relay.api.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record HeartbeatRequest(
        @NotBlank(message = "worker_id is required")
        String worker_id,

        @NotNull(message = "fencing_token is required")
        Long fencing_token,

        Integer lease_seconds
) {}
