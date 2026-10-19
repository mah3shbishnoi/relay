package dev.relay.api.dto;

import jakarta.validation.constraints.NotBlank;

public record ClaimJobsRequest(
        @NotBlank(message = "worker_id is required")
        String worker_id,
        Integer max_jobs,
        Integer lease_seconds
) {}
