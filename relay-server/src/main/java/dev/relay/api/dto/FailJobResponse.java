package dev.relay.api.dto;

import dev.relay.core.model.JobState;

import java.time.Instant;

public record FailJobResponse(
        JobState state,
        Instant run_at
) {}
