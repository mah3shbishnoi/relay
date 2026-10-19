package dev.relay.store;

import dev.relay.core.model.JobState;

import java.time.Instant;

public record FailResult(
        JobState state,
        Instant runAt
) {}
