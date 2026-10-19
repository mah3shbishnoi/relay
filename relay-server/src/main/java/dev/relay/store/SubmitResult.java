package dev.relay.store;

import dev.relay.core.model.Job;

public sealed interface SubmitResult {
    record Created(Job job) implements SubmitResult {}
    record Duplicate(Job job) implements SubmitResult {}
    record Conflict(Job existingJob) implements SubmitResult {}
}
