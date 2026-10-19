package dev.relay.api.dto;

import dev.relay.core.model.Job;
import dev.relay.core.model.JobState;

import java.time.Instant;
import java.util.UUID;

public record JobResponse(
        UUID id,
        String queue,
        Object payload,
        JobState state,
        int priority,
        Instant run_at,
        int attempts,
        int max_attempts,
        Instant created_at,
        Instant updated_at,
        Instant finished_at,
        String last_error
) {
    public static JobResponse from(Job job, Object parsedPayload) {
        return new JobResponse(
                job.id(),
                job.queue(),
                parsedPayload,
                job.state(),
                job.priority(),
                job.runAt(),
                job.attempts(),
                job.maxAttempts(),
                job.createdAt(),
                job.updatedAt(),
                job.finishedAt(),
                job.lastError()
        );
    }
}
