package dev.relay.api.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import tools.jackson.databind.JsonNode;

public record SubmitJobRequest(
        @NotBlank(message = "Queue name is required")
        @Size(min = 1, max = 100, message = "Queue name must be between 1 and 100 characters")
        String queue,

        @NotNull(message = "Payload is required")
        JsonNode payload,

        Integer priority,
        Integer delay_seconds,
        Integer max_attempts
) {
    public int resolvedPriority() {
        return priority != null ? priority : 0;
    }
}
