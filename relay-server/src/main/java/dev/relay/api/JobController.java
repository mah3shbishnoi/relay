package dev.relay.api;

import dev.relay.api.dto.*;
import dev.relay.api.exception.IdempotencyConflictException;
import dev.relay.api.exception.JobNotFoundException;
import dev.relay.api.exception.PayloadTooLargeException;
import dev.relay.config.RelayProperties;
import dev.relay.core.model.Job;
import dev.relay.core.util.PayloadHasher;
import dev.relay.store.FailResult;
import dev.relay.store.JobStore;
import dev.relay.store.SubmitResult;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

@RestController
@RequestMapping("/v1/jobs")
public class JobController {

    private final JobStore jobStore;
    private final RelayProperties relayProperties;
    private final ObjectMapper objectMapper;

    public JobController(JobStore jobStore, RelayProperties relayProperties, ObjectMapper objectMapper) {
        this.jobStore = jobStore;
        this.relayProperties = relayProperties;
        this.objectMapper = objectMapper;
    }

    @PostMapping
    public ResponseEntity<JobResponse> submit(
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @Valid @RequestBody SubmitJobRequest request
    ) {
        String payloadJson;
        try {
            payloadJson = objectMapper.writeValueAsString(request.payload());
        } catch (JacksonException e) {
            throw new IllegalArgumentException("Invalid payload JSON: " + e.getMessage());
        }

        byte[] payloadBytes = payloadJson.getBytes(StandardCharsets.UTF_8);
        if (payloadBytes.length > relayProperties.getMaxPayloadBytes()) {
            throw new PayloadTooLargeException(
                    "Payload size (" + payloadBytes.length + " bytes) exceeds maximum allowed size ("
                            + relayProperties.getMaxPayloadBytes() + " bytes)"
            );
        }

        String payloadHash = PayloadHasher.sha256Hex(payloadJson);

        SubmitResult result = jobStore.submit(
                request.queue(),
                payloadJson,
                payloadHash,
                idempotencyKey,
                request.resolvedPriority(),
                request.delay_seconds(),
                request.max_attempts()
        );

        return switch (result) {
            case SubmitResult.Created(Job job) ->
                    ResponseEntity.status(HttpStatus.CREATED)
                            .body(JobResponse.from(job, request.payload()));
            case SubmitResult.Duplicate(Job job) ->
                    ResponseEntity.ok(JobResponse.from(job, parsePayload(job.payload())));
            case SubmitResult.Conflict(Job existing) ->
                    throw new IdempotencyConflictException(
                            "Idempotency key '" + idempotencyKey + "' was already used with a different payload"
                    );
        };
    }

    @GetMapping("/{id}")
    public ResponseEntity<JobResponse> getJob(@PathVariable UUID id) {
        Job job = jobStore.findById(id)
                .orElseThrow(() -> new JobNotFoundException("Job " + id + " not found"));

        return ResponseEntity.ok(JobResponse.from(job, parsePayload(job.payload())));
    }

    @PostMapping("/{id}/heartbeat")
    public ResponseEntity<HeartbeatResponse> heartbeat(
            @PathVariable UUID id,
            @Valid @RequestBody HeartbeatRequest request
    ) {
        Instant newExpiry = jobStore.heartbeat(id, request.worker_id(), request.fencing_token(), request.lease_seconds());
        return ResponseEntity.ok(new HeartbeatResponse(newExpiry));
    }

    @PostMapping("/{id}/complete")
    public ResponseEntity<Void> complete(
            @PathVariable UUID id,
            @Valid @RequestBody CompleteJobRequest request
    ) {
        jobStore.complete(id, request.worker_id(), request.fencing_token());
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/{id}/fail")
    public ResponseEntity<FailJobResponse> fail(
            @PathVariable UUID id,
            @Valid @RequestBody FailJobRequest request
    ) {
        FailResult result = jobStore.fail(
                id,
                request.worker_id(),
                request.fencing_token(),
                request.error(),
                request.isRetryable()
        );
        return ResponseEntity.ok(new FailJobResponse(result.state(), result.runAt()));
    }

    @PostMapping("/{id}/requeue")
    public ResponseEntity<Void> requeue(@PathVariable UUID id) {
        boolean success = jobStore.requeue(id);
        if (!success) {
            throw new JobNotFoundException("Job " + id + " not found or not in DEAD state");
        }
        return ResponseEntity.ok().build();
    }

    private JsonNode parsePayload(String rawJson) {
        try {
            return objectMapper.readTree(rawJson);
        } catch (JacksonException e) {
            return null;
        }
    }
}
