package dev.relay.api;

import dev.relay.api.dto.ClaimJobsRequest;
import dev.relay.api.dto.ClaimedJobResponse;
import dev.relay.api.dto.QueueStatsResponse;
import dev.relay.core.model.Job;
import dev.relay.store.JobStore;
import dev.relay.store.QueueStats;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.util.List;

@RestController
@RequestMapping("/v1/queues/{queue}")
public class QueueController {

    private final JobStore jobStore;
    private final ObjectMapper objectMapper;

    public QueueController(JobStore jobStore, ObjectMapper objectMapper) {
        this.jobStore = jobStore;
        this.objectMapper = objectMapper;
    }

    @PostMapping("/claim")
    public ResponseEntity<List<ClaimedJobResponse>> claim(
            @PathVariable String queue,
            @Valid @RequestBody ClaimJobsRequest request
    ) {
        int limit = (request.max_jobs() != null && request.max_jobs() > 0) ? request.max_jobs() : 10;
        List<Job> claimedJobs = jobStore.claim(queue, request.worker_id(), limit, request.lease_seconds());

        List<ClaimedJobResponse> response = claimedJobs.stream()
                .map(job -> new ClaimedJobResponse(
                        job.id(),
                        parsePayload(job.payload()),
                        job.attempts(),
                        job.fencingToken(),
                        job.leaseExpiresAt()
                ))
                .toList();

        return ResponseEntity.ok(response);
    }

    @GetMapping("/stats")
    public ResponseEntity<QueueStatsResponse> getStats(@PathVariable String queue) {
        QueueStats stats = jobStore.getQueueStats(queue);
        return ResponseEntity.ok(QueueStatsResponse.from(stats));
    }

    private Object parsePayload(String rawJson) {
        try {
            return objectMapper.readTree(rawJson);
        } catch (JacksonException e) {
            return rawJson;
        }
    }
}
