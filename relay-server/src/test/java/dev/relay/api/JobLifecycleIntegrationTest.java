package dev.relay.api;

import dev.relay.AbstractIntegrationTest;
import dev.relay.core.model.JobState;
import dev.relay.reaper.JobReaper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class JobLifecycleIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private WebApplicationContext context;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private JobReaper jobReaper;

    @Autowired
    private dev.relay.reaper.JobRetentionCleaner jobRetentionCleaner;

    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        this.mockMvc = MockMvcBuilders.webAppContextSetup(context).build();
    }

    @Test
    @DisplayName("Complete happy path: Submit -> Claim -> Heartbeat -> Complete (204)")
    void lifecycle_happyPath() throws Exception {
        String queue = "lifecycle-" + UUID.randomUUID();

        // 1. Submit
        String submitRes = mockMvc.perform(post("/v1/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                    "queue": "%s",
                                    "payload": {"step": "test"}
                                }
                                """.formatted(queue)))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        String jobId = com.jayway.jsonpath.JsonPath.read(submitRes, "$.id");

        // 2. Claim
        String claimRes = mockMvc.perform(post("/v1/queues/" + queue + "/claim")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                    "worker_id": "worker-42",
                                    "max_jobs": 1
                                }
                                """))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        long token = ((Number) com.jayway.jsonpath.JsonPath.read(claimRes, "$[0].fencing_token")).longValue();

        // 3. Heartbeat
        mockMvc.perform(post("/v1/jobs/" + jobId + "/heartbeat")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                    "worker_id": "worker-42",
                                    "fencing_token": %d
                                }
                                """.formatted(token)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.lease_expires_at").isNotEmpty());

        // 4. Complete
        mockMvc.perform(post("/v1/jobs/" + jobId + "/complete")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                    "worker_id": "worker-42",
                                    "fencing_token": %d
                                }
                                """.formatted(token)))
                .andExpect(status().isNoContent());

        // Verify in DB that state is SUCCEEDED
        String state = jdbcClient.sql("SELECT state FROM jobs WHERE id = :id")
                .param("id", UUID.fromString(jobId))
                .query(String.class)
                .single();
        assertThat(state).isEqualTo("SUCCEEDED");
    }

    @Test
    @DisplayName("Fencing token check: Stale worker with token 1 cannot complete job when token is 2 (Guarantee #5)")
    void lifecycle_fencingTokenRejectsStaleWorker() throws Exception {
        String queue = "fencing-" + UUID.randomUUID();

        // Submit
        String submitRes = mockMvc.perform(post("/v1/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                    "queue": "%s",
                                    "payload": {"task": "fencing"}
                                }
                                """.formatted(queue)))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        String jobId = com.jayway.jsonpath.JsonPath.read(submitRes, "$.id");

        // Claim by worker-1 (gets token 1)
        mockMvc.perform(post("/v1/queues/" + queue + "/claim")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                    "worker_id": "worker-1",
                                    "max_jobs": 1
                                }
                                """))
                .andExpect(status().isOk());

        // Simulate lease expiry and re-claim by worker-2
        jdbcClient.sql("UPDATE jobs SET state = 'SCHEDULED', lease_owner = NULL, lease_expires_at = NULL WHERE id = :id")
                .param("id", UUID.fromString(jobId))
                .update();

        // Claim by worker-2 (gets token 2)
        mockMvc.perform(post("/v1/queues/" + queue + "/claim")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                    "worker_id": "worker-2",
                                    "max_jobs": 1
                                }
                                """))
                .andExpect(status().isOk());

        // Now stale worker-1 attempts to complete with token 1 -> MUST BE REJECTED with 409!
        mockMvc.perform(post("/v1/jobs/" + jobId + "/complete")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                    "worker_id": "worker-1",
                                    "fencing_token": 1
                                }
                                """))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.status", is(409)));

        // Verify REJECTED_STALE event was logged
        Integer staleEvents = jdbcClient.sql("SELECT count(*) FROM job_events WHERE job_id = :id AND event_type = 'REJECTED_STALE'")
                .param("id", UUID.fromString(jobId))
                .query(Integer.class)
                .single();
        assertThat(staleEvents).isEqualTo(1);
    }

    @Test
    @DisplayName("Fail max_attempts times moves job to DEAD (Guarantee #6)")
    void lifecycle_failMaxAttemptsMovesToDead() throws Exception {
        String queue = "dead-queue-" + UUID.randomUUID();

        // Submit with max_attempts = 1
        String submitRes = mockMvc.perform(post("/v1/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                    "queue": "%s",
                                    "payload": {"data": 123},
                                    "max_attempts": 1
                                }
                                """.formatted(queue)))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        String jobId = com.jayway.jsonpath.JsonPath.read(submitRes, "$.id");

        // Claim (attempt = 1)
        mockMvc.perform(post("/v1/queues/" + queue + "/claim")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                    "worker_id": "worker-1",
                                    "max_jobs": 1
                                }
                                """))
                .andExpect(status().isOk());

        // Fail
        mockMvc.perform(post("/v1/jobs/" + jobId + "/fail")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                    "worker_id": "worker-1",
                                    "fencing_token": 1,
                                    "error": "Out of memory",
                                    "retryable": true
                                }
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.state", is("DEAD")));

        // Requeue DEAD job back to SCHEDULED
        mockMvc.perform(post("/v1/jobs/" + jobId + "/requeue"))
                .andExpect(status().isOk());

        String state = jdbcClient.sql("SELECT state FROM jobs WHERE id = :id")
                .param("id", UUID.fromString(jobId))
                .query(String.class)
                .single();
        assertThat(state).isEqualTo("SCHEDULED");
    }

    @Test
    @DisplayName("Reaper recovers expired leases and returns them to SCHEDULED")
    void lifecycle_reaperRecoversExpiredLease() throws Exception {
        String queue = "reaper-queue-" + UUID.randomUUID();

        String submitRes = mockMvc.perform(post("/v1/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                    "queue": "%s",
                                    "payload": {"work": "abc"}
                                }
                                """.formatted(queue)))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        String jobId = com.jayway.jsonpath.JsonPath.read(submitRes, "$.id");

        // Claim
        mockMvc.perform(post("/v1/queues/" + queue + "/claim")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                    "worker_id": "worker-dead",
                                    "max_jobs": 1
                                }
                                """))
                .andExpect(status().isOk());

        // Fast-forward lease_expires_at into the past
        jdbcClient.sql("UPDATE jobs SET lease_expires_at = now() - interval '10 seconds' WHERE id = :id")
                .param("id", UUID.fromString(jobId))
                .update();

        // Run reaper manually
        jobReaper.reap();

        // Verify state is back to SCHEDULED
        String state = jdbcClient.sql("SELECT state FROM jobs WHERE id = :id")
                .param("id", UUID.fromString(jobId))
                .query(String.class)
                .single();
        assertThat(state).isEqualTo("SCHEDULED");
    }

    @Test
    @DisplayName("Stale worker after reaper recovery is rejected with 409 and logs REJECTED_STALE")
    void lifecycle_staleWorkerAfterReaperRecovery() throws Exception {
        String queue = "stale-reaper-" + UUID.randomUUID();

        String submitRes = mockMvc.perform(post("/v1/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                    "queue": "%s",
                                    "payload": {"work": "fencing-reap"}
                                }
                                """.formatted(queue)))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        String jobId = com.jayway.jsonpath.JsonPath.read(submitRes, "$.id");

        // Worker 1 claims
        String claim1 = mockMvc.perform(post("/v1/queues/" + queue + "/claim")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                    "worker_id": "worker-1",
                                    "max_jobs": 1
                                }
                                """))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        long token1 = ((Number) com.jayway.jsonpath.JsonPath.read(claim1, "$[0].fencing_token")).longValue();
        assertThat(token1).isEqualTo(1L);

        // Fast-forward lease into past and run reaper
        jdbcClient.sql("UPDATE jobs SET lease_expires_at = now() - interval '10 seconds' WHERE id = :id")
                .param("id", UUID.fromString(jobId))
                .update();
        jobReaper.reap();

        // Advance run_at so worker-2 does not have to wait for exponential backoff
        jdbcClient.sql("UPDATE jobs SET run_at = now() WHERE id = :id")
                .param("id", UUID.fromString(jobId))
                .update();

        // Worker 2 re-claims
        String claim2 = mockMvc.perform(post("/v1/queues/" + queue + "/claim")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                    "worker_id": "worker-2",
                                    "max_jobs": 1
                                }
                                """))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        long token2 = ((Number) com.jayway.jsonpath.JsonPath.read(claim2, "$[0].fencing_token")).longValue();
        assertThat(token2).isEqualTo(2L);

        // Stale Worker 1 attempts complete
        mockMvc.perform(post("/v1/jobs/" + jobId + "/complete")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                    "worker_id": "worker-1",
                                    "fencing_token": %d
                                }
                                """.formatted(token1)))
                .andExpect(status().isConflict());

        // Valid Worker 2 completes
        mockMvc.perform(post("/v1/jobs/" + jobId + "/complete")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                    "worker_id": "worker-2",
                                    "fencing_token": %d
                                }
                                """.formatted(token2)))
                .andExpect(status().isNoContent());

        String finalState = jdbcClient.sql("SELECT state FROM jobs WHERE id = :id")
                .param("id", UUID.fromString(jobId))
                .query(String.class)
                .single();
        assertThat(finalState).isEqualTo("SUCCEEDED");

        Integer rejectedStaleEvents = jdbcClient.sql("SELECT count(*) FROM job_events WHERE job_id = :id AND event_type = 'REJECTED_STALE'")
                .param("id", UUID.fromString(jobId))
                .query(Integer.class)
                .single();
        assertThat(rejectedStaleEvents).isEqualTo(1);
    }

    @Test
    @DisplayName("Retention cleanup removes finished jobs older than retention threshold")
    void lifecycle_retentionCleanupRemovesOldFinishedJobs() throws Exception {
        String queue = "retention-" + UUID.randomUUID();

        // Submit and complete job 1 (old)
        String res1 = mockMvc.perform(post("/v1/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                    "queue": "%s",
                                    "payload": {"id": 1}
                                }
                                """.formatted(queue)))
                .andReturn().getResponse().getContentAsString();
        String id1 = com.jayway.jsonpath.JsonPath.read(res1, "$.id");

        mockMvc.perform(post("/v1/queues/" + queue + "/claim")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"worker_id": "w1", "max_jobs": 1}
                                """));
        mockMvc.perform(post("/v1/jobs/" + id1 + "/complete")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"worker_id": "w1", "fencing_token": 1}
                                """));

        // Submit and complete job 2 (recent)
        String res2 = mockMvc.perform(post("/v1/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                    "queue": "%s",
                                    "payload": {"id": 2}
                                }
                                """.formatted(queue)))
                .andReturn().getResponse().getContentAsString();
        String id2 = com.jayway.jsonpath.JsonPath.read(res2, "$.id");

        mockMvc.perform(post("/v1/queues/" + queue + "/claim")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"worker_id": "w1", "max_jobs": 1}
                                """));
        mockMvc.perform(post("/v1/jobs/" + id2 + "/complete")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"worker_id": "w1", "fencing_token": 1}
                                """));

        // Age job 1 finished_at to 10 days ago
        jdbcClient.sql("UPDATE jobs SET finished_at = now() - interval '10 days' WHERE id = :id")
                .param("id", UUID.fromString(id1))
                .update();

        // Run retention cleaner
        int deleted = jobRetentionCleaner.clean();
        assertThat(deleted).isGreaterThanOrEqualTo(1);

        // Job 1 must be gone
        Integer job1Count = jdbcClient.sql("SELECT count(*) FROM jobs WHERE id = :id")
                .param("id", UUID.fromString(id1))
                .query(Integer.class)
                .single();
        assertThat(job1Count).isEqualTo(0);

        // Job 2 must still exist
        Integer job2Count = jdbcClient.sql("SELECT count(*) FROM jobs WHERE id = :id")
                .param("id", UUID.fromString(id2))
                .query(Integer.class)
                .single();
        assertThat(job2Count).isEqualTo(1);
    }
}

