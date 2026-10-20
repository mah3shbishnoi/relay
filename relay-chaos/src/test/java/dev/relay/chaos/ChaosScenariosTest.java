package dev.relay.chaos;

import dev.relay.RelayApplication;
import dev.relay.reaper.JobReaper;
import dev.relay.store.JobStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.testcontainers.containers.PostgreSQLContainer;

import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.is;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(classes = RelayApplication.class)
class ChaosScenariosTest {

    static final PostgreSQLContainer<?> postgres;

    static {
        postgres = new PostgreSQLContainer<>("postgres:17")
                .withDatabaseName("test")
                .withUsername("test")
                .withPassword("test");
        postgres.start();
    }

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @Autowired
    private WebApplicationContext context;

    @Autowired
    private JdbcClient jdbcClient;

    @Autowired
    private JobStore jobStore;

    @Autowired
    private JobReaper jobReaper;

    private MockMvc mockMvc;
    private InvariantChecker invariantChecker;

    @BeforeEach
    void setUp() {
        this.mockMvc = MockMvcBuilders.webAppContextSetup(context).build();
        this.invariantChecker = new InvariantChecker(jdbcClient);
    }

    @Test
    @DisplayName("C1: Worker crash mid-job — lease expires, reaper recovers job, completed by healthy worker")
    void scenario_C1_workerCrashAndRecovery() throws Exception {
        String queue = "c1-crash-" + UUID.randomUUID();

        String res = mockMvc.perform(post("/v1/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"queue": "%s", "payload": {"file": "report.pdf"}}
                                """.formatted(queue)))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        String jobId = com.jayway.jsonpath.JsonPath.read(res, "$.id");

        // Worker 1 claims job but crashes
        mockMvc.perform(post("/v1/queues/" + queue + "/claim")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"worker_id": "worker-crash", "max_jobs": 1}
                                """))
                .andExpect(status().isOk());

        // Fast-forward lease into the past and trigger reaper
        jdbcClient.sql("UPDATE jobs SET lease_expires_at = now() - interval '30 seconds' WHERE id = :id")
                .param("id", UUID.fromString(jobId))
                .update();
        jobReaper.reap();

        // Advance run_at past jitter delay for healthy worker
        jdbcClient.sql("UPDATE jobs SET run_at = now() WHERE id = :id")
                .param("id", UUID.fromString(jobId))
                .update();

        // Healthy worker claims and completes
        String claim2 = mockMvc.perform(post("/v1/queues/" + queue + "/claim")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"worker_id": "worker-healthy", "max_jobs": 1}
                                """))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        long token2 = ((Number) com.jayway.jsonpath.JsonPath.read(claim2, "$[0].fencing_token")).longValue();

        mockMvc.perform(post("/v1/jobs/" + jobId + "/complete")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"worker_id": "worker-healthy", "fencing_token": %d}
                                """.formatted(token2)))
                .andExpect(status().isNoContent());

        InvariantChecker.InvariantReport report = invariantChecker.checkAll();
        assertThat(report.passed()).withFailMessage(String.join("\n", report.violations())).isTrue();
    }

    @Test
    @DisplayName("C2: Paused worker wakes after expiry — stale completion rejected (409) and new worker wins")
    void scenario_C2_workerPausedPastLeaseThenResumes() throws Exception {
        String queue = "c2-pause-" + UUID.randomUUID();

        String res = mockMvc.perform(post("/v1/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"queue": "%s", "payload": {"task": "image-resize"}}
                                """.formatted(queue)))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        String jobId = com.jayway.jsonpath.JsonPath.read(res, "$.id");

        // Worker 1 claims
        String claim1 = mockMvc.perform(post("/v1/queues/" + queue + "/claim")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"worker_id": "worker-paused", "max_jobs": 1}
                                """))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        long token1 = ((Number) com.jayway.jsonpath.JsonPath.read(claim1, "$[0].fencing_token")).longValue();

        // Worker 1 paused (lease expires)
        jdbcClient.sql("UPDATE jobs SET lease_expires_at = now() - interval '10 seconds' WHERE id = :id")
                .param("id", UUID.fromString(jobId))
                .update();
        jobReaper.reap();

        jdbcClient.sql("UPDATE jobs SET run_at = now() WHERE id = :id")
                .param("id", UUID.fromString(jobId))
                .update();

        // Worker 2 claims
        String claim2 = mockMvc.perform(post("/v1/queues/" + queue + "/claim")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"worker_id": "worker-active", "max_jobs": 1}
                                """))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        long token2 = ((Number) com.jayway.jsonpath.JsonPath.read(claim2, "$[0].fencing_token")).longValue();

        // Paused worker wakes up and tries to complete
        mockMvc.perform(post("/v1/jobs/" + jobId + "/complete")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"worker_id": "worker-paused", "fencing_token": %d}
                                """.formatted(token1)))
                .andExpect(status().isConflict());

        // Worker 2 completes
        mockMvc.perform(post("/v1/jobs/" + jobId + "/complete")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"worker_id": "worker-active", "fencing_token": %d}
                                """.formatted(token2)))
                .andExpect(status().isNoContent());

        InvariantChecker.InvariantReport report = invariantChecker.checkAll();
        assertThat(report.passed()).withFailMessage(String.join("\n", report.violations())).isTrue();
    }

    @Test
    @DisplayName("C7: Concurrent reapers — multiple reaper instances recover expired leases without deadlocks")
    void scenario_C7_concurrentReapers() throws Exception {
        String queue = "c7-reapers-" + UUID.randomUUID();
        int jobCount = 20;

        for (int i = 0; i < jobCount; i++) {
            mockMvc.perform(post("/v1/jobs")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("""
                                    {"queue": "%s", "payload": {"item": %d}}
                                    """.formatted(queue, i)))
                    .andExpect(status().isCreated());
        }

        mockMvc.perform(post("/v1/queues/" + queue + "/claim")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"worker_id": "batch-w", "max_jobs": %d}
                                """.formatted(jobCount)))
                .andExpect(status().isOk());

        // Expire all leases
        jdbcClient.sql("UPDATE jobs SET lease_expires_at = now() - interval '5 seconds' WHERE queue = :queue")
                .param("queue", queue)
                .update();

        int reaperThreads = 3;
        ExecutorService executor = Executors.newFixedThreadPool(reaperThreads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(reaperThreads);
        AtomicInteger totalReaped = new AtomicInteger();

        for (int i = 0; i < reaperThreads; i++) {
            executor.submit(() -> {
                try {
                    start.await();
                    totalReaped.addAndGet(jobReaper.reap());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown();
        boolean completed = done.await(10, TimeUnit.SECONDS);
        executor.shutdown();

        assertThat(completed).isTrue();
        assertThat(totalReaped.get()).isEqualTo(jobCount);

        InvariantChecker.InvariantReport report = invariantChecker.checkAll();
        assertThat(report.passed()).withFailMessage(String.join("\n", report.violations())).isTrue();
    }

    @Test
    @DisplayName("C9: Poison job — fails max_attempts times, moves to DEAD without blocking queue")
    void scenario_C9_poisonJobMovesToDead() throws Exception {
        String queue = "c9-poison-" + UUID.randomUUID();

        // 1. Submit poison job (max_attempts = 2)
        String poisonRes = mockMvc.perform(post("/v1/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"queue": "%s", "payload": {"bad": true}, "max_attempts": 2}
                                """.formatted(queue)))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        String poisonId = com.jayway.jsonpath.JsonPath.read(poisonRes, "$.id");

        // 2. Submit normal job
        String normalRes = mockMvc.perform(post("/v1/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"queue": "%s", "payload": {"good": true}, "priority": -1}
                                """.formatted(queue)))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        String normalId = com.jayway.jsonpath.JsonPath.read(normalRes, "$.id");

        // Attempt 1 for poison job
        mockMvc.perform(post("/v1/queues/" + queue + "/claim")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"worker_id": "w1", "max_jobs": 1}
                                """))
                .andExpect(status().isOk());

        mockMvc.perform(post("/v1/jobs/" + poisonId + "/fail")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"worker_id": "w1", "fencing_token": 1, "error": "NullPointerException", "retryable": true}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.state", is("SCHEDULED")));

        // Set run_at to now for immediate retry
        jdbcClient.sql("UPDATE jobs SET run_at = now() WHERE id = :id")
                .param("id", UUID.fromString(poisonId))
                .update();

        // Attempt 2 for poison job -> should reach max_attempts (2) and transition to DEAD
        mockMvc.perform(post("/v1/queues/" + queue + "/claim")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"worker_id": "w1", "max_jobs": 1}
                                """))
                .andExpect(status().isOk());

        mockMvc.perform(post("/v1/jobs/" + poisonId + "/fail")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"worker_id": "w1", "fencing_token": 2, "error": "Fatal Error", "retryable": true}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.state", is("DEAD")));

        // Normal job can now be claimed and completed without obstruction
        String normalClaim = mockMvc.perform(post("/v1/queues/" + queue + "/claim")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"worker_id": "w2", "max_jobs": 1}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].id", is(normalId)))
                .andReturn().getResponse().getContentAsString();
        long token = ((Number) com.jayway.jsonpath.JsonPath.read(normalClaim, "$[0].fencing_token")).longValue();

        mockMvc.perform(post("/v1/jobs/" + normalId + "/complete")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"worker_id": "w2", "fencing_token": %d}
                                """.formatted(token)))
                .andExpect(status().isNoContent());

        InvariantChecker.InvariantReport report = invariantChecker.checkAll();
        assertThat(report.passed()).withFailMessage(String.join("\n", report.violations())).isTrue();
    }

    @Test
    @DisplayName("C10: Massive concurrent submissions with identical idempotency key produce exactly one job")
    void scenario_C10_concurrentIdempotentSubmissions() throws Exception {
        String queue = "c10-idemp-" + UUID.randomUUID();
        String idempotencyKey = "key-" + UUID.randomUUID();
        int threads = 20;

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        ConcurrentLinkedQueue<Integer> statusCodes = new ConcurrentLinkedQueue<>();

        for (int i = 0; i < threads; i++) {
            executor.submit(() -> {
                try {
                    start.await();
                    int code = mockMvc.perform(post("/v1/jobs")
                                    .header("Idempotency-Key", idempotencyKey)
                                    .contentType(MediaType.APPLICATION_JSON)
                                    .content("""
                                            {"queue": "%s", "payload": {"charge": 100}}
                                            """.formatted(queue)))
                            .andReturn().getResponse().getStatus();
                    statusCodes.add(code);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown();
        boolean completed = done.await(10, TimeUnit.SECONDS);
        executor.shutdown();

        assertThat(completed).isTrue();
        assertThat(statusCodes).hasSize(threads);
        assertThat(statusCodes).contains(201); // Exactly one 201 Created

        Integer count = jdbcClient.sql("SELECT count(*) FROM jobs WHERE queue = :queue AND idempotency_key = :key")
                .param("queue", queue)
                .param("key", idempotencyKey)
                .query(Integer.class)
                .single();
        assertThat(count).isEqualTo(1);

        InvariantChecker.InvariantReport report = invariantChecker.checkAll();
        assertThat(report.passed()).withFailMessage(String.join("\n", report.violations())).isTrue();
    }
}
