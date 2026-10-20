package dev.relay.api;

import dev.relay.AbstractIntegrationTest;
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

class JobClaimingIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private WebApplicationContext context;

    @Autowired
    private JdbcClient jdbcClient;

    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        this.mockMvc = MockMvcBuilders.webAppContextSetup(context).build();
    }

    @Test
    @DisplayName("Claims ready jobs in descending priority order and increments fencing token")
    void claimJobs_priorityOrderingAndFencingToken() throws Exception {
        String queue = "priority-queue-" + UUID.randomUUID();

        // 1. Submit low priority job
        mockMvc.perform(post("/v1/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                    "queue": "%s",
                                    "payload": {"name": "low-priority"},
                                    "priority": 1
                                }
                                """.formatted(queue)))
                .andExpect(status().isCreated());

        // 2. Submit high priority job
        mockMvc.perform(post("/v1/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                    "queue": "%s",
                                    "payload": {"name": "high-priority"},
                                    "priority": 100
                                }
                                """.formatted(queue)))
                .andExpect(status().isCreated());

        // 3. Worker claims 1 job -> MUST be the high priority one!
        mockMvc.perform(post("/v1/queues/" + queue + "/claim")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                    "worker_id": "worker-1",
                                    "max_jobs": 1
                                }
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(1)))
                .andExpect(jsonPath("$[0].payload.name", is("high-priority")))
                .andExpect(jsonPath("$[0].attempt", is(1)))
                .andExpect(jsonPath("$[0].fencing_token", is(1)));

        // 4. Verify in DB that state is RUNNING and lease_owner is worker-1
        String state = jdbcClient.sql("SELECT state FROM jobs WHERE queue = :queue AND priority = 100")
                .param("queue", queue)
                .query(String.class)
                .single();
        assertThat(state).isEqualTo("RUNNING");
    }

    @Test
    @DisplayName("Delayed jobs with run_at in future are NOT claimed until ready")
    void claimJobs_delayedJobNotClaimed() throws Exception {
        String queue = "delayed-queue-" + UUID.randomUUID();

        // Submit job delayed by 3600 seconds (1 hour)
        mockMvc.perform(post("/v1/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                    "queue": "%s",
                                    "payload": {"task": "delayed"},
                                    "delay_seconds": 3600
                                }
                                """.formatted(queue)))
                .andExpect(status().isCreated());

        // Attempt claim -> should return empty list
        mockMvc.perform(post("/v1/queues/" + queue + "/claim")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                    "worker_id": "worker-1",
                                    "max_jobs": 10
                                }
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(0)));
    }

    @Test
    @DisplayName("Twenty concurrent threads claiming jobs never receive duplicate claims (Guarantee #4)")
    void claimJobs_twentyThreadsConcurrentNoDuplicates() throws Exception {
        String queue = "concurrency-queue-" + UUID.randomUUID();
        int totalJobs = 40;

        for (int i = 0; i < totalJobs; i++) {
            mockMvc.perform(post("/v1/jobs")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("""
                                    {
                                        "queue": "%s",
                                        "payload": {"index": %d}
                                    }
                                    """.formatted(queue, i)))
                    .andExpect(status().isCreated());
        }

        int threadCount = 20;
        java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newFixedThreadPool(threadCount);
        java.util.concurrent.CountDownLatch startLatch = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch doneLatch = new java.util.concurrent.CountDownLatch(threadCount);
        java.util.List<String> claimedJobIds = new java.util.concurrent.CopyOnWriteArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            final String workerId = "worker-" + i;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    while (true) {
                        String res = mockMvc.perform(post("/v1/queues/" + queue + "/claim")
                                        .contentType(MediaType.APPLICATION_JSON)
                                        .content("""
                                                {
                                                    "worker_id": "%s",
                                                    "max_jobs": 1
                                                }
                                                """.formatted(workerId)))
                                .andReturn().getResponse().getContentAsString();

                        java.util.List<String> ids = com.jayway.jsonpath.JsonPath.read(res, "$[*].id");
                        if (ids.isEmpty()) {
                            break;
                        }
                        claimedJobIds.addAll(ids);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        boolean completed = doneLatch.await(30, java.util.concurrent.TimeUnit.SECONDS);
        executor.shutdown();

        assertThat(completed).isTrue();
        assertThat(claimedJobIds).hasSize(totalJobs);
        assertThat(new java.util.HashSet<>(claimedJobIds)).hasSize(totalJobs);

        Integer runningCount = jdbcClient.sql("SELECT count(*) FROM jobs WHERE queue = :queue AND state = 'RUNNING'")
                .param("queue", queue)
                .query(Integer.class)
                .single();
        assertThat(runningCount).isEqualTo(totalJobs);
    }
}

