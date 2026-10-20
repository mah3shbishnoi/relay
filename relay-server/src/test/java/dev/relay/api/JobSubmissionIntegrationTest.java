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
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class JobSubmissionIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private WebApplicationContext webApplicationContext;

    @Autowired
    private JdbcClient jdbcClient;

    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        this.mockMvc = MockMvcBuilders.webAppContextSetup(this.webApplicationContext).build();
    }

    @Test
    @DisplayName("1. Fresh job submission creates job in DB, writes SUBMITTED event, returns 201")
    void submitJob_freshSubmission_returns201() throws Exception {
        String key = "test-key-" + UUID.randomUUID();
        String requestBody = """
                {
                    "queue": "emails",
                    "payload": {"to": "alice@example.com", "subject": "Welcome"},
                    "priority": 10
                }
                """;

        mockMvc.perform(post("/v1/jobs")
                        .header("Idempotency-Key", key)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id").isNotEmpty())
                .andExpect(jsonPath("$.queue", is("emails")))
                .andExpect(jsonPath("$.state", is("SCHEDULED")))
                .andExpect(jsonPath("$.priority", is(10)))
                .andExpect(jsonPath("$.attempts", is(0)))
                .andExpect(jsonPath("$.max_attempts", is(5)));

        // Verify event log wrote 'SUBMITTED' in same transaction (Guarantee #1)
        Integer eventCount = jdbcClient.sql("SELECT count(*) FROM job_events WHERE event_type = 'SUBMITTED'")
                .query(Integer.class)
                .single();
        assertThat(eventCount).isGreaterThanOrEqualTo(1);
    }

    @Test
    @DisplayName("2. Idempotent replay: Same key + same payload returns 200 OK with original job ID")
    void submitJob_idempotentReplay_returns200() throws Exception {
        String key = "replay-key-" + UUID.randomUUID();
        String requestBody = """
                {
                    "queue": "payments",
                    "payload": {"orderId": 42, "amount": 99.99}
                }
                """;

        // First submission -> 201 Created
        String firstResponse = mockMvc.perform(post("/v1/jobs")
                        .header("Idempotency-Key", key)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();

        // Second submission with exact same key and payload -> 200 OK
        mockMvc.perform(post("/v1/jobs")
                        .header("Idempotency-Key", key)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestBody))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(
                        com.jayway.jsonpath.JsonPath.read(firstResponse, "$.id").toString()
                ));
    }

    @Test
    @DisplayName("3. Payload collision / tamper: Same key + different payload returns 409 Conflict")
    void submitJob_keyReusedWithDifferentPayload_returns409() throws Exception {
        String key = "conflict-key-" + UUID.randomUUID();

        // First request with Alice's payload
        mockMvc.perform(post("/v1/jobs")
                        .header("Idempotency-Key", key)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                    "queue": "reports",
                                    "payload": {"user": "Alice"}
                                }
                                """))
                .andExpect(status().isCreated());

        // Second request with same key but Bob's payload -> 409 Conflict!
        mockMvc.perform(post("/v1/jobs")
                        .header("Idempotency-Key", key)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                    "queue": "reports",
                                    "payload": {"user": "Bob"}
                                }
                                """))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.status", is(409)))
                .andExpect(jsonPath("$.error", is("Conflict")))
                .andExpect(jsonPath("$.message", containsString("already used with a different payload")));
    }

    @Test
    @DisplayName("4. Validation error: Missing queue returns 422 Unprocessable Entity")
    void submitJob_missingQueue_returns422() throws Exception {
        mockMvc.perform(post("/v1/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                    "queue": "",
                                    "payload": {"hello": "world"}
                                }
                                """))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(jsonPath("$.status", is(422)));
    }

    @Test
    @DisplayName("5. GET /v1/jobs/{id} returns job details, or 404 if not found")
    void getJob_foundAndNotFound() throws Exception {
        // Create job
        String createResponse = mockMvc.perform(post("/v1/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                    "queue": "lookup",
                                    "payload": {"item": "xyz"}
                                }
                                """))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();

        String jobId = com.jayway.jsonpath.JsonPath.read(createResponse, "$.id");

        // Fetch existing job
        mockMvc.perform(get("/v1/jobs/" + jobId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id", is(jobId)))
                .andExpect(jsonPath("$.queue", is("lookup")))
                .andExpect(jsonPath("$.state", is("SCHEDULED")));

        // Fetch non-existent job
        mockMvc.perform(get("/v1/jobs/" + UUID.randomUUID()))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.status", is(404)));
    }
}
