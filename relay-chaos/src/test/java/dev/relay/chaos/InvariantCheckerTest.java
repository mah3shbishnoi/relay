package dev.relay.chaos;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.PostgreSQLContainer;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class InvariantCheckerTest {

    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:17");
    static JdbcClient jdbcClient;
    static InvariantChecker checker;

    @BeforeAll
    static void setUp() {
        postgres.start();

        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource.setUrl(postgres.getJdbcUrl());
        dataSource.setUsername(postgres.getUsername());
        dataSource.setPassword(postgres.getPassword());

        jdbcClient = JdbcClient.create(dataSource);
        checker = new InvariantChecker(jdbcClient);

        // Initialize schema
        jdbcClient.sql("""
                CREATE TYPE job_state AS ENUM ('SCHEDULED', 'RUNNING', 'SUCCEEDED', 'DEAD');

                CREATE TABLE jobs (
                  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                  queue            TEXT NOT NULL,
                  payload          JSONB NOT NULL,
                  payload_hash     TEXT NOT NULL,
                  idempotency_key  TEXT,
                  state            job_state NOT NULL DEFAULT 'SCHEDULED',
                  priority         INT NOT NULL DEFAULT 0,
                  run_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
                  attempts         INT NOT NULL DEFAULT 0,
                  max_attempts     INT NOT NULL DEFAULT 5,
                  lease_owner      TEXT,
                  lease_expires_at TIMESTAMPTZ,
                  fencing_token    BIGINT NOT NULL DEFAULT 0,
                  last_error       TEXT,
                  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
                  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
                  finished_at      TIMESTAMPTZ
                );

                CREATE UNIQUE INDEX uq_jobs_idempotency
                  ON jobs (queue, idempotency_key) WHERE idempotency_key IS NOT NULL;

                CREATE TABLE job_events (
                  id            BIGSERIAL PRIMARY KEY,
                  job_id        UUID NOT NULL,
                  event_type    TEXT NOT NULL,
                  attempt       INT,
                  fencing_token BIGINT,
                  worker_id     TEXT,
                  at            TIMESTAMPTZ NOT NULL DEFAULT now(),
                  details       JSONB
                );
                """).update();
    }

    @org.junit.jupiter.api.BeforeEach
    void clean() {
        jdbcClient.sql("TRUNCATE jobs, job_events CASCADE").update();
    }

    @AfterAll
    static void tearDown() {
        postgres.stop();
    }

    @Test
    @DisplayName("Empty or fully consistent database passes all invariants")
    void checkAll_consistentDatabase_passes() {
        UUID jobId = UUID.randomUUID();

        // Consistent job: submitted, claimed (token 1), completed (token 1)
        jdbcClient.sql("""
                INSERT INTO jobs (id, queue, payload, payload_hash, state, attempts, max_attempts)
                VALUES (:id, 'invariants', '{}'::jsonb, 'hash', 'SUCCEEDED', 1, 5)
                """)
                .param("id", jobId)
                .update();

        jdbcClient.sql("""
                INSERT INTO job_events (job_id, event_type, attempt, fencing_token)
                VALUES (:id, 'SUBMITTED', 0, 0),
                       (:id, 'CLAIMED', 1, 1),
                       (:id, 'COMPLETED', 1, 1)
                """)
                .param("id", jobId)
                .update();

        InvariantChecker.InvariantReport report = checker.checkAll();
        assertThat(report.passed()).isTrue();
        assertThat(report.violations()).isEmpty();
    }

    @Test
    @DisplayName("Detects I4 Violation when a job has a stale COMPLETED event accepted")
    void checkAll_staleCompletion_detectsI4Violation() {
        UUID jobId = UUID.randomUUID();

        jdbcClient.sql("""
                INSERT INTO jobs (id, queue, payload, payload_hash, state, attempts, max_attempts)
                VALUES (:id, 'stale-queue', '{}'::jsonb, 'hash', 'SUCCEEDED', 2, 5)
                """)
                .param("id", jobId)
                .update();

        // Claimed with token 1, then re-claimed with token 2, BUT completed with token 1!
        jdbcClient.sql("""
                INSERT INTO job_events (job_id, event_type, attempt, fencing_token)
                VALUES (:id, 'CLAIMED', 1, 1),
                       (:id, 'CLAIMED', 2, 2),
                       (:id, 'COMPLETED', 1, 1)
                """)
                .param("id", jobId)
                .update();

        InvariantChecker.InvariantReport report = checker.checkAll();
        assertThat(report.passed()).isFalse();
        assertThat(report.violations()).anyMatch(v -> v.contains("I4 Violation"));
    }
}
