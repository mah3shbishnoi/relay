package dev.relay.store;

import dev.relay.config.RelayProperties;
import dev.relay.core.model.Job;
import dev.relay.core.model.JobState;
import dev.relay.api.exception.StaleFencingTokenException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import dev.relay.metrics.RelayMetrics;
import java.time.Duration;

@Repository
public class JobStore {

    private final JdbcClient jdbcClient;
    private final RelayProperties relayProperties;
    private final RelayMetrics metrics;

    public JobStore(JdbcClient jdbcClient, RelayProperties relayProperties, RelayMetrics metrics) {
        this.jdbcClient = jdbcClient;
        this.relayProperties = relayProperties;
        this.metrics = metrics;
    }

    @Transactional
    public SubmitResult submit(
            String queue,
            String payloadJson,
            String payloadHash,
            String idempotencyKey,
            int priority,
            Integer delaySeconds,
            Integer maxAttemptsOverride
    ) {
        int currentDepth = jdbcClient.sql("""
                SELECT count(*) FROM jobs
                WHERE queue = :queue AND state IN ('SCHEDULED', 'RUNNING')
                """)
                .param("queue", queue)
                .query(Integer.class)
                .single();

        if (currentDepth >= relayProperties.getMaxQueueDepth()) {
            throw new QueueFullException("Queue '" + queue + "' is at maximum capacity (" + relayProperties.getMaxQueueDepth() + ")");
        }

        int maxAttempts = (maxAttemptsOverride != null && maxAttemptsOverride > 0)
                ? maxAttemptsOverride
                : relayProperties.getMaxAttempts();

        int delay = (delaySeconds != null && delaySeconds > 0) ? delaySeconds : 0;

        Optional<Job> insertedJob = jdbcClient.sql("""
                INSERT INTO jobs (queue, payload, payload_hash, idempotency_key, priority, run_at, max_attempts)
                VALUES (:queue, :payload::jsonb, :payloadHash, :idempotencyKey, :priority,
                        CASE WHEN :delay > 0 THEN now() + (:delay * interval '1 second') ELSE now() END,
                        :maxAttempts)
                ON CONFLICT (queue, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
                RETURNING id, queue, payload::text, payload_hash, idempotency_key, state, priority,
                          run_at, attempts, max_attempts, lease_owner, lease_expires_at, fencing_token,
                          last_error, created_at, updated_at, finished_at
                """)
                .param("queue", queue)
                .param("payload", payloadJson)
                .param("payloadHash", payloadHash)
                .param("idempotencyKey", idempotencyKey)
                .param("priority", priority)
                .param("delay", delay)
                .param("maxAttempts", maxAttempts)
                .query(this::mapJob)
                .optional();

        if (insertedJob.isPresent()) {
            Job job = insertedJob.get();
            jdbcClient.sql("""
                    INSERT INTO job_events (job_id, event_type, attempt, fencing_token, at)
                    VALUES (:jobId, 'SUBMITTED', 0, 0, now())
                    """)
                    .param("jobId", job.id())
                    .update();

            metrics.recordJobSubmitted(job.queue());
            return new SubmitResult.Created(job);
        }

        Job existing = jdbcClient.sql("""
                SELECT id, queue, payload::text, payload_hash, idempotency_key, state, priority,
                       run_at, attempts, max_attempts, lease_owner, lease_expires_at, fencing_token,
                       last_error, created_at, updated_at, finished_at
                FROM jobs
                WHERE queue = :queue AND idempotency_key = :idempotencyKey
                """)
                .param("queue", queue)
                .param("idempotencyKey", idempotencyKey)
                .query(this::mapJob)
                .single();

        if (existing.payloadHash().equals(payloadHash)) {
            return new SubmitResult.Duplicate(existing);
        } else {
            return new SubmitResult.Conflict(existing);
        }
    }

    @Transactional
    public List<Job> claim(String queue, String workerId, int limit, Integer leaseSecondsOverride) {
        int leaseSec = (leaseSecondsOverride != null && leaseSecondsOverride > 0)
                ? leaseSecondsOverride
                : relayProperties.getLeaseSeconds();
        int n = limit > 0 ? limit : relayProperties.getClaimBatchSize();

        List<Job> claimedJobs = jdbcClient.sql("""
                WITH picked AS (
                  SELECT id FROM jobs
                  WHERE queue = :queue AND state = 'SCHEDULED' AND run_at <= now()
                  ORDER BY priority DESC, run_at ASC
                  LIMIT :n
                  FOR UPDATE SKIP LOCKED
                )
                UPDATE jobs j
                SET state = 'RUNNING',
                    lease_owner = :workerId,
                    lease_expires_at = now() + (:leaseSec * interval '1 second'),
                    attempts = j.attempts + 1,
                    fencing_token = j.fencing_token + 1,
                    updated_at = now()
                FROM picked
                WHERE j.id = picked.id
                RETURNING j.id, j.queue, j.payload::text, j.payload_hash, j.idempotency_key, j.state, j.priority,
                          j.run_at, j.attempts, j.max_attempts, j.lease_owner, j.lease_expires_at, j.fencing_token,
                          j.last_error, j.created_at, j.updated_at, j.finished_at
                """)
                .param("queue", queue)
                .param("workerId", workerId)
                .param("leaseSec", leaseSec)
                .param("n", n)
                .query(this::mapJob)
                .list();

        for (Job job : claimedJobs) {
            jdbcClient.sql("""
                    INSERT INTO job_events (job_id, event_type, attempt, fencing_token, worker_id, at)
                    VALUES (:jobId, 'CLAIMED', :attempt, :fencingToken, :workerId, now())
                    """)
                    .param("jobId", job.id())
                    .param("attempt", job.attempts())
                    .param("fencingToken", job.fencingToken())
                    .param("workerId", workerId)
                    .update();

            metrics.recordJobClaimed(job.queue());
        }

        return claimedJobs;
    }

    @Transactional(noRollbackFor = StaleFencingTokenException.class)
    public Instant heartbeat(UUID id, String workerId, long fencingToken, Integer leaseSecondsOverride) {
        int leaseSec = (leaseSecondsOverride != null && leaseSecondsOverride > 0)
                ? leaseSecondsOverride
                : relayProperties.getLeaseSeconds();

        Optional<Timestamp> newExpiry = jdbcClient.sql("""
                UPDATE jobs
                SET lease_expires_at = now() + (:leaseSec * interval '1 second'), updated_at = now()
                WHERE id = :id AND state = 'RUNNING'
                  AND lease_owner = :workerId AND fencing_token = :fencingToken
                  AND lease_expires_at > now()
                RETURNING lease_expires_at
                """)
                .param("id", id)
                .param("workerId", workerId)
                .param("fencingToken", fencingToken)
                .param("leaseSec", leaseSec)
                .query(Timestamp.class)
                .optional();

        if (newExpiry.isEmpty()) {
            metrics.recordStaleRejection();
            jdbcClient.sql("""
                    INSERT INTO job_events (job_id, event_type, fencing_token, worker_id, at)
                    VALUES (:jobId, 'REJECTED_STALE', :fencingToken, :workerId, now())
                    """)
                    .param("jobId", id)
                    .param("fencingToken", fencingToken)
                    .param("workerId", workerId)
                    .update();
            throw new StaleFencingTokenException("Lease expired or invalid fencing token for job " + id);
        }

        jdbcClient.sql("""
                INSERT INTO job_events (job_id, event_type, fencing_token, worker_id, at)
                VALUES (:jobId, 'HEARTBEAT', :fencingToken, :workerId, now())
                """)
                .param("jobId", id)
                .param("fencingToken", fencingToken)
                .param("workerId", workerId)
                .update();

        return newExpiry.get().toInstant();
    }

    @Transactional(noRollbackFor = StaleFencingTokenException.class)
    public void complete(UUID id, String workerId, long fencingToken) {
        Optional<UUID> completedId = jdbcClient.sql("""
                UPDATE jobs
                SET state = 'SUCCEEDED', finished_at = now(),
                    lease_owner = NULL, lease_expires_at = NULL, updated_at = now()
                WHERE id = :id AND state = 'RUNNING'
                  AND lease_owner = :workerId AND fencing_token = :fencingToken
                  AND lease_expires_at > now()
                RETURNING id
                """)
                .param("id", id)
                .param("workerId", workerId)
                .param("fencingToken", fencingToken)
                .query(UUID.class)
                .optional();

        if (completedId.isEmpty()) {
            metrics.recordStaleRejection();
            jdbcClient.sql("""
                    INSERT INTO job_events (job_id, event_type, fencing_token, worker_id, at)
                    VALUES (:jobId, 'REJECTED_STALE', :fencingToken, :workerId, now())
                    """)
                    .param("jobId", id)
                    .param("fencingToken", fencingToken)
                    .param("workerId", workerId)
                    .update();
            throw new StaleFencingTokenException("Lease expired or invalid fencing token for job " + id);
        }

        jdbcClient.sql("""
                INSERT INTO job_events (job_id, event_type, fencing_token, worker_id, at)
                VALUES (:jobId, 'COMPLETED', :fencingToken, :workerId, now())
                """)
                .param("jobId", id)
                .param("fencingToken", fencingToken)
                .param("workerId", workerId)
                .update();
    }

    @Transactional(noRollbackFor = StaleFencingTokenException.class)
    public FailResult fail(UUID id, String workerId, long fencingToken, String errorMessage, boolean retryable) {
        int baseSec = relayProperties.getBackoffBaseSeconds();
        int capSec = relayProperties.getBackoffCapSeconds();

        record FailRow(String state, Timestamp runAt) {}

        Optional<FailRow> row = jdbcClient.sql("""
                UPDATE jobs
                SET state = CASE WHEN NOT :retryable OR attempts >= max_attempts
                                 THEN 'DEAD'::job_state ELSE 'SCHEDULED'::job_state END,
                    run_at = CASE WHEN NOT :retryable OR attempts >= max_attempts THEN run_at
                                  ELSE now() + ((random() * LEAST(:capSec, :baseSec * power(2::numeric, (attempts - 1)::numeric))) * interval '1 second') END,
                    finished_at = CASE WHEN NOT :retryable OR attempts >= max_attempts THEN now() END,
                    last_error = :errorMessage,
                    lease_owner = NULL, lease_expires_at = NULL, updated_at = now()
                WHERE id = :id AND state = 'RUNNING'
                  AND lease_owner = :workerId AND fencing_token = :fencingToken
                  AND lease_expires_at > now()
                RETURNING state, run_at
                """)
                .param("id", id)
                .param("workerId", workerId)
                .param("fencingToken", fencingToken)
                .param("errorMessage", errorMessage)
                .param("retryable", retryable)
                .param("baseSec", baseSec)
                .param("capSec", capSec)
                .query((rs, rowNum) -> new FailRow(rs.getString("state"), rs.getTimestamp("run_at")))
                .optional();

        if (row.isEmpty()) {
            metrics.recordStaleRejection();
            jdbcClient.sql("""
                    INSERT INTO job_events (job_id, event_type, fencing_token, worker_id, at)
                    VALUES (:jobId, 'REJECTED_STALE', :fencingToken, :workerId, now())
                    """)
                    .param("jobId", id)
                    .param("fencingToken", fencingToken)
                    .param("workerId", workerId)
                    .update();
            throw new StaleFencingTokenException("Lease expired or invalid fencing token for job " + id);
        }

        JobState finalState = JobState.valueOf(row.get().state());
        String eventType = (finalState == JobState.DEAD) ? "DEAD" : "FAILED";

        jdbcClient.sql("""
                INSERT INTO job_events (job_id, event_type, fencing_token, worker_id, at)
                VALUES (:jobId, :eventType, :fencingToken, :workerId, now())
                """)
                .param("jobId", id)
                .param("eventType", eventType)
                .param("fencingToken", fencingToken)
                .param("workerId", workerId)
                .update();

        return new FailResult(finalState, row.get().runAt() != null ? row.get().runAt().toInstant() : null);
    }

    @Transactional
    public boolean requeue(UUID id) {
        Optional<UUID> requeued = jdbcClient.sql("""
                UPDATE jobs
                SET state = 'SCHEDULED', run_at = now(), attempts = 0, last_error = NULL,
                    finished_at = NULL, updated_at = now()
                WHERE id = :id AND state = 'DEAD'
                RETURNING id
                """)
                .param("id", id)
                .query(UUID.class)
                .optional();

        if (requeued.isPresent()) {
            jdbcClient.sql("""
                    INSERT INTO job_events (job_id, event_type, at)
                    VALUES (:jobId, 'REQUEUED', now())
                    """)
                    .param("jobId", id)
                    .update();
            return true;
        }
        return false;
    }

    @Transactional
    public int reapExpiredLeases(int batchSize) {
        int baseSec = relayProperties.getBackoffBaseSeconds();
        int capSec = relayProperties.getBackoffCapSeconds();

        record ReapedRow(UUID id, String state, int attempts, long fencingToken, String leaseOwner) {}

        List<ReapedRow> reaped = jdbcClient.sql("""
                WITH expired AS (
                  SELECT id FROM jobs
                  WHERE state = 'RUNNING' AND lease_expires_at < now()
                  ORDER BY lease_expires_at ASC
                  LIMIT :batchSize
                  FOR UPDATE SKIP LOCKED
                )
                UPDATE jobs j
                SET state = CASE WHEN j.attempts >= j.max_attempts
                                 THEN 'DEAD'::job_state ELSE 'SCHEDULED'::job_state END,
                    run_at = CASE WHEN j.attempts >= j.max_attempts THEN j.run_at
                                  ELSE now() + ((random() * LEAST(:capSec, :baseSec * power(2::numeric, (j.attempts - 1)::numeric))) * interval '1 second') END,
                    finished_at = CASE WHEN j.attempts >= j.max_attempts THEN now() END,
                    last_error = 'lease expired',
                    lease_owner = NULL, lease_expires_at = NULL, updated_at = now()
                FROM expired
                WHERE j.id = expired.id
                RETURNING j.id, j.state, j.attempts, j.fencing_token, j.lease_owner
                """)
                .param("batchSize", batchSize)
                .param("baseSec", baseSec)
                .param("capSec", capSec)
                .query((rs, rowNum) -> new ReapedRow(
                        rs.getObject("id", UUID.class),
                        rs.getString("state"),
                        rs.getInt("attempts"),
                        rs.getLong("fencing_token"),
                        rs.getString("lease_owner")
                ))
                .list();

        for (ReapedRow r : reaped) {
            String eventType = "DEAD".equals(r.state()) ? "DEAD" : "LEASE_EXPIRED";
            jdbcClient.sql("""
                    INSERT INTO job_events (job_id, event_type, attempt, fencing_token, worker_id, at)
                    VALUES (:jobId, :eventType, :attempt, :fencingToken, :workerId, now())
                    """)
                    .param("jobId", r.id())
                    .param("eventType", eventType)
                    .param("attempt", r.attempts())
                    .param("fencingToken", r.fencingToken())
                    .param("workerId", r.leaseOwner())
                    .update();
        }

        return reaped.size();
    }

    @Transactional
    public int deleteSucceededOlderThan(Duration retentionPeriod, int batchSize) {
        return jdbcClient.sql("""
                WITH to_delete AS (
                  SELECT id FROM jobs
                  WHERE state = 'SUCCEEDED'
                    AND finished_at < now() - (:retentionSeconds * interval '1 second')
                  LIMIT :batchSize
                  FOR UPDATE SKIP LOCKED
                ),
                deleted_events AS (
                  DELETE FROM job_events WHERE job_id IN (SELECT id FROM to_delete)
                )
                DELETE FROM jobs
                WHERE id IN (SELECT id FROM to_delete)
                """)
                .param("retentionSeconds", retentionPeriod.toSeconds())
                .param("batchSize", batchSize)
                .update();
    }

    public QueueStats getQueueStats(String queue) {
        return jdbcClient.sql("""
                SELECT
                  COUNT(*) FILTER (WHERE state = 'SCHEDULED') AS scheduled_count,
                  COUNT(*) FILTER (WHERE state = 'RUNNING') AS running_count,
                  COUNT(*) FILTER (WHERE state = 'SUCCEEDED') AS succeeded_count,
                  COUNT(*) FILTER (WHERE state = 'DEAD') AS dead_count,
                  COALESCE(EXTRACT(EPOCH FROM (now() - MIN(created_at) FILTER (WHERE state = 'SCHEDULED'))), 0) AS oldest_scheduled_age_seconds
                FROM jobs
                WHERE queue = :queue
                """)
                .param("queue", queue)
                .query((rs, rowNum) -> new QueueStats(
                        rs.getLong("scheduled_count"),
                        rs.getLong("running_count"),
                        rs.getLong("succeeded_count"),
                        rs.getLong("dead_count"),
                        rs.getDouble("oldest_scheduled_age_seconds")
                ))
                .single();
    }

    public Optional<Job> findById(UUID id) {
        return jdbcClient.sql("""
                SELECT id, queue, payload::text, payload_hash, idempotency_key, state, priority,
                       run_at, attempts, max_attempts, lease_owner, lease_expires_at, fencing_token,
                       last_error, created_at, updated_at, finished_at
                FROM jobs
                WHERE id = :id
                """)
                .param("id", id)
                .query(this::mapJob)
                .optional();
    }

    private Job mapJob(ResultSet rs, int rowNum) throws SQLException {
        Timestamp runAtTs = rs.getTimestamp("run_at");
        Timestamp leaseExpiresAtTs = rs.getTimestamp("lease_expires_at");
        Timestamp createdAtTs = rs.getTimestamp("created_at");
        Timestamp updatedAtTs = rs.getTimestamp("updated_at");
        Timestamp finishedAtTs = rs.getTimestamp("finished_at");

        return new Job(
                rs.getObject("id", UUID.class),
                rs.getString("queue"),
                rs.getString("payload"),
                rs.getString("payload_hash"),
                rs.getString("idempotency_key"),
                JobState.valueOf(rs.getString("state")),
                rs.getInt("priority"),
                runAtTs != null ? runAtTs.toInstant() : null,
                rs.getInt("attempts"),
                rs.getInt("max_attempts"),
                rs.getString("lease_owner"),
                leaseExpiresAtTs != null ? leaseExpiresAtTs.toInstant() : null,
                rs.getLong("fencing_token"),
                rs.getString("last_error"),
                createdAtTs != null ? createdAtTs.toInstant() : null,
                updatedAtTs != null ? updatedAtTs.toInstant() : null,
                finishedAtTs != null ? finishedAtTs.toInstant() : null
        );
    }
}
