package dev.relay.chaos;

import org.springframework.jdbc.core.simple.JdbcClient;

import java.util.ArrayList;
import java.util.List;

public class InvariantChecker {

    private final JdbcClient jdbcClient;

    public InvariantChecker(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    public record InvariantReport(
            boolean passed,
            List<String> violations
    ) {}

    public InvariantReport checkAll() {
        List<String> violations = new ArrayList<>();

        // I1: No lost jobs (all jobs must be in a valid state)
        int invalidStates = jdbcClient.sql("""
                SELECT count(*) FROM jobs
                WHERE state NOT IN ('SCHEDULED', 'RUNNING', 'SUCCEEDED', 'DEAD')
                """)
                .query(Integer.class)
                .single();
        if (invalidStates > 0) {
            violations.add("I1 Violation: Found " + invalidStates + " jobs with invalid/unknown state");
        }

        // I2: Attempts never exceed max_attempts
        int exceededAttempts = jdbcClient.sql("""
                SELECT count(*) FROM jobs WHERE attempts > max_attempts
                """)
                .query(Integer.class)
                .single();
        if (exceededAttempts > 0) {
            violations.add("I2 Violation: Found " + exceededAttempts + " jobs where attempts > max_attempts");
        }

        // I3: Each SUCCEEDED job has exactly one COMPLETED event
        int invalidCompletedEvents = jdbcClient.sql("""
                SELECT count(*) FROM (
                    SELECT job_id, count(*) AS c
                    FROM job_events
                    WHERE event_type = 'COMPLETED'
                    GROUP BY job_id
                    HAVING count(*) <> 1
                ) sub
                """)
                .query(Integer.class)
                .single();
        if (invalidCompletedEvents > 0) {
            violations.add("I3 Violation: Found " + invalidCompletedEvents + " jobs with multiple or zero COMPLETED events");
        }

        // I4: No stale acceptance (COMPLETED token must match the latest CLAIMED token for that job)
        int staleCompletions = jdbcClient.sql("""
                SELECT count(*) FROM (
                    SELECT c.job_id
                    FROM job_events c
                    JOIN (
                        SELECT job_id, MAX(fencing_token) AS max_token
                        FROM job_events
                        WHERE event_type = 'CLAIMED'
                        GROUP BY job_id
                    ) l ON c.job_id = l.job_id
                    WHERE c.event_type = 'COMPLETED' AND c.fencing_token < l.max_token
                ) sub
                """)
                .query(Integer.class)
                .single();
        if (staleCompletions > 0) {
            violations.add("I4 Violation: Found " + staleCompletions + " COMPLETED events accepted with stale fencing token");
        }

        // I5: Fencing tokens in CLAIMED events increase by exactly 1 per job
        int nonMonotonicTokens = jdbcClient.sql("""
                SELECT count(*) FROM (
                    SELECT job_id, fencing_token,
                           LAG(fencing_token) OVER (PARTITION BY job_id ORDER BY id) AS prev_token
                    FROM job_events
                    WHERE event_type = 'CLAIMED'
                ) sub
                WHERE prev_token IS NOT NULL AND fencing_token <> prev_token + 1
                """)
                .query(Integer.class)
                .single();
        if (nonMonotonicTokens > 0) {
            violations.add("I5 Violation: Found " + nonMonotonicTokens + " CLAIMED events where fencing token did not increment by 1");
        }

        // I6: Exactly one job exists per (queue, idempotency_key)
        int duplicateIdempotency = jdbcClient.sql("""
                SELECT count(*) FROM (
                    SELECT queue, idempotency_key, count(*)
                    FROM jobs
                    WHERE idempotency_key IS NOT NULL
                    GROUP BY queue, idempotency_key
                    HAVING count(*) > 1
                ) sub
                """)
                .query(Integer.class)
                .single();
        if (duplicateIdempotency > 0) {
            violations.add("I6 Violation: Found duplicate jobs for the same (queue, idempotency_key)");
        }

        // I8: Every DEAD job reached max_attempts or has a recorded last_error
        int invalidDead = jdbcClient.sql("""
                SELECT count(*) FROM jobs
                WHERE state = 'DEAD' AND attempts < max_attempts AND last_error IS NULL
                """)
                .query(Integer.class)
                .single();
        if (invalidDead > 0) {
            violations.add("I8 Violation: Found " + invalidDead + " DEAD jobs without reaching max_attempts or having an error");
        }

        return new InvariantReport(violations.isEmpty(), violations);
    }
}
