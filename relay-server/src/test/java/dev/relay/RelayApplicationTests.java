package dev.relay;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;

import static org.assertj.core.api.Assertions.assertThat;

class RelayApplicationTests extends AbstractIntegrationTest {

    @Autowired
    private JdbcClient jdbcClient;

    @Test
    @DisplayName("Context loads and Flyway applies V1 schema to PostgreSQL")
    void contextLoadsAndSchemaApplied() {
        Integer jobCount = jdbcClient.sql("SELECT count(*) FROM jobs")
                .query(Integer.class)
                .single();

        assertThat(jobCount).isZero();
    }
}
