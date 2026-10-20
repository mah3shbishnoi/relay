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
        Integer tableExists = jdbcClient.sql("""
                SELECT count(*) FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = 'jobs'
                """)
                .query(Integer.class)
                .single();

        assertThat(tableExists).isEqualTo(1);

        Integer jobCount = jdbcClient.sql("SELECT count(*) FROM jobs")
                .query(Integer.class)
                .single();
        assertThat(jobCount).isGreaterThanOrEqualTo(0);
    }
}
