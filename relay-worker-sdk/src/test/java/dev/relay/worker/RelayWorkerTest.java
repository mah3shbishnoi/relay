package dev.relay.worker;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class RelayWorkerTest {

    private HttpServer server;
    private int port;
    private final AtomicBoolean completeCalled = new AtomicBoolean(false);

    @BeforeEach
    void setUp() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        port = server.getAddress().getPort();

        UUID jobId = UUID.randomUUID();

        // Mock /v1/queues/test-queue/claim
        server.createContext("/v1/queues/test-queue/claim", exchange -> {
            String response = """
                    [
                        {
                            "id": "%s",
                            "payload": {"action": "email"},
                            "attempt": 1,
                            "fencing_token": 1,
                            "lease_expires_at": "2026-10-19T20:00:00Z"
                        }
                    ]
                    """.formatted(jobId);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, response.getBytes().length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.getBytes());
            }
        });

        // Mock /v1/jobs/{id}/complete
        server.createContext("/v1/jobs/" + jobId + "/complete", exchange -> {
            completeCalled.set(true);
            exchange.sendResponseHeaders(204, -1);
            exchange.close();
        });

        server.start();
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    @DisplayName("RelayWorker claims job, invokes handler, and reports completion")
    void worker_claimsAndCompletesJob() throws Exception {
        RelayClient client = new RelayClient("http://127.0.0.1:" + port);
        CountDownLatch handled = new CountDownLatch(1);

        RelayWorker worker = new RelayWorker(
                client,
                "test-queue",
                "worker-test-1",
                ctx -> {
                    assertThat(ctx.fencingToken()).isEqualTo(1L);
                    handled.countDown();
                },
                1,
                30,
                5,
                100
        );

        worker.start();
        boolean processed = handled.await(5, TimeUnit.SECONDS);
        worker.stop(Duration.ofSeconds(2));

        assertThat(processed).isTrue();
        assertThat(completeCalled.get()).isTrue();
    }
}
