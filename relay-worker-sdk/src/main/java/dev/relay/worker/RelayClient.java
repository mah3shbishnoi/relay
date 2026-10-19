package dev.relay.worker;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class RelayClient {

    private final String baseUrl;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    public RelayClient(String baseUrl) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        this.objectMapper = new ObjectMapper();
    }

    public UUID submit(String queue, String payloadJson, String idempotencyKey, Integer priority, Integer delaySeconds) throws IOException, InterruptedException {
        Map<String, Object> bodyMap = new java.util.HashMap<>();
        bodyMap.put("queue", queue);
        bodyMap.put("payload", objectMapper.readTree(payloadJson));
        if (priority != null) bodyMap.put("priority", priority);
        if (delaySeconds != null) bodyMap.put("delay_seconds", delaySeconds);

        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/v1/jobs"))
                .header("Content-Type", "application/json")
                .timeout(Duration.ofSeconds(10));

        if (idempotencyKey != null) {
            builder.header("Idempotency-Key", idempotencyKey);
        }

        HttpRequest request = builder.POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(bodyMap))).build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 201 && response.statusCode() != 200) {
            throw new IOException("Failed to submit job: HTTP " + response.statusCode() + " " + response.body());
        }

        JsonNode resNode = objectMapper.readTree(response.body());
        return UUID.fromString(resNode.get("id").asText());
    }

    public List<ClaimedJob> claim(String queue, String workerId, int maxJobs, Integer leaseSeconds) throws IOException, InterruptedException {
        Map<String, Object> bodyMap = new java.util.HashMap<>();
        bodyMap.put("worker_id", workerId);
        bodyMap.put("max_jobs", maxJobs);
        if (leaseSeconds != null) {
            bodyMap.put("lease_seconds", leaseSeconds);
        }

        String requestBody = objectMapper.writeValueAsString(bodyMap);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/v1/queues/" + queue + "/claim"))
                .header("Content-Type", "application/json")
                .timeout(Duration.ofSeconds(10))
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IOException("Failed to claim jobs: HTTP " + response.statusCode() + " " + response.body());
        }

        JsonNode root = objectMapper.readTree(response.body());
        List<ClaimedJob> jobs = new ArrayList<>();
        if (root.isArray()) {
            for (JsonNode node : root) {
                UUID id = UUID.fromString(node.get("id").asText());
                String payload = node.get("payload").toString();
                int attempt = node.get("attempt").asInt();
                long fencingToken = node.get("fencing_token").asLong();
                Instant leaseExpiry = Instant.parse(node.get("lease_expires_at").asText());
                jobs.add(new ClaimedJob(id, payload, attempt, fencingToken, leaseExpiry));
            }
        }
        return jobs;
    }

    public boolean heartbeat(UUID id, String workerId, long fencingToken, Integer leaseSeconds) {
        try {
            Map<String, Object> bodyMap = new java.util.HashMap<>();
            bodyMap.put("worker_id", workerId);
            bodyMap.put("fencing_token", fencingToken);
            if (leaseSeconds != null) {
                bodyMap.put("lease_seconds", leaseSeconds);
            }

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + "/v1/jobs/" + id + "/heartbeat"))
                    .header("Content-Type", "application/json")
                    .timeout(Duration.ofSeconds(5))
                    .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(bodyMap)))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            return response.statusCode() == 200;
        } catch (Exception e) {
            return false;
        }
    }

    public boolean complete(UUID id, String workerId, long fencingToken) {
        try {
            Map<String, Object> bodyMap = Map.of(
                    "worker_id", workerId,
                    "fencing_token", fencingToken
            );

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + "/v1/jobs/" + id + "/complete"))
                    .header("Content-Type", "application/json")
                    .timeout(Duration.ofSeconds(5))
                    .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(bodyMap)))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            return response.statusCode() == 204;
        } catch (Exception e) {
            return false;
        }
    }

    public boolean fail(UUID id, String workerId, long fencingToken, String errorMessage, boolean retryable) {
        try {
            Map<String, Object> bodyMap = Map.of(
                    "worker_id", workerId,
                    "fencing_token", fencingToken,
                    "error", errorMessage != null ? errorMessage : "",
                    "retryable", retryable
            );

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + "/v1/jobs/" + id + "/fail"))
                    .header("Content-Type", "application/json")
                    .timeout(Duration.ofSeconds(5))
                    .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(bodyMap)))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            return response.statusCode() == 200;
        } catch (Exception e) {
            return false;
        }
    }
}
