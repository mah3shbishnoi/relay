package dev.relay.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "relay")
public class RelayProperties {
    private int leaseSeconds = 30;
    private int heartbeatIntervalSeconds = 10;
    private int maxAttempts = 5;
    private int backoffBaseSeconds = 2;
    private int backoffCapSeconds = 300;
    private int claimBatchSize = 10;
    private int reaperIntervalSeconds = 5;
    private int reaperBatchSize = 100;
    private int maxPayloadBytes = 262144;
    private int maxQueueDepth = 100000;
    private int workerPollIntervalMs = 500;
    private int retentionDays = 7;
    private int retentionBatchSize = 1000;

    public int getLeaseSeconds() { return leaseSeconds; }
    public void setLeaseSeconds(int leaseSeconds) { this.leaseSeconds = leaseSeconds; }

    public int getHeartbeatIntervalSeconds() { return heartbeatIntervalSeconds; }
    public void setHeartbeatIntervalSeconds(int heartbeatIntervalSeconds) { this.heartbeatIntervalSeconds = heartbeatIntervalSeconds; }

    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }

    public int getBackoffBaseSeconds() { return backoffBaseSeconds; }
    public void setBackoffBaseSeconds(int backoffBaseSeconds) { this.backoffBaseSeconds = backoffBaseSeconds; }

    public int getBackoffCapSeconds() { return backoffCapSeconds; }
    public void setBackoffCapSeconds(int backoffCapSeconds) { this.backoffCapSeconds = backoffCapSeconds; }

    public int getClaimBatchSize() { return claimBatchSize; }
    public void setClaimBatchSize(int claimBatchSize) { this.claimBatchSize = claimBatchSize; }

    public int getReaperIntervalSeconds() { return reaperIntervalSeconds; }
    public void setReaperIntervalSeconds(int reaperIntervalSeconds) { this.reaperIntervalSeconds = reaperIntervalSeconds; }

    public int getReaperBatchSize() { return reaperBatchSize; }
    public void setReaperBatchSize(int reaperBatchSize) { this.reaperBatchSize = reaperBatchSize; }

    public int getMaxPayloadBytes() { return maxPayloadBytes; }
    public void setMaxPayloadBytes(int maxPayloadBytes) { this.maxPayloadBytes = maxPayloadBytes; }

    public int getMaxQueueDepth() { return maxQueueDepth; }
    public void setMaxQueueDepth(int maxQueueDepth) { this.maxQueueDepth = maxQueueDepth; }

    public int getWorkerPollIntervalMs() { return workerPollIntervalMs; }
    public void setWorkerPollIntervalMs(int workerPollIntervalMs) { this.workerPollIntervalMs = workerPollIntervalMs; }

    public int getRetentionDays() { return retentionDays; }
    public void setRetentionDays(int retentionDays) { this.retentionDays = retentionDays; }

    public int getRetentionBatchSize() { return retentionBatchSize; }
    public void setRetentionBatchSize(int retentionBatchSize) { this.retentionBatchSize = retentionBatchSize; }
}
