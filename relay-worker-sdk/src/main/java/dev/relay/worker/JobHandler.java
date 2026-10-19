package dev.relay.worker;

@FunctionalInterface
public interface JobHandler {
    void handle(JobContext context) throws Exception;
}
