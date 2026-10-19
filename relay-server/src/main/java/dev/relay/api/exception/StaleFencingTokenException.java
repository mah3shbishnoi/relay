package dev.relay.api.exception;

public class StaleFencingTokenException extends RuntimeException {
    public StaleFencingTokenException(String message) {
        super(message);
    }
}
