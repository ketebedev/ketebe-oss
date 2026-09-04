package io.ketebe;
public final class TransportException extends KetebeException {
    public TransportException(String message) { super(message); }
    public TransportException(String message, Throwable cause) { super(message, cause); }
}
