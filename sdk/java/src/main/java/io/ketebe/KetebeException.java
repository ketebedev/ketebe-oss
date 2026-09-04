package io.ketebe;
public class KetebeException extends RuntimeException {
    public KetebeException(String message) { super(message); }
    public KetebeException(String message, Throwable cause) { super(message, cause); }
}
