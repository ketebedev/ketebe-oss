package io.ketebe;
public final class ApiException extends KetebeException {
    private final int statusCode;
    private final String code;
    public ApiException(int statusCode, String code, String message) { super(message); this.statusCode = statusCode; this.code = code; }
    public int statusCode() { return statusCode; }
    public String code() { return code; }
}
