package io.basin.sdk;

/**
 * Raised when the transport layer fails before a server response is received —
 * connection refused, timeout, TLS error, etc.
 */
public class BasinNetworkException extends BasinException {
    public BasinNetworkException(String message, Throwable cause) {
        super(message, cause);
    }

    public BasinNetworkException(String message) {
        super(message);
    }
}
