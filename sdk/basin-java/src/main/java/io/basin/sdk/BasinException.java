package io.basin.sdk;

/**
 * Base class for all Basin SDK exceptions.
 *
 * <p>Every non-2xx response from a Basin server raises {@link BasinApiException}.
 * Transport failures before a response is received raise {@link BasinNetworkException}.
 */
public class BasinException extends RuntimeException {
    public BasinException(String message) {
        super(message);
    }

    public BasinException(String message, Throwable cause) {
        super(message, cause);
    }
}
