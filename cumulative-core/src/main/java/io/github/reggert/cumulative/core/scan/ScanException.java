package io.github.reggert.cumulative.core.scan;

import javax.annotation.Nonnull;


/**
 * Unchecked exception to wrap checked Accumulo exceptions thrown during scans.
 */
public class ScanException extends RuntimeException {
    private static final long serialVersionUID = 1L;


    /**
     * Constructs an exception with the specified message and cause.
     *
     * @param message
     * exception description.
     * @param cause
     * checked exception that caused the error.
     */
    public ScanException(@Nonnull final String message, @Nonnull final Exception cause) {
        super(message, cause);
    }
}
