package edu.pku.dblab.exceptions;

import java.util.Formatter;

/**
 * This class represnets exceptions that can use format string as input. This class
 * has not empty constructor.
 */
public abstract class FormattedException extends Exception {

    public FormattedException(Throwable cause) {
        super(cause);
    }

    public FormattedException(String formatString, Throwable cause, Object... parameters) {
        super(String.format(formatString, parameters), cause);
    }

    public FormattedException(String message, Object... parameters) {
        super(String.format(message, parameters));
    }
}
