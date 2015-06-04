package edu.pku.dblab.exceptions;

public class GenericException extends FormattedException {

    public GenericException(String message, Object... parameters) {
        super(message, parameters);
    }

    public GenericException(String formatString, Throwable cause, Object... parameters) {
        super(formatString, cause, parameters);
    }

    public GenericException(Throwable cause) {
        super(cause);
    }

}
