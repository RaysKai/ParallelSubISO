package edu.pku.dblab.exceptions;

/*
 * Copyright (C) 2014 Gai Lei.
 *
 */
public class WrongParameterException extends GenericException {

    public WrongParameterException(Throwable cause) {
        super(cause);
    }

    public WrongParameterException(String message, Throwable cause, Object... args) {
        super(message, cause, args);
    }

    public WrongParameterException(String message, Object... args) {
        super(message, args);
    }
}
