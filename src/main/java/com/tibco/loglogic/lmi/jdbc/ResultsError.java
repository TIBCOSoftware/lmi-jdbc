/*
 * Copyright (c) 2014-2018 TIBCO Software Inc. All Rights Reserved.
 * Licensed under a BSD-type license. See TIBCO LICENSE.txt for license text.
 */
package com.tibco.loglogic.lmi.jdbc;

/**
 * Class to hold the error response while trying to map the response string
 *
 */
class ResultsError {

    private StackTraceElement[] element;

    private String errorMessage;

    private Throwable cause;

    public StackTraceElement[] getElement() {
        return element;
    }

    public void setElement( StackTraceElement[] element ) {
        this.element = element;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage( String errorMessage ) {
        this.errorMessage = errorMessage;
    }

    public Throwable getThrowable() {
        return cause;
    }

    public void setThrowable( Throwable cause ) {
        this.cause = cause;
    }

}
