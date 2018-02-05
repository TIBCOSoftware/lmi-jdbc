/*
 * Copyright (c) 2014-2018 TIBCO Software Inc. All Rights Reserved.
 * Licensed under a BSD-type license. See TIBCO LICENSE.txt for license text.
 */
package com.tibco.loglogic.lmi.jdbc;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Error response from the server.
 */
class ErrorDetails {

    @JsonProperty
    private String id;

    @JsonProperty
    private String message;

    @JsonProperty
    private String details;

    @SuppressWarnings("unused")
    public String getQueryId() {
        return this.id;
    }

    @SuppressWarnings("unused")
    public void setQueryId( String queryId ) {
        this.id = queryId;
    }

    @SuppressWarnings("WeakerAccess")
    public String getMessage() {
        return this.message;
    }

    @SuppressWarnings("WeakerAccess")
    public void setMessage( String message ) {
        this.message = message;
    }

    @SuppressWarnings("unused")
    public String getDetails() {
        return this.details;
    }

    @SuppressWarnings("unused")
    public void setDetails( String details ) {
        this.details = details;
    }

}
