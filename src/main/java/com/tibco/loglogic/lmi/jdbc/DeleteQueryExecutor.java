/*
 * Copyright (c) 2014-2018 TIBCO Software Inc. All Rights Reserved.
 * Licensed under a BSD-type license. See TIBCO LICENSE.txt for license text.
 */
package com.tibco.loglogic.lmi.jdbc;

import java.util.concurrent.CountDownLatch;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.impl.client.CloseableHttpClient;

/**
 * Task for starting the query on the query node. It should be executed on the connection's executor.
 */
class DeleteQueryExecutor
    implements Runnable {

    private final CountDownLatch latch;

    private final String queryId;

    private final LmiConnection connection;

    private ErrorDetails errorResponse;

    /**
     * The Constructor.
     *
     * @param queryId ID of the query
     */
    DeleteQueryExecutor( LmiConnection connection, String queryId ) {
        this.connection = connection;
        this.queryId = queryId;

        // create latch
        this.latch = new CountDownLatch( 1 );
    }

    /**
     * Tells the server to delete the query.
     *
     * @param queryId the Id of the query to be deleted
     */
    private void invokePostRequest( String queryId )
        throws Exception {

        HttpDelete deleteRequest = new HttpDelete( connection.getUrl() + "/" + queryId );
        deleteRequest.setHeader( "Accept", "application/json, text/plain" );
        deleteRequest.setHeader( "Content-Type", "application/json;charset=\"UTF-8\"" );

        CloseableHttpClient httpClient = connection.getHttpClient();

        CloseableHttpResponse closeableHttpResponse = httpClient.execute( deleteRequest );
        closeableHttpResponse.close();
    }

    @Override
    public void run() {
        try {
            invokePostRequest( queryId );
        }
        catch ( Exception e ) {
            errorResponse = new ErrorDetails();
            errorResponse.setMessage( e.getMessage() );
        }
        finally {
            // notify others we have done
            latch.countDown();
        }
    }

    ErrorDetails getErrorResponse()
        throws InterruptedException {

        // make sure that the response is back
        latch.await();

        return errorResponse;
    }
}
