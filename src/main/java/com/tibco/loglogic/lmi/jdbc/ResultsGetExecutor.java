/*
 * Copyright (c) 2014-2018 TIBCO Software Inc. All Rights Reserved.
 * Licensed under a BSD-type license. See TIBCO LICENSE.txt for license text.
 */
package com.tibco.loglogic.lmi.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Task to obtain next batch of values for result set. It should be executed on the connection's executor.
 */
class ResultsGetExecutor
    implements Runnable {

    private volatile boolean eofReached;

    private List<List<String>> buffer;

    private final String queryId;

    private final int batchSize;

    private final CountDownLatch latch;

    private final LmiConnection connection;

    private final LmiStatement statement;

    private final LmiResultSet resultSet;

    private ResultsError resultsError;


    @JsonIgnoreProperties(ignoreUnknown = true)
    static class ErrorOrWarning {
        private String text;
        private String severity;

        public String getText() {
            return text;
        }

        public void setText(String text) {
            this.text = text;
        }

        public String getSeverity() {
            return severity;
        }

        public void setSeverity(String severity) {
            this.severity = severity;
        }
    }

    /**
     * JSON object with a batch of the values for columns.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    static class QueryResults {

        private int offset;

        private List<List<String>> rows;

        private List<ErrorOrWarning> errorsOrWarnings;

        private int progress;

        private int timeSpent;

        private boolean hasMore;

        public int getOffset() {
            return offset;
        }

        public void setOffset( int offset ) {
            this.offset = offset;
        }

        public List<List<String>> getRows() {
            return rows;
        }

        public void setRows( List<List<String>> rows ) {
            this.rows = rows;
        }

        public int getProgress() {
            return progress;
        }

        public void setProgress( int progress ) {
            this.progress = progress;
        }

        public int getTimeSpent() {
            return timeSpent;
        }

        public void setTimeSpent( int timeSpent ) {
            this.timeSpent = timeSpent;
        }

        public boolean isHasMore() {
            return hasMore;
        }

        public void setHasMore( boolean hasMore ) {
            this.hasMore = hasMore;
        }

        public List<ErrorOrWarning> getErrorsOrWarnings() {
            return errorsOrWarnings;
        }

        public void setErrorOrWarnings(List<ErrorOrWarning> errorOrWarnings) {
            this.errorsOrWarnings = errorsOrWarnings;
        }
    }

    public ResultsGetExecutor(LmiConnection connection, LmiStatement lmiStatement, LmiResultSet rs, String queryId, int batchSize ) {
        this.connection = connection;
        this.eofReached = false;
        this.queryId = queryId;
        this.batchSize = batchSize;
        this.latch = new CountDownLatch( 1 );
        this.statement = lmiStatement;
        this.resultSet = rs;
    }

    /**
     * Values for the next batch, blocking call.
     */
    List<List<String>> getBuffer()
        throws InterruptedException {

        // wait for the response
        await();
        if ( resultsError == null ) {
            return buffer;
        }
        else {
            return null;
        }
    }

    private void await()
        throws InterruptedException {

        latch.await();
    }

    @Override
    public void run() {

        Thread.currentThread().getThreadGroup().getParent();

        QueryResults queryResults = null;

        try {

            HttpGet getRequest = new HttpGet( connection.getUrl() + "/" + queryId + "/results" + "?size=" + batchSize
                + "&longPollTimeout=" + connection.getPollingPeriod() );
            System.out.println( "getRequest.uri=" + getRequest.getURI() );
            // the socket timeout needs to be longer than the request timeout to give the server time to respond
            RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout( connection.getNetworkTimeout() )
                .setConnectTimeout( connection.getNetworkTimeout() ).build();
            getRequest.setConfig( requestConfig );

            ObjectMapper mapper = connection.getObjectMapper();

            int timeLeft = connection.getPollingTimeout() * 1000;
            while ( true ) {
                if ( timeLeft <= 0 ) {
                    throw new SQLException( "Timeout while polling for the results" );
                }

                System.out.print( "Getting results..." );
                System.out.flush();
                try ( CloseableHttpResponse response = connection.getHttpClient().execute( getRequest )) {
                    if (response.getStatusLine().getStatusCode() == 200) {
                        // successful response
                        String responseString = EntityUtils.toString(response.getEntity());
                        queryResults = mapper.readValue(responseString, QueryResults.class);
                        System.out
                                .println("OK, rows=" + queryResults.getRows().size() + ", more=" + queryResults.isHasMore());
                        if (queryResults.getRows().size() == 0 && queryResults.isHasMore()) {
                            // the query has not finished yet after the waiting time, retry
                            timeLeft -= connection.getPollingPeriod();
                            System.out.println("Retry, left: " + timeLeft + "ms");
                            continue;
                        }
                        buffer = queryResults.getRows();
                        for ( ErrorOrWarning errorOrWarning : queryResults.errorsOrWarnings ) {
                            if ( errorOrWarning.getSeverity().equals("WARNING")) {
                                resultSet.addWarning( errorOrWarning.getText() );
                            }

                        }
                        break;
                    } else {
                        String errorMessage = "Bad response from querynode: " + response.getStatusLine().getStatusCode() + ": "
                                + response.getStatusLine().getReasonPhrase();
                        // unsuccessful response
                        resultsError = new ResultsError();
                        resultsError.setErrorMessage(errorMessage);
                        break;
                    }
                }
            }
        }
        catch ( Exception e ) {
            resultsError = new ResultsError();
            resultsError.setErrorMessage( e.getMessage() );
            resultsError.setThrowable( e.getCause() );
            resultsError.setElement( e.getStackTrace() );
        }
        finally {
            // Added a null check in case of unsuccessful response
            if ( queryResults != null ) {
                if ( !queryResults.isHasMore() ) {
                    eofReached = true;
                }
            }
            latch.countDown();
        }
    }

    /**
     * Check if the EOF has been reached, blocking call.
     */
    boolean isEofReached()
        throws InterruptedException {

        // make sure the response is back
        await();

        return eofReached;
    }

    ResultsError getResultsError() {
        return resultsError;
    }

}
