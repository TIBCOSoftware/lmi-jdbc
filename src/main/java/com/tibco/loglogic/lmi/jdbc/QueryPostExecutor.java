/*
 * Copyright (c) 2014-2018 TIBCO Software Inc. All Rights Reserved.
 * Licensed under a BSD-type license. See TIBCO LICENSE.txt for license text.
 */
package com.tibco.loglogic.lmi.jdbc;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Task for starting the query on the query node. It should be executed on the connection's executor.
 */
class QueryPostExecutor
    implements Runnable {

    private final CountDownLatch latch;

    private final String query;

    private QueryMetadata metadata;

    private PostErrorResponse errorResponse;

    private final LmiConnection connection;

    /**
     * Request to create query send to query node.
     * <p>
     * The server will response with {@link QueryMetadata} or {@link PostErrorResponse}.
     */
    static private class CreateQueryRequest {

        @JsonProperty
        String query;

        @JsonProperty
        final boolean cached = false;

        @JsonProperty
        int timeToLive;

        public void setQuery( String query, int timeToLive ) {
            this.query = query;
            this.timeToLive = timeToLive;
        }
    }

    /**
     * Part of the {@link QueryMetadata}, describe particular column.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    static class ColumnDesc {

        @JsonProperty
        private String name;

        @JsonProperty
        private String type;

        @JsonCreator
        public ColumnDesc( @JsonProperty("name") String name, @JsonProperty("type") String type ) {
            this.name = name;
            this.type = type;
        }

        public String getName() {
            return this.name;
        }

        public void setName( String name ) {
            this.name = name;
        }

        public String getType() {
            return type;
        }

        public void setType( String type ) {
            this.type = type;
        }
    }

    /**
     * JSON object received as a response to {@link CreateQueryRequest}.
     * <p>
     * Provide the query ID and the query meta-data - list of columns with types.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    static class QueryMetadata {

        @JsonProperty
        private List<ColumnDesc> columns;

        @JsonProperty
        String queryId;

        public List<ColumnDesc> getColumns() {
            return columns;
        }

        public void setColumns( List<ColumnDesc> columns ) {
            this.columns = columns;
        }

        public String getQueryId() {
            return queryId;
        }

        public void setQueryId( String queryId ) {
            this.queryId = queryId;
        }
    }

    /**
     * Error response from the server.
     */
    static class PostErrorResponse {

        @JsonProperty
        private String id;

        @JsonProperty
        private String message;

        @JsonProperty
        private String details;

        public String getQueryId() {
            return id;
        }

        public void setQueryId( String queryId ) {
            this.id = queryId;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage( String message ) {
            this.message = message;
        }

        public String getDetails() {
            return details;
        }

        public void setDetails( String details ) {
            this.details = details;
        }

    }

    /**
     * The Constructor.
     *
     * @param connection to the server
     * @param query string
     */
    QueryPostExecutor( LmiConnection connection, String query ) {
        this.connection = connection;
        this.latch = new CountDownLatch( 1 );
        this.query = query;
    }

    /**
     * Gets json data for the query, which include query ID and the column meta-data.
     */
    private CloseableHttpResponse invokePostRequest()
        throws Exception {

        HttpPost postRequest = new HttpPost( connection.getUrl() );
        RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout( connection.getNetworkTimeout() )
            .setConnectTimeout( connection.getNetworkTimeout() ).build();
        postRequest.setConfig( requestConfig );
        postRequest.setHeader( "Accept", "application/json, text/plain " );
        postRequest.setHeader( "Content-Type", "application/json;charset=\"UTF-8\"" );

        CreateQueryRequest request = new CreateQueryRequest();
        request.setQuery( query, connection.getQueryTimeout() );

        String jsonPayload = connection.getObjectMapper().writeValueAsString( request );
        StringEntity entity = new StringEntity( jsonPayload );
        System.out.println( "postRequest.entity=" + jsonPayload );
        entity.setContentType( "application/json;charset=\"UTF-8\"" );
        postRequest.setEntity( entity );
        System.out.print( "Getting response..." );
        System.out.flush();
        return connection.getHttpClient().execute( postRequest );
    }

    @Override
    public void run() {
        ObjectMapper mapper = connection.getObjectMapper();

        try (CloseableHttpResponse response = invokePostRequest()) {
            // get the data from server

            final String json = EntityUtils.toString( response.getEntity() );

            if ( response.getStatusLine().getStatusCode() == 200 ) {
                // If the response is formatted to QueryMetadata instance
                QueryMetadata queryMetadata = mapper.readValue( json, QueryMetadata.class );
                if ( queryMetadata.getColumns() != null ) {
                    metadata = queryMetadata;
                }
                System.out
                    .println( "OK: id=" + queryMetadata.getQueryId() + ", columns=" + queryMetadata.columns.size() );
            }
            else if ( response.getStatusLine().getStatusCode() == 400 ) {
                System.out.println( "BAD:400" );
                // If the response is formatted to PostErrorResponse instance
                // Which generally indicates error in query
                try {
                    errorResponse = mapper.readValue( json, PostErrorResponse.class );
                }
                catch ( Exception exception ) {
                    // not even proper error
                    errorResponse = new PostErrorResponse();
                    errorResponse.setMessage( json );
                }
            }
            else {
                System.out.println( "BAD:" + response.getStatusLine().getStatusCode() + ":"
                    + response.getStatusLine().getReasonPhrase() );
                errorResponse = new PostErrorResponse();
                errorResponse.setMessage( "Could not connect to QueryNode: " + connection.getUrl() + ": "
                    + response.getStatusLine().toString() );
            }
        }
        catch ( Exception e ) {
            errorResponse = new PostErrorResponse();
            errorResponse.setMessage( e.getMessage() );
        }
        finally {
            // notify others we have done
            latch.countDown();
        }
    }

    /**
     * Valid response from the query, will be null if error occurred. See {@link #getErrorResponse()} for error details.
     * <p>
     * Blocking call, will wait until the response is received from the server.
     */
    QueryMetadata getMetadata()
        throws InterruptedException {

        // wait for the response
        await();

        return metadata;
    }

    /**
     * Error response from the post, null when it was successful.
     * <p>
     * Blocking call, will wait until the response is received from the server.
     */
    PostErrorResponse getErrorResponse()
        throws InterruptedException {

        // wait for the response
        await();

        return errorResponse;
    }

    private void await()
        throws InterruptedException {

        latch.await();
    }
}
