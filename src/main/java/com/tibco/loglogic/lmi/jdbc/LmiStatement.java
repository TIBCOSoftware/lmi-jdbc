/*
 * Copyright (c) 2014-2018 TIBCO Software Inc. All Rights Reserved.
 * Licensed under a BSD-type license. See TIBCO LICENSE.txt for license text.
 */
package com.tibco.loglogic.lmi.jdbc;

import static java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;

/**
 * JDBC Statement for Apollo.
 */
public class LmiStatement
    implements Statement {

    private final LmiConnection connection;

    private int batchSize;

    private int queryTimeoutMillis;

    private LmiResultSet resultSet;

    private boolean closed;

    private boolean closeOnCompletion;

    private final int cursorType;

    /**
     * The Constructor.
     *
     * @param connection Apollo connection
     */
    LmiStatement( LmiConnection connection, int resultSetType ) {
        if ( connection == null ) {
            throw new NullPointerException( "Apollo Connection Object is null" );
        }

        this.cursorType = resultSetType;
        this.connection = connection;
        this.batchSize = connection.getBatchSize();
        this.queryTimeoutMillis = connection.getNetworkTimeout();
    }

    @Override
    public ResultSet executeQuery( String sql )
        throws SQLException {
        try {
            // close current result set, if any
            if ( this.resultSet != null ) {
                this.resultSet.close();
            }

            // execute query

            /*
             * // TODO remove workarounds Pattern pattern = Pattern.compile( "\\s+FROM\\s+[^\\s]+\\s([^\\s]+)\\s+" );
             * Matcher matcher = pattern.matcher( sql ); String tableAlias = null; if ( matcher.find() ) { tableAlias =
             * matcher.group( 1 ); }
             * 
             * if ( tableAlias != null ) { sql = sql.replace( tableAlias + ".", "" ); sql = sql.replace( tableAlias, ""
             * ); }
             * 
             * pattern = Pattern .compile(
             * "sys_eventTime\\s+>=\\s+('\\d\\d\\d\\d-\\d\\d-\\d\\d \\d\\d:\\d\\d:\\d\\d')\\s+AND\\s+" +
             * "sys_eventTime\\s+<=\\s+('\\d\\d\\d\\d-\\d\\d-\\d\\d \\d\\d:\\d\\d:\\d\\d')" ); matcher =
             * pattern.matcher( sql ); while ( matcher.find() ) { sql = sql.substring( 0, matcher.start( 0 ) ) +
             * "sys_eventTime IN " + matcher.group( 1 ) + ":" + matcher.group( 2 ) + "" + sql.substring( matcher.end()
             * ); matcher = pattern.matcher( sql ); }
             */

            System.out.println( "EXECUTING:" + sql );

            LmiConnection conn = (LmiConnection) getConnection();

            QueryPostExecutor command = new QueryPostExecutor( conn, sql );
            conn.getExecutor().execute( command );

            if ( command.getErrorResponse() != null ) {
                throw new SQLException( command.getErrorResponse().getMessage() );
            }

            resultSet = new LmiResultSet( this, command.getMetadata(), batchSize, queryTimeoutMillis );

            return resultSet;
        }
        catch ( SQLException e ) {
            throw e;
        }
        catch ( Exception e ) {
            throw new SQLException( "Error executing query", e );
        }
    }

    @Override
    public <T> T unwrap( Class<T> iface )
        throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor( Class<?> iface )
        throws SQLException {
        return false;
    }

    @Override
    public int executeUpdate( String sql )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY DATABASE" );
    }

    @Override
    public void close()
        throws SQLException {

        closed = true;

        if ( connection.closed ) {
            return;
        }

        if ( resultSet != null && !resultSet.closed ) {
            resultSet.close();
            resultSet = null;
        }
    }

    @Override
    public int getMaxFieldSize()
        throws SQLException {
        return 0; // no limit
    }

    @Override
    public void setMaxFieldSize( int max )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "Setting max field size is not supported." );
    }

    @Override
    public int getMaxRows()
        throws SQLException {
        return batchSize;
    }

    @Override
    public void setMaxRows( int max )
        throws SQLException {
        batchSize = max;
    }

    @Override
    public void setEscapeProcessing( boolean enable )
        throws SQLException {
        // ignore
    }

    @Override
    public int getQueryTimeout() {
        return queryTimeoutMillis / 1000;
    }

    @Override
    public void setQueryTimeout( int seconds ) {
        this.queryTimeoutMillis = seconds * 1000;
    }

    @Override
    public void cancel()
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "Canceling statement" );
    }

    @Override
    public SQLWarning getWarnings()
        throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings()
        throws SQLException {
    }

    @Override
    public void setCursorName( String name )
        throws SQLException {
        // If the database does not support positioned update/delete, this method is a noop
    }

    @Override
    public boolean execute( String sql )
        throws SQLException {

        // assume this is a SELECT query
        executeQuery( sql );
        return true;
    }

    @Override
    public ResultSet getResultSet()
        throws SQLException {
        return resultSet;
    }

    @Override
    public int getUpdateCount()
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY DATABASE" );
    }

    @Override
    public boolean getMoreResults()
        throws SQLException {
        return false;
    }

    @Override
    public void setFetchDirection( int direction )
        throws SQLException {
        if ( direction != ResultSet.FETCH_FORWARD ) {
            throw new SQLFeatureNotSupportedException();
        }
    }

    @Override
    public int getFetchDirection()
        throws SQLException {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize( int rows )
        throws SQLException {
    }

    @Override
    public int getFetchSize()
        throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetConcurrency()
        throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType()
        throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public void addBatch( String sql )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "Batching not supported" );
    }

    @Override
    public void clearBatch()
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "Batching not supported" );
    }

    @Override
    public int[] executeBatch()
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "Batching not supported" );
    }

    @Override
    public Connection getConnection()
        throws SQLException {
        if ( this.connection.isClosed() ) {
            throw new SQLException( "Connection is closed" );
        }
        return this.connection;
    }

    @Override
    public boolean getMoreResults( int current )
        throws SQLException {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys()
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY DATABASE" );
    }

    @Override
    public int executeUpdate( String sql, int autoGeneratedKeys )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY DATABASE" );
    }

    @Override
    public int executeUpdate( String sql, int[] columnIndexes )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY DATABASE" );
    }

    @Override
    public int executeUpdate( String sql, String[] columnNames )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY DATABASE" );
    }

    @Override
    public boolean execute( String sql, int autoGeneratedKeys )
        throws SQLException {
        return execute( sql );
    }

    @Override
    public boolean execute( String sql, int[] columnIndexes )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY DATABASE" );
    }

    @Override
    public boolean execute( String sql, String[] columnNames )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY DATABASE" );
    }

    @Override
    public int getResultSetHoldability()
        throws SQLException {
        return HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public boolean isClosed()
        throws SQLException {
        return closed;
    }

    @Override
    public void setPoolable( boolean poolable )
        throws SQLException {
    }

    @Override
    public boolean isPoolable()
        throws SQLException {
        return false;
    }

    @Override
    public void closeOnCompletion()
        throws SQLException {
        closeOnCompletion = true;
    }

    @Override
    public boolean isCloseOnCompletion()
        throws SQLException {
        return closeOnCompletion;
    }
}
