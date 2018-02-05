/*
 * Copyright (c) 2014-2018 TIBCO Software Inc. All Rights Reserved.
 * Licensed under a BSD-type license. See TIBCO LICENSE.txt for license text.
 */
package com.tibco.loglogic.lmi.jdbc;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tibco.loglogic.lmi.jdbc.QueryPostExecutor.ColumnDesc;
import com.tibco.loglogic.lmi.jdbc.QueryPostExecutor.QueryMetadata;

/**
 * Apollo ResultSet.
 */
public class LmiResultSet
    implements ResultSet {

    private final Map<String, Integer> fieldMap;

    private final ResultSetMetaData resultSetMetaData;

    private final LmiStatement statement;

    private int batchSize;

    private final int queryTimeoutMillis;

    private List<String> currentRow = null;

    private QueryMetadata metadata;

    private int bufferIndex = -1;

    private List<List<String>> currentBuffer;

    private ResultsGetExecutor resultsGetExecutor;

    boolean closed = false;

    private boolean wasNull = false;

    private final boolean staticContent;

    private SQLWarning sqlWarning = null;

    // This is set when the server tells us the query is finished and stops us making another request. Also, the server
    // will delete any query that has completed, so if this field is 'true' there is no need to delete the query on
    // close.
    private boolean eofReached = false;

    LmiResultSet( LmiStatement statement, QueryMetadata metadata, int batchSize, int queryTimeoutMillis )
        throws SQLException {

        this.statement = statement;
        this.metadata = metadata;
        this.batchSize = batchSize;
        this.queryTimeoutMillis = queryTimeoutMillis;

        this.fieldMap = getFieldMap( metadata.getColumns() );
        this.resultSetMetaData = new LmiResultSetMetaData( metadata );

        this.resultsGetExecutor = submitRequestToServer();
        this.staticContent = false;
    }

    LmiResultSet( ColumnDesc[] columns, String[][] values )
        throws SQLException {

        this.metadata = new QueryMetadata();
        metadata.setColumns( Arrays.asList( columns ) );
        currentBuffer = new ArrayList<>();
        for ( String[] valuesInRow : values ) {
            currentBuffer.add( Arrays.asList( valuesInRow ) );
        }
        this.batchSize = 0;
        this.statement = null;
        this.queryTimeoutMillis = 0;
        this.fieldMap = getFieldMap( metadata.getColumns() );
        this.resultSetMetaData = new LmiResultSetMetaData( metadata );

        this.resultsGetExecutor = null;
        this.staticContent = true;
    }

    private static Map<String, Integer> getFieldMap( List<ColumnDesc> columns ) {
        Map<String, Integer> map = new HashMap<>( columns.size() );
        for ( int i = 0; i < columns.size(); i++ ) {
            String name = columns.get( i ).getName().toLowerCase();
            if ( !map.containsKey( name ) ) {
                map.put( name, i + 1 );
            }
        }
        return map;
    }

    private ResultsGetExecutor submitRequestToServer()
        throws SQLException {

        LmiConnection connection = (LmiConnection) statement.getConnection();

        ResultsGetExecutor resultsGetExecutor = new ResultsGetExecutor( connection, statement, this,
                                                                        metadata.getQueryId(), batchSize );

        connection.getExecutor().execute( resultsGetExecutor );
        return resultsGetExecutor;
    }

    @Override
    public boolean next()
        throws SQLException {
        checkClosed();
        try {

            if ( bufferIndex != -1 && bufferIndex < currentBuffer.size() ) {

                // still processing the current buffer
                currentRow = currentBuffer.get( bufferIndex );
                // submit a request for the next resultsGetExecutor while processing this one
                if ( !eofReached && resultsGetExecutor == null ) {
                    resultsGetExecutor = submitRequestToServer();
                }

                bufferIndex++;
                return true;
            }
            else if ( !eofReached ) {

                if ( staticContent ) {
                    eofReached = true;
                    bufferIndex++;
                    if ( currentBuffer.size() > 0 ) {
                        currentRow = currentBuffer.get( 0 );
                        bufferIndex = 1;
                        return true;
                    }
                    else
                        return false;
                }

                if ( resultsGetExecutor == null ) {
                    // in case the previous execution threw an error
                    resultsGetExecutor = submitRequestToServer();
                }

                // this will block until we receive response from server
                List<List<String>> buffer = resultsGetExecutor.getBuffer();

                if ( buffer == null && resultsGetExecutor.getResultsError() != null ) {
                    final String errorMessage = resultsGetExecutor.getResultsError().getErrorMessage();
                    resultsGetExecutor = null;
                    throw new SQLException( errorMessage );
                }

                if ( resultsGetExecutor.isEofReached() ) {
                    eofReached = true;
                }

                // reset the task
                resultsGetExecutor = null;

                final boolean next = switchBuffers( buffer );

                bufferIndex++;

                return next;

            }
            else {
                return false;
            }
        }
        catch ( Exception e ) {
            // propagateIfInstanceOf(e, SQLException.class);
            throw new SQLException( "Error fetching results", e );
        }
    }

    private boolean switchBuffers( List<List<String>> buffer ) {

        if ( buffer != null && buffer.size() > 0 ) {
            currentBuffer = buffer;
            bufferIndex = 0;
            currentRow = buffer.get( this.bufferIndex );
            return true;
        }
        else {
            // If we reached this stage it means that the request to the
            // query node for the next resultsGetExecutor of results returned null
            // and it may signify that the query node does not have any
            // results to return . In that case return false
            return false;
        }
    }

    private Object column( int index )
        throws SQLException {
        checkClosed();
        checkValidRow();
        if ( ( index <= 0 ) || ( index > resultSetMetaData.getColumnCount() ) ) {
            throw new SQLException( "Invalid column index: " + index );
        }
        return currentRow.get( index - 1 );
    }

    private void checkValidRow()
        throws SQLException {
        if ( currentRow == null ) {
            throw new SQLException( "Not on a valid row" );
        }
    }

    /**
     * @throws SQLException when the ResultSet is closed.
     */
    private void checkClosed()
        throws SQLException {
        if ( closed ) {
            throw new SQLException( "Result Set is closed" );
        }
    }

    @Override
    public void close()
        throws SQLException {

        System.out.println( "ResultSet.close" );

        if ( closed ) {
            System.out.println( "Result set was closed already" );
            return;
        }

        closed = true;

        if ( staticContent )
            return;

        LmiConnection conn = (LmiConnection) getStatement().getConnection();
        DeleteQueryExecutor closeCmd = new DeleteQueryExecutor( conn, metadata.getQueryId() );
        conn.getExecutor().execute( closeCmd );

        // wait for the query to result
        ErrorDetails errorResponse;
        try {
            errorResponse = closeCmd.getErrorResponse();
        }
        catch ( InterruptedException e ) {
            throw new SQLException( "Result Set closing is interrupted.", e );
        }

        if ( errorResponse != null ) {
            throw new SQLException( errorResponse.getMessage() );
        }

        System.out.println( "ResultSet is now closed" );
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal( int columnIndex, int scale )
        throws SQLException {
        return new BigDecimal( new BigInteger( getString( columnIndex ) ), scale );
    }

    private byte[] getBytesFromString( String str ) {
        String[] bytesStr = str.replaceAll( "\\[", "" ).replaceAll( "\\]", "" ).split( "," );

        byte[] ret = new byte[bytesStr.length];

        for ( int i = 0; i < bytesStr.length; i++ ) {
            try {
                ret[i] = Byte.parseByte( bytesStr[i].trim() );
            }
            catch ( NumberFormatException ignored ) {
            }
        }

        return ret;
    }

    @Override
    public byte[] getBytes( int columnIndex )
        throws SQLException {
        Object value = column( columnIndex );
        if ( value == null ) {
            wasNull = true;
            return null;
        }
        else {
            wasNull = false;
        }

        if ( value instanceof String ) {
            return getBytesFromString( (String) value );
        }
        else {
            throw new SQLException( "Unexpected type for getBytes: " + value.getClass() );
        }
    }

    @Override
    public Date getDate( int columnIndex )
        throws SQLException {
        Timestamp timestamp = getTimestamp( columnIndex );
        if ( wasNull )
            return null;
        return new Date( timestamp.getTime() );
    }

    private int columnIndex( String label )
        throws SQLException {
        if ( label == null ) {
            throw new SQLException( "Column label is null" );
        }
        Integer index = fieldMap.get( label.toLowerCase() );
        if ( index == null ) {
            throw new SQLException( "Invalid column label: " + label );
        }
        return index;
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
    public boolean wasNull()
        throws SQLException {
        return wasNull;
    }

    @Override
    public Time getTime( int columnIndex )
        throws SQLException {
        Timestamp timestamp = getTimestamp( columnIndex );
        if ( wasNull )
            return null;
        return new Time( timestamp.getTime() );
    }

    @Override
    public Timestamp getTimestamp( int columnIndex )
        throws SQLException {

        Object value = column( columnIndex );

        if ( value == null ) {
            wasNull = true;
            return new Timestamp( 0 );
        }
        wasNull = false;
        if ( value instanceof Long )
            return new Timestamp( (Long) value );
        if ( value instanceof String )
            return new Timestamp( Long.parseLong( value.toString() ) );

        throw new SQLException( "Unexpected type for timestamp : " + value.getClass() );
    }

    @Override
    public InputStream getAsciiStream( int columnIndex )
        throws SQLException {
        String str = getString( columnIndex );
        if ( wasNull )
            return null;
        return new ByteArrayInputStream( str.getBytes( StandardCharsets.US_ASCII ) );
    }

    @Override
    @Deprecated
    public InputStream getUnicodeStream( int columnIndex )
        throws SQLException {
        String str = getString( columnIndex );
        if ( wasNull )
            return null;
        return new ByteArrayInputStream( str.getBytes( StandardCharsets.UTF_16LE ) );
    }

    @Override
    public InputStream getBinaryStream( int columnIndex )
        throws SQLException {
        byte[] bytes = getBytes( columnIndex );
        if ( wasNull )
            return null;
        return new ByteArrayInputStream( bytes );
    }

    @Override
    public String getString( int columnIndex )
        throws SQLException {
        Object value = column( columnIndex );
        if ( value == null ) {
            wasNull = true;
            return null;
        }
        wasNull = false;
        return value.toString();
    }

    @Override
    public boolean getBoolean( int columnIndex )
        throws SQLException {
        Object value = column( columnIndex );
        if ( value == null ) {
            wasNull = true;
            return false;
        }
        wasNull = false;
        if ( value instanceof Boolean )
            return (Boolean) value;
        if ( value instanceof String )
            return ( (String) value ).equalsIgnoreCase( "true" );
        throw new SQLException( "Unexpected type for boolean: " + value.getClass() );
    }

    @Override
    public byte getByte( int columnIndex )
        throws SQLException {
        Object value = column( columnIndex );
        if ( value == null ) {
            wasNull = true;
            return 0;
        }
        wasNull = false;
        return ( (Number) value ).byteValue();
    }

    @Override
    public short getShort( int columnIndex )
        throws SQLException {
        Object value = column( columnIndex );
        if ( value == null ) {
            wasNull = true;
            return 0;
        }
        wasNull = false;
        return ( (Number) value ).shortValue();
    }

    @Override
    public int getInt( int columnIndex )
        throws SQLException {
        Object value = column( columnIndex );
        if ( value == null ) {
            wasNull = true;
            return 0;
        }
        wasNull = false;
        if ( value instanceof Number )
            return ( (Number) value ).intValue();
        if ( value instanceof String ) {
            try {
                return Integer.parseInt( (String) value );
            }
            catch ( NumberFormatException nfe ) {
                throw new SQLException( nfe );
            }
        }
        throw new SQLException( "Unexpected object type for an INT " + value.getClass() );
    }

    @Override
    public long getLong( int columnIndex )
        throws SQLException {
        Object value = column( columnIndex );
        if ( value == null ) {
            wasNull = true;
            return 0;
        }
        wasNull = false;
        if ( value instanceof Number )
            return ( (Number) value ).longValue();
        if ( value instanceof String ) {
            try {
                return Long.parseLong( (String) value );
            }
            catch ( NumberFormatException nfe ) {
                throw new SQLException( nfe );
            }
        }
        throw new SQLException( "Unexpected object type for an LONG " + value.getClass() );
    }

    @Override
    public float getFloat( int columnIndex )
        throws SQLException {
        Object value = column( columnIndex );
        if ( value == null ) {
            wasNull = true;
            return 0;
        }
        wasNull = false;
        return ( (Number) value ).floatValue();
    }

    @Override
    public double getDouble( int columnIndex )
        throws SQLException {
        Object value = column( columnIndex );
        if ( value == null ) {
            wasNull = true;
            return 0;
        }
        wasNull = false;
        if ( value instanceof Number )
            return ( (Number) value ).doubleValue();
        if ( value instanceof String ) {
            try {
                return Double.parseDouble( (String) value );
            }
            catch ( NumberFormatException nfe ) {
                throw new SQLException( nfe );
            }
        }
        throw new SQLException( "Unexpected object type for an DOUBLE " + value.getClass() );
    }

    @Override
    public long getLong( String columnLabel )
        throws SQLException {
        return getLong( columnIndex( columnLabel ) );
    }

    @Override
    public float getFloat( String columnLabel )
        throws SQLException {
        return getFloat( columnIndex( columnLabel ) );
    }

    @Override
    public double getDouble( String columnLabel )
        throws SQLException {
        return getDouble( columnIndex( columnLabel ) );
    }

    @Override
    @Deprecated
    public BigDecimal getBigDecimal( String columnLabel, int scale )
        throws SQLException {
        // noinspection deprecation
        return getBigDecimal( columnIndex( columnLabel ), scale );
    }

    @Override
    public byte[] getBytes( String columnLabel )
        throws SQLException {

        return getBytes( columnIndex( columnLabel ) );
    }

    @Override
    public Date getDate( String columnLabel )
        throws SQLException {
        return getDate( columnIndex( columnLabel ) );
    }

    @Override
    public Time getTime( String columnLabel )
        throws SQLException {
        return getTime( columnIndex( columnLabel ) );
    }

    @Override
    public Timestamp getTimestamp( String columnLabel )
        throws SQLException {
        return getTimestamp( columnIndex( columnLabel ) );
    }

    @Override
    public InputStream getAsciiStream( String columnLabel )
        throws SQLException {
        return getAsciiStream( columnIndex( columnLabel ) );
    }

    @Override
    @Deprecated
    public InputStream getUnicodeStream( String columnLabel )
        throws SQLException {
        // noinspection deprecation
        return getUnicodeStream( columnIndex( columnLabel ) );
    }

    @Override
    public InputStream getBinaryStream( String columnLabel )
        throws SQLException {
        return getBinaryStream( columnIndex( columnLabel ) );
    }

    @Override
    public SQLWarning getWarnings()
        throws SQLException {
        return sqlWarning;
    }

    @Override
    public void clearWarnings()
        throws SQLException {
        sqlWarning = null;
    }

    @Override
    public String getCursorName()
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "cursors not supported" );
    }

    @Override
    public ResultSetMetaData getMetaData()
        throws SQLException {
        return this.resultSetMetaData;
    }

    @Override
    public Object getObject( int columnIndex )
        throws SQLException {
        switch ( LmiResultSetMetaData.getSQLTypeId( metadata.getColumns().get( columnIndex ).getType() ) ) {
            case Types.VARCHAR:
                return getString( columnIndex );
            case Types.TIMESTAMP:
                return getDate( columnIndex );
            case Types.INTEGER:
                return getInt( columnIndex );
            case Types.BIGINT:
                return getBigDecimal( columnIndex );
            case Types.DOUBLE:
                return getDouble( columnIndex );
            case Types.BOOLEAN:
                return getBoolean( columnIndex );
            default:
                throw new SQLException( "column type not supported" );
        }
    }

    @Override
    public Object getObject( String columnLabel )
        throws SQLException {
        return getObject( columnIndex( columnLabel ) );
    }

    @Override
    public int findColumn( String columnLabel )
        throws SQLException {
        return columnIndex( columnLabel );
    }

    @Override
    public Date getDate( int columnIndex, Calendar cal )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "getDate with calendar not supported");
    }

    @Override
    public Date getDate( String columnLabel, Calendar cal )
        throws SQLException {
        return getDate( columnIndex( columnLabel ), cal );
    }

    @Override
    public Time getTime( int columnIndex, Calendar cal )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "getTime with calendar not supported");
    }

    @Override
    public Time getTime( String columnLabel, Calendar cal )
        throws SQLException {
        return getTime( columnIndex( columnLabel ), cal );
    }

    @Override
    public Timestamp getTimestamp( int columnIndex, Calendar cal )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "getTimestamp with calendar not supported");
    }

    @Override
    public Timestamp getTimestamp( String columnLabel, Calendar cal )
        throws SQLException {
        return getTimestamp( columnIndex( columnLabel ), cal );
    }

    @Override
    public URL getURL( int columnIndex )
        throws SQLException {
        try {
            return new URL( getString( columnIndex ) );
        }
        catch ( MalformedURLException e ) {
            throw new SQLException( e );
        }
    }

    @Override
    public URL getURL( String columnLabel )
        throws SQLException {
        return getURL( columnIndex( columnLabel ) );
    }

    @Override
    public void updateRef( int columnIndex, Ref x )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateRef( String columnLabel, Ref x )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );

    }

    @Override
    public void updateBlob( int columnIndex, Blob x )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateBlob( String columnLabel, Blob x )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateClob( int columnIndex, Clob x )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateClob( String columnLabel, Clob x )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateArray( int columnIndex, Array x )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateArray( String columnLabel, Array x )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public RowId getRowId( int columnIndex )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public RowId getRowId( String columnLabel )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateRowId( int columnIndex, RowId x )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateRowId( String columnLabel, RowId x )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public int getHoldability()
        throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public boolean isClosed()
        throws SQLException {
        System.out.println( "ResultSet.closed?" );
        return closed;
    }

    @Override
    public void updateNString( int columnIndex, String nString )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateNString( String columnLabel, String nString )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateNClob( int columnIndex, NClob nClob )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateNClob( String columnLabel, NClob nClob )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public NClob getNClob( int columnIndex )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "CLOB not supported" );
    }

    @Override
    public NClob getNClob( String columnLabel )
        throws SQLException {
        return getNClob( columnIndex( columnLabel ) );
    }

    @Override
    public SQLXML getSQLXML( int columnIndex )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "SQLXML not supported" );
    }

    @Override
    public SQLXML getSQLXML( String columnLabel )
        throws SQLException {
        return getSQLXML( columnIndex( columnLabel ) );
    }

    @Override
    public void updateSQLXML( int columnIndex, SQLXML xmlObject )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateSQLXML( String columnLabel, SQLXML xmlObject )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public String getNString( int columnIndex )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "NString" );
    }

    @Override
    public String getNString( String columnLabel )
        throws SQLException {
        return getNString( columnIndex( columnLabel ) );
    }

    @Override
    public Reader getNCharacterStream( int columnIndex )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "NCharacterStream" );
    }

    @Override
    public Reader getNCharacterStream( String columnLabel )
        throws SQLException {
        return getNCharacterStream( columnIndex( columnLabel ) );
    }

    @Override
    public void updateNCharacterStream( int columnIndex, Reader x, long length )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateNCharacterStream( String columnLabel, Reader reader, long length )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateAsciiStream( int columnIndex, InputStream x, long length )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateBinaryStream( int columnIndex, InputStream x, long length )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateCharacterStream( int columnIndex, Reader x, long length )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateAsciiStream( String columnLabel, InputStream x, long length )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateBinaryStream( String columnLabel, InputStream x, long length )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateCharacterStream( String columnLabel, Reader reader, long length )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateBlob( int columnIndex, InputStream inputStream, long length )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateBlob( String columnLabel, InputStream inputStream, long length )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateClob( int columnIndex, Reader reader, long length )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateClob( String columnLabel, Reader reader, long length )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateNClob( int columnIndex, Reader reader, long length )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateNClob( String columnLabel, Reader reader, long length )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateNCharacterStream( int columnIndex, Reader x )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateNCharacterStream( String columnLabel, Reader reader )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateAsciiStream( int columnIndex, InputStream x )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateBinaryStream( int columnIndex, InputStream x )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateCharacterStream( int columnIndex, Reader x )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateAsciiStream( String columnLabel, InputStream x )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateBinaryStream( String columnLabel, InputStream x )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateCharacterStream( String columnLabel, Reader reader )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateBlob( int columnIndex, InputStream inputStream )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateBlob( String columnLabel, InputStream inputStream )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateClob( int columnIndex, Reader reader )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateClob( String columnLabel, Reader reader )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateNClob( int columnIndex, Reader reader )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateNClob( String columnLabel, Reader reader )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public <T> T getObject( int columnIndex, Class<T> type )
        throws SQLException {
        Object o = getObject( columnIndex );
        if ( type == String.class )
            return (T) o.toString();
        return (T) o; // will fail if T is not a compatible type
    }

    @Override
    public <T> T getObject( String columnLabel, Class<T> type )
        throws SQLException {
        return getObject( columnIndex( columnLabel ), type );
    }

    @Override
    public String getString( String columnLabel )
        throws SQLException {
        return getString( columnIndex( columnLabel ) );
    }

    @Override
    public boolean getBoolean( String columnLabel )
        throws SQLException {
        return getBoolean( columnIndex( columnLabel ) );
    }

    @Override
    public byte getByte( String columnLabel )
        throws SQLException {
        return getByte( columnIndex( columnLabel ) );
    }

    @Override
    public short getShort( String columnLabel )
        throws SQLException {
        return getShort( columnIndex( columnLabel ) );
    }

    @Override
    public int getInt( String columnLabel )
        throws SQLException {
        return getInt( columnIndex( columnLabel ) );
    }

    @Override
    public Reader getCharacterStream( int columnIndex )
        throws SQLException {
        String s = getString( columnIndex );
        if ( wasNull )
            return null;
        return new StringReader( s );
    }

    @Override
    public Reader getCharacterStream( String columnLabel )
        throws SQLException {
        return ( getCharacterStream( columnIndex( columnLabel ) ) );
    }

    @Override
    public BigDecimal getBigDecimal( int columnIndex )
        throws SQLException {
        return new BigDecimal( getString( columnIndex ) );
    }

    @Override
    public BigDecimal getBigDecimal( String columnLabel )
        throws SQLException {
        return getBigDecimal( columnIndex( columnLabel ) );
    }

    @Override
    public boolean isBeforeFirst()
        throws SQLException {
        return currentRow == null && !eofReached;
    }

    @Override
    public boolean isAfterLast()
        throws SQLException {
        return currentRow == null && eofReached;
    }

    @Override
    public boolean isFirst()
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "isFirst" );
    }

    @Override
    public boolean isLast()
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "isLast" );
    }

    @Override
    public void beforeFirst()
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "beforeFirst" );
    }

    @Override
    public void afterLast()
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "afterLast" );
    }

    @Override
    public boolean first()
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "first" );
    }

    @Override
    public boolean last()
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "last" );
    }

    @Override
    public int getRow()
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "getRow" );
    }

    @Override
    public boolean absolute( int row )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "absolute" );
    }

    @Override
    public boolean relative( int rows )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "relative" );
    }

    @Override
    public boolean previous()
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "previous" );
    }

    @Override
    public int getFetchDirection()
        throws SQLException {
        return FETCH_FORWARD;
    }

    @Override
    public void setFetchDirection( int direction )
        throws SQLException {
        if ( direction != FETCH_FORWARD )
            throw new SQLFeatureNotSupportedException( "Fetch direction other than FETCH_FORWARD" );
    }

    @Override
    public int getFetchSize()
        throws SQLException {
        return this.batchSize;
    }

    @Override
    public void setFetchSize( int rows ) {
        this.batchSize = rows;
    }

    @Override
    public int getType()
        throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency()
        throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean rowUpdated()
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public boolean rowInserted()
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public boolean rowDeleted()
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateNull( int columnIndex )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateBoolean( int columnIndex, boolean x )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateByte( int columnIndex, byte x )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateShort( int columnIndex, short x )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateInt( int columnIndex, int x )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateLong( int columnIndex, long x )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateFloat( int columnIndex, float x )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateDouble( int columnIndex, double x )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateBigDecimal( int columnIndex, BigDecimal x )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateString( int columnIndex, String x )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateBytes( int columnIndex, byte[] x )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateDate( int columnIndex, Date x )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateTime( int columnIndex, Time x )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateTimestamp( int columnIndex, Timestamp x )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateAsciiStream( int columnIndex, InputStream x, int length )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateBinaryStream( int columnIndex, InputStream x, int length )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateCharacterStream( int columnIndex, Reader x, int length )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateObject( int columnIndex, Object x, int scaleOrLength )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateObject( int columnIndex, Object x )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateNull( String columnLabel )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateBoolean( String columnLabel, boolean x )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateByte( String columnLabel, byte x )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateShort( String columnLabel, short x )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateInt( String columnLabel, int x )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateLong( String columnLabel, long x )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateFloat( String columnLabel, float x )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateDouble( String columnLabel, double x )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateBigDecimal( String columnLabel, BigDecimal x )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateString( String columnLabel, String x )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateBytes( String columnLabel, byte[] x )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateTime( String columnLabel, Time x )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateTimestamp( String columnLabel, Timestamp x )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateAsciiStream( String columnLabel, InputStream x, int length )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateBinaryStream( String columnLabel, InputStream x, int length )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateCharacterStream( String columnLabel, Reader reader, int length )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateObject( String columnLabel, Object x, int scaleOrLength )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateObject( String columnLabel, Object x )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void insertRow()
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void updateRow()
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void deleteRow()
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void refreshRow()
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "refreshRow" );
    }

    @Override
    public void cancelRowUpdates()
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void moveToInsertRow()
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void moveToCurrentRow()
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public Statement getStatement()
        throws SQLException {
        return statement;
    }

    @Override
    public Object getObject( int columnIndex, Map<String, Class<?>> map )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "getObject" );
    }

    @Override
    public Ref getRef( int columnIndex )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "getRef" );
    }

    @Override
    public Blob getBlob( int columnIndex )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "getBlob" );
    }

    @Override
    public Clob getClob( int columnIndex )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "getClob" );
    }

    @Override
    public Array getArray( int columnIndex )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "getArray" );
    }

    @Override
    public Object getObject( String columnLabel, Map<String, Class<?>> map )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "getObject" );
    }

    @Override
    public Ref getRef( String columnLabel )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "getRef" );
    }

    @Override
    public Blob getBlob( String columnLabel )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "getBlob" );
    }

    @Override
    public Clob getClob( String columnLabel )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "getClob" );
    }

    @Override
    public Array getArray( String columnLabel )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "getArray" );
    }

    @Override
    public void updateDate( String columnLabel, Date x )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    void addWarning( String reason ) {
        SQLWarning newSqlWarning = new SQLWarning( reason );
        if ( sqlWarning != null )
            sqlWarning.setNextWarning( newSqlWarning );
        else
            sqlWarning = newSqlWarning;
    }
}
