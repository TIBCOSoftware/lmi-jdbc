/*
 * Copyright (c) 2014-2018 TIBCO Software Inc. All Rights Reserved.
 * Licensed under a BSD-type license. See TIBCO LICENSE.txt for license text.
 */
package com.tibco.loglogic.lmi.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/**
 * Apollo's prepared statement.
 */
public class LmiPreparedStatement
    extends LmiStatement
    implements PreparedStatement {

    /** SQL template */
    private final String sql;

    /** indexes of each parameter in the SQL query */
    private final int[] parameterIndxes;

    /** values set by the user for each parameter */
    private final Object[] parameterValues;

    LmiPreparedStatement( LmiConnection connection, String sql, int resultSetType ) {
        super( connection, resultSetType );
        this.sql = sql;

        this.parameterIndxes = initArray( this.sql );
        this.parameterValues = new Object[this.parameterIndxes.length];
    }

    /**
     * Find indexes of each parameter in parameterized SQL query
     */
    private int[] initArray( String sql ) {

        List<Integer> paramIndexes = new ArrayList<>();

        // find all '?' that are not inside the double quote strings
        int paramCount = 0;
        boolean escapingQuote = false;
        boolean inQuote = false;
        for ( int i = 0; i < sql.length(); i++ ) {
            char ch = sql.charAt( i );
            if ( inQuote ) {
                if ( escapingQuote ) {
                    escapingQuote = false;
                }
                else {
                    if ( ch == '\\' ) {
                        escapingQuote = true;
                    }
                    else if ( ch == '"' ) {
                        inQuote = false;
                    }
                }
            }
            else {
                if ( ch == '?' ) {
                    paramCount++;
                    paramIndexes.add( i );
                }
                else if ( ch == '"' ) {
                    inQuote = true;
                }
            }
        }
        int[] result = new int[paramCount];
        for ( int i = 0; i < result.length; i++ ) {
            result[i] = paramIndexes.get( i );
        }
        return result;
    }

    @Override
    public ResultSet executeQuery()
        throws SQLException {

        // execute the query with substituted arguments
        return executeQuery( substituteParams() );
    }

    private String substituteParams()
        throws SQLException {

        if ( this.parameterIndxes.length == 0 ) {
            return this.sql;
        }

        // check if all values are set
        for ( int i = 0; i < this.parameterValues.length; i++ ) {
            if ( this.parameterValues[i] == null ) {
                throw new SQLSyntaxErrorException( String //
                    .format( "Requred parameter %d is not set for prepared statement: %s", ( i + 1 ), this.sql ) );
            }
        }

        // substitute
        StringBuilder sb = new StringBuilder();
        int lastIndex = 0;
        for ( int i = 0; i < this.parameterValues.length; i++ ) {

            String stringValue = computeStringValue( this.parameterValues[i] );
            if ( stringValue == null ) {
                throw new SQLSyntaxErrorException( String.format( "Unsupported parameter %d for the query: %s", i,
                                                                  this.sql ) );
            }
            int currentIndex = this.parameterIndxes[i];

            sb.append( this.sql, lastIndex, currentIndex );
            sb.append( stringValue );
            lastIndex = currentIndex + 1;
        }
        if ( lastIndex < this.sql.length() ) {
            sb.append( this.sql, lastIndex, this.sql.length() );
        }

        return sb.toString();
    }

    /**
     * Compute string value of the object that will be injected into query string.
     *
     * @return String representation of the object or null is object type is not supported.
     */
    private String computeStringValue( Object object ) {
        if ( object instanceof Number ) {
            return String.valueOf( object );
        }
        if ( object instanceof String ) {
            return escapeString( (String) object );
        }
        if ( object instanceof Date ) {
            // encode Data as Long
            return String.valueOf( ( (Date) object ).getTime() );
        }

        // unknown - return null
        return null;
    }

    private String escapeString( String string ) {

        StringBuilder sb = new StringBuilder( string.length() + 2 );
        sb.append( '"' );

        int lastIndex = 0;
        for ( int i = 0; i < string.length(); i++ ) {
            char ch = string.charAt( i );
            if ( ch == '"' || ch == '\\' ) {
                if ( lastIndex < i ) {
                    sb.append( string, lastIndex, i - 1 );
                }
                sb.append( '\\' );
                sb.append( ch );
                lastIndex = i + 1;
            }
        }
        if ( lastIndex < string.length() ) {
            sb.append( string, lastIndex, string.length() );
        }
        sb.append( '"' );

        return sb.toString();
    }

    @Override
    public int executeUpdate()
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void setNull( int parameterIndex, int sqlType )
        throws SQLException {
        int i = validateParamIndex( parameterIndex );
        this.parameterValues[i] = null;
    }

    @Override
    public void setBoolean( int parameterIndex, boolean x )
        throws SQLException {
        int i = validateParamIndex( parameterIndex );
        this.parameterValues[i] = x;
    }

    @Override
    public void setByte( int parameterIndex, byte x )
        throws SQLException {
        int i = validateParamIndex( parameterIndex );
        this.parameterValues[i] = x;
    }

    @Override
    public void setShort( int parameterIndex, short x )
        throws SQLException {
        int i = validateParamIndex( parameterIndex );
        this.parameterValues[i] = x;
    }

    @Override
    public void setInt( int parameterIndex, int x )
        throws SQLException {
        int i = validateParamIndex( parameterIndex );
        this.parameterValues[i] = x;
    }

    @Override
    public void setLong( int parameterIndex, long x )
        throws SQLException {
        int i = validateParamIndex( parameterIndex );
        this.parameterValues[i] = x;
    }

    @Override
    public void setFloat( int parameterIndex, float x )
        throws SQLException {
        int i = validateParamIndex( parameterIndex );
        this.parameterValues[i] = x;
    }

    @Override
    public void setDouble( int parameterIndex, double x )
        throws SQLException {
        int i = validateParamIndex( parameterIndex );
        this.parameterValues[i] = x;
    }

    @Override
    public void setBigDecimal( int parameterIndex, BigDecimal x )
        throws SQLException {
        int i = validateParamIndex( parameterIndex );
        this.parameterValues[i] = x;
    }

    @Override
    public void setString( int parameterIndex, String x )
        throws SQLException {
        int i = validateParamIndex( parameterIndex );
        this.parameterValues[i] = x;
    }

    private int validateParamIndex( int parameterIndex )
        throws SQLException {
        if ( parameterIndex > 0 && parameterIndex <= this.parameterValues.length ) {
            return parameterIndex - 1;
        }
        throw new SQLSyntaxErrorException( String.format( "Invalid paramter index to the query: %s", this.sql ) );
    }

    @Override
    public void setBytes( int parameterIndex, byte[] x )
        throws SQLException {
        throwUnsupportedType();
    }

    @Override
    public void setDate( int parameterIndex, Date x )
        throws SQLException {
        int i = validateParamIndex( parameterIndex );
        this.parameterValues[i] = x;
    }

    @Override
    public void setTime( int parameterIndex, Time x )
        throws SQLException {
        int i = validateParamIndex( parameterIndex );
        this.parameterValues[i] = x;
    }

    @Override
    public void setTimestamp( int parameterIndex, Timestamp x )
        throws SQLException {
        int i = validateParamIndex( parameterIndex );
        this.parameterValues[i] = x;
    }

    @Override
    public void setAsciiStream( int parameterIndex, InputStream x, int length )
        throws SQLException {
        throwUnsupportedType();
    }

    @Override
    @Deprecated
    public void setUnicodeStream( int parameterIndex, InputStream x, int length )
        throws SQLException {
        throwUnsupportedType();
    }

    @Override
    public void setBinaryStream( int parameterIndex, InputStream x, int length )
        throws SQLException {
        throwUnsupportedType();
    }

    @Override
    public void clearParameters()
        throws SQLException {
        for ( int i = 0; i < this.parameterValues.length; i++ ) {
            this.parameterValues[i] = null;
        }
    }

    @Override
    public void setObject( int parameterIndex, Object x, int targetSqlType )
        throws SQLException {
        int i = validateParamIndex( parameterIndex );
        this.parameterValues[i] = x;
    }

    @Override
    public void setObject( int parameterIndex, Object x )
        throws SQLException {
        int i = validateParamIndex( parameterIndex );
        this.parameterValues[i] = x;
    }

    @Override
    public boolean execute()
        throws SQLException {

        // assume this is a query
        return super.execute( this.substituteParams() );
    }

    @Override
    public void addBatch()
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "READ-ONLY" );
    }

    @Override
    public void setCharacterStream( int parameterIndex, Reader reader, int length )
        throws SQLException {
        throwUnsupportedType();
    }

    @Override
    public void setRef( int parameterIndex, Ref x )
        throws SQLException {
        throwUnsupportedType();
    }

    @Override
    public void setBlob( int parameterIndex, Blob x )
        throws SQLException {
        throwUnsupportedType();
    }

    @Override
    public void setClob( int parameterIndex, Clob x )
        throws SQLException {
        throwUnsupportedType();
    }

    @Override
    public void setArray( int parameterIndex, Array x )
        throws SQLException {
        throwUnsupportedType();
    }

    @Override
    public ResultSetMetaData getMetaData()
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "Metadata for query is not available before the query is ezecuted." );
    }

    @Override
    public void setDate( int parameterIndex, Date x, Calendar cal )
        throws SQLException {
        int i = validateParamIndex( parameterIndex );
        this.parameterValues[i] = x.getTime();
    }

    @Override
    public void setTime( int parameterIndex, Time x, Calendar cal )
        throws SQLException {
        int i = validateParamIndex( parameterIndex );
        this.parameterValues[i] = x.getTime();
    }

    @Override
    public void setTimestamp( int parameterIndex, Timestamp x, Calendar cal )
        throws SQLException {
        int i = validateParamIndex( parameterIndex );
        this.parameterValues[i] = x.getTime();
    }

    @Override
    public void setNull( int parameterIndex, int sqlType, String typeName )
        throws SQLException {
        int i = validateParamIndex( parameterIndex );
        this.parameterValues[i] = null;
    }

    @Override
    public void setURL( int parameterIndex, URL x )
        throws SQLException {
        throwUnsupportedType();
    }

    @Override
    public ParameterMetaData getParameterMetaData()
        throws SQLException {
        return null;
    }

    @Override
    public void setRowId( int parameterIndex, RowId x )
        throws SQLException {
        throwUnsupportedType();
    }

    @Override
    public void setNString( int parameterIndex, String value )
        throws SQLException {
        int i = validateParamIndex( parameterIndex );
        this.parameterValues[i] = value;
    }

    @Override
    public void setNCharacterStream( int parameterIndex, Reader value, long length )
        throws SQLException {
        throwUnsupportedType();
    }

    @Override
    public void setNClob( int parameterIndex, NClob value )
        throws SQLException {
        throwUnsupportedType();
    }

    @Override
    public void setClob( int parameterIndex, Reader reader, long length )
        throws SQLException {
        throwUnsupportedType();
    }

    @Override
    public void setBlob( int parameterIndex, InputStream inputStream, long length )
        throws SQLException {
        throwUnsupportedType();
    }

    @Override
    public void setNClob( int parameterIndex, Reader reader, long length )
        throws SQLException {
        throwUnsupportedType();
    }

    @Override
    public void setSQLXML( int parameterIndex, SQLXML xmlObject )
        throws SQLException {
        throwUnsupportedType();
    }

    @Override
    public void setObject( int parameterIndex, Object x, int targetSqlType, int scaleOrLength )
        throws SQLException {
        throwUnsupportedType();
    }

    @Override
    public void setAsciiStream( int parameterIndex, InputStream x, long length )
        throws SQLException {
        throwUnsupportedType();
    }

    @Override
    public void setBinaryStream( int parameterIndex, InputStream x, long length )
        throws SQLException {
        throwUnsupportedType();
    }

    @Override
    public void setCharacterStream( int parameterIndex, Reader reader, long length )
        throws SQLException {
        throwUnsupportedType();
    }

    @Override
    public void setAsciiStream( int parameterIndex, InputStream x )
        throws SQLException {
        throwUnsupportedType();
    }

    @Override
    public void setBinaryStream( int parameterIndex, InputStream x )
        throws SQLException {
        throwUnsupportedType();
    }

    @Override
    public void setCharacterStream( int parameterIndex, Reader reader )
        throws SQLException {
        throwUnsupportedType();
    }

    @Override
    public void setNCharacterStream( int parameterIndex, Reader value )
        throws SQLException {
        throwUnsupportedType();
    }

    @Override
    public void setClob( int parameterIndex, Reader reader )
        throws SQLException {
        throwUnsupportedType();
    }

    @Override
    public void setBlob( int parameterIndex, InputStream inputStream )
        throws SQLException {
        throwUnsupportedType();
    }

    @Override
    public void setNClob( int parameterIndex, Reader reader )
        throws SQLException {
        throwUnsupportedType();
    }

    private void throwUnsupportedType()
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "Unsupported type." );
    }
}
