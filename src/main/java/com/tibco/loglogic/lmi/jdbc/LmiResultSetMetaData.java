/*
 * Copyright (c) 2014-2018 TIBCO Software Inc. All Rights Reserved.
 * Licensed under a BSD-type license. See TIBCO LICENSE.txt for license text.
 */
package com.tibco.loglogic.lmi.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

import com.tibco.loglogic.lmi.jdbc.QueryPostExecutor.ColumnDesc;
import com.tibco.loglogic.lmi.jdbc.QueryPostExecutor.QueryMetadata;

public class LmiResultSetMetaData
    implements ResultSetMetaData {

    private static final int INET_ADDRESS = -10;

    private final QueryMetadata metadata;

    LmiResultSetMetaData( QueryMetadata metadata ) {
        this.metadata = metadata;
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
    public int getColumnCount()
        throws SQLException {
        return this.metadata.getColumns().size();
    }

    @Override
    public boolean isAutoIncrement( int column )
        throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive( int column )
        throws SQLException {
        return false;
    }

    @Override
    public boolean isSearchable( int column )
        throws SQLException {
        return true;
    }

    @Override
    public boolean isCurrency( int column )
        throws SQLException {
        return false;
    }

    @Override
    public int isNullable( int column )
        throws SQLException {
        return columnNullable;
    }

    @Override
    public boolean isSigned( int column )
        throws SQLException {
        return false;
    }

    @Override
    public int getColumnDisplaySize( int column )
        throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getColumnLabel( int column )
        throws SQLException {
        return getColumnDesc( column ).getName();
    }

    @Override
    public String getColumnName( int column )
        throws SQLException {
        return getColumnDesc( column ).getName();
    }

    @Override
    public String getSchemaName( int column )
        throws SQLException {
        return "";
    }

    @Override
    public int getPrecision( int column )
        throws SQLException {
        return 0;
    }

    @Override
    public int getScale( int column )
        throws SQLException {
        return 0;
    }

    @Override
    public String getTableName( int column )
        throws SQLException {
        return "";
    }

    @Override
    public String getCatalogName( int column )
        throws SQLException {
        return "";
    }

    @Override
    public int getColumnType( int column )
        throws SQLException {

        if ( "sys_eventTime".equalsIgnoreCase( getColumnName( column ) ) ) {
            return Types.TIMESTAMP;
        }

        return getSQLTypeId( ( getColumnDesc( column ).getType() ) );
    }

    @Override
    public String getColumnTypeName( int column )
        throws SQLException {
        return getSQLTypeName( getColumnType( column ) );
    }

    @Override
    public boolean isReadOnly( int column )
        throws SQLException {
        return true;
    }

    @Override
    public boolean isWritable( int column )
        throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable( int column )
        throws SQLException {
        return false;
    }

    @Override
    public String getColumnClassName( int column )
        throws SQLException {
        return getJavaTypeId( getColumnDesc( column ).getType() );
    }

    static String getSQLTypeName( int type ) {

        switch ( type ) {
            case Types.VARCHAR:
                return "VARCHAR";
            case Types.TIMESTAMP:
                return "TIMESTAMP";
            case Types.INTEGER:
                return "INTEGER";
            case Types.BIGINT:
                return "BIGINT";
            case Types.DOUBLE:
                return "DOUBLE";
            case Types.BOOLEAN:
                return "BOOLEAN";
            case INET_ADDRESS:
                return "VARCHAR";
        }
        return "OTHER";
    }

    private ColumnDesc getColumnDesc( int column ) {
        return this.metadata.getColumns().get( column - 1 );
    }

    static int getSQLTypeId( String apolloType ) {
        if ( "String".equalsIgnoreCase( apolloType ) ) {
            return Types.VARCHAR;
        }
        else if ( "Timestamp".equalsIgnoreCase( apolloType ) ) {
            return Types.TIMESTAMP;
        }
        else if ( "INT".equalsIgnoreCase( apolloType ) ) {
            return Types.INTEGER;
        }
        else if ( "LONG".equalsIgnoreCase( apolloType ) ) {
            return Types.BIGINT;
        }
        else if ( "DOUBLE".equalsIgnoreCase( apolloType ) ) {
            return Types.DOUBLE;
        }
        else if ( "BOOLEAN".equalsIgnoreCase( apolloType ) ) {
            return Types.BOOLEAN;
        }
        else if ( "INET_ADDR".equalsIgnoreCase( apolloType ) ) {
            return Types.VARCHAR;
        }

        return Types.OTHER;
    }

    /**
     * Return the corresponding java class name the specified Unity type
     *
     * @param apolloType the unity type
     * @return the corresponding java class name
     */
    private String getJavaTypeId( String apolloType ) {
        if ( "STRING".equalsIgnoreCase( apolloType ) ) {
            return String.class.getName();
        }
        else if ( "TIMESTAMP".equalsIgnoreCase( apolloType ) ) {
            return Timestamp.class.getName();
        }
        else if ( "INT".equalsIgnoreCase( apolloType ) ) {
            return Integer.class.getName();
        }
        else if ( "LONG".equalsIgnoreCase( apolloType ) ) {
            return Long.class.getName();
        }
        else if ( "DOUBLE".equalsIgnoreCase( apolloType ) ) {
            return Double.class.getName();
        }
        else if ( "BOOLEAN".equalsIgnoreCase( apolloType ) ) {
            return Boolean.class.getName();
        }
        else if ( "INET_ADDR".equalsIgnoreCase( apolloType ) ) {
            return String.class.getName();
        }

        return null;
    }

    public String getQueryId() {
        return metadata.getQueryId();
    }
}
