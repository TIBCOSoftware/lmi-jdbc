/*
 * Copyright (c) 2014-2018 TIBCO Software Inc. All Rights Reserved.
 * Licensed under a BSD-type license. See TIBCO LICENSE.txt for license text.
 */
package com.tibco.loglogic.lmi.jdbc;

import static com.tibco.loglogic.lmi.jdbc.LmiConnection.DEFAULT_BATCH_SIZE;
import static com.tibco.loglogic.lmi.jdbc.LmiConnection.DEFAULT_INSECURE_MODE;
import static com.tibco.loglogic.lmi.jdbc.LmiConnection.DEFAULT_NETWORK_TIMEOUT_MILIS;
import static com.tibco.loglogic.lmi.jdbc.LmiConnection.DEFAULT_NO_HOSTNAME_VERIFICATION;
import static com.tibco.loglogic.lmi.jdbc.LmiConnection.DEFAULT_QUERY_TIMEOUT;
import static com.tibco.loglogic.lmi.jdbc.LmiConnection.PROPERTY_ACCEPTED_CERTIFICATE_FINGERPRINTS;
import static com.tibco.loglogic.lmi.jdbc.LmiConnection.PROPERTY_BATCH_SIZE;
import static com.tibco.loglogic.lmi.jdbc.LmiConnection.PROPERTY_INSECURE_MODE;
import static com.tibco.loglogic.lmi.jdbc.LmiConnection.PROPERTY_KEYSTORE_PASSWORD;
import static com.tibco.loglogic.lmi.jdbc.LmiConnection.PROPERTY_KEYSTORE_URL;
import static com.tibco.loglogic.lmi.jdbc.LmiConnection.PROPERTY_NETWORK_TIMEOUT;
import static com.tibco.loglogic.lmi.jdbc.LmiConnection.PROPERTY_NO_HOSTNAME_VERIFICATION;
import static com.tibco.loglogic.lmi.jdbc.LmiConnection.PROPERTY_PASSWORD;
import static com.tibco.loglogic.lmi.jdbc.LmiConnection.PROPERTY_QUERY_TIMEOUT;
import static com.tibco.loglogic.lmi.jdbc.LmiConnection.PROPERTY_USER;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * JDBC Driver for Apollo.
 */
public class LmiJdbcDriver
    implements java.sql.Driver {

    static final int VERSION_MAJOR = 1;

    static final int VERSION_MINOR = 0;

    static final int JDBC_VERSION_MAJOR = 4;

    static final int JDBC_VERSION_MINOR = 1;

    private static final String DRIVER_NAME = "JDBC Driver for TIBCO LogLogic(R) Advanced Features";

    private static final String DRIVER_VERSION = VERSION_MAJOR + "." + VERSION_MINOR;

    private static final String JDBC_URL_START = "jdbc:";

    private static final String DRIVER_URL_START = "jdbc:lmi:";

    /*
     * This static block loads the JDBC driver.
     */
    static {
        try {
            DriverManager.registerDriver( new LmiJdbcDriver() );
        }
        catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
    }

    public Connection connect( String url, Properties info )
        throws SQLException {

        if ( !acceptsURL( url ) ) {
            return null;
        }

        URI uri = parseDriverUrl( url, info );

        try {
            LmiConnection ret = new LmiConnection( uri.getHost(), uri.getPort(), info );
            return ret;
        }
        catch ( Exception e ) {
            throw new SQLException( "Error in creating connection", e );
        }

    }

    public boolean acceptsURL( String url )
        throws SQLException {
        return url.startsWith( DRIVER_URL_START );
    }

    public DriverPropertyInfo[] getPropertyInfo( String url, Properties info )
        throws SQLException {
        List<DriverPropertyInfo> driverPropertyInfoList = new ArrayList<>();

        if ( !info.containsKey( PROPERTY_USER ) ) {
            DriverPropertyInfo driverPropertyInfo = new DriverPropertyInfo( PROPERTY_USER, null );
            driverPropertyInfo.description = "Name of the user for connection";
            driverPropertyInfo.required = true;
            driverPropertyInfoList.add( driverPropertyInfo );
        }

        if ( !info.containsKey( PROPERTY_PASSWORD ) ) {
            DriverPropertyInfo driverPropertyInfo = new DriverPropertyInfo( PROPERTY_PASSWORD, null );
            driverPropertyInfo.description = "Password of the user for connection";
            driverPropertyInfo.required = true;
            driverPropertyInfoList.add( driverPropertyInfo );
        }

        if ( !info.containsKey( PROPERTY_NETWORK_TIMEOUT ) ) {
            DriverPropertyInfo driverPropertyInfo = new DriverPropertyInfo( PROPERTY_NETWORK_TIMEOUT,
                    DEFAULT_NETWORK_TIMEOUT_MILIS);
            driverPropertyInfo.description = "Timeout for network operations";
            driverPropertyInfo.required = false;
        }

        if ( !info.containsKey( PROPERTY_BATCH_SIZE ) ) {
            DriverPropertyInfo driverPropertyInfo = new DriverPropertyInfo( PROPERTY_BATCH_SIZE, DEFAULT_BATCH_SIZE );
            driverPropertyInfo.description = "Size of batches";
            driverPropertyInfo.required = false;
        }

        if ( !info.containsKey( PROPERTY_QUERY_TIMEOUT ) ) {
            DriverPropertyInfo driverPropertyInfo = new DriverPropertyInfo( PROPERTY_QUERY_TIMEOUT, DEFAULT_QUERY_TIMEOUT );
            driverPropertyInfo.description = "TTL for the query";
            driverPropertyInfo.required = false;
        }

        if ( !info.containsKey( PROPERTY_NO_HOSTNAME_VERIFICATION ) ) {
            DriverPropertyInfo driverPropertyInfo = new DriverPropertyInfo( PROPERTY_NO_HOSTNAME_VERIFICATION, DEFAULT_NO_HOSTNAME_VERIFICATION );
            driverPropertyInfo.description = "Set to true to disable hostname verification";
            driverPropertyInfo.required = false;
        }

        if ( !info.containsKey( PROPERTY_ACCEPTED_CERTIFICATE_FINGERPRINTS ) ) {
            DriverPropertyInfo driverPropertyInfo = new DriverPropertyInfo( PROPERTY_ACCEPTED_CERTIFICATE_FINGERPRINTS, "" );
            driverPropertyInfo.description = "List of accepted certificate fingerprints";
            driverPropertyInfo.required = false;
        }

        if ( !info.containsKey( PROPERTY_KEYSTORE_URL ) ) {
            DriverPropertyInfo driverPropertyInfo = new DriverPropertyInfo( PROPERTY_KEYSTORE_URL, "" );
            driverPropertyInfo.description = "Location of the keystore to use";
            driverPropertyInfo.required = false;
        }

        if ( !info.containsKey( PROPERTY_KEYSTORE_PASSWORD ) ) {
            DriverPropertyInfo driverPropertyInfo = new DriverPropertyInfo( PROPERTY_KEYSTORE_PASSWORD, "" );
            driverPropertyInfo.description = "Password for the keystore to use";
            driverPropertyInfo.required = false;
        }

        if ( !info.containsKey( PROPERTY_INSECURE_MODE ) ) {
            DriverPropertyInfo driverPropertyInfo = new DriverPropertyInfo( PROPERTY_INSECURE_MODE, DEFAULT_INSECURE_MODE );
            driverPropertyInfo.description = "Insecure mode: no certificate or hostname validation";
            driverPropertyInfo.required = false;
        }

        return driverPropertyInfoList.toArray( new DriverPropertyInfo[driverPropertyInfoList.size()] );
    }

    public int getMajorVersion() {
        return JDBC_VERSION_MAJOR;
    }

    public int getMinorVersion() {
        return JDBC_VERSION_MINOR;
    }

    /**
     * A driver may only report true here if it passes the JDBC compliance tests JDBC compliance requires full support
     * for the JDBC API and full support for SQL 92 Entry Level
     *
     * @return always false
     */
    public boolean jdbcCompliant() {
        return false;
    }

    /**
     * Always fail because we do not use java.util.logging.
     *
     * @throws SQLFeatureNotSupportedException always
     */
    public Logger getParentLogger()
        throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException( "getParentLogger()" );
    }

    @SuppressWarnings("unused")
    public static String getDriverName() {
        return DRIVER_NAME;
    }

    @SuppressWarnings("unused")
    public static String getDriverVersion() {
        return DRIVER_VERSION;
    }

    /**
     * Method to parse the URL supplied
     */
    private URI parseDriverUrl( String urlString, Properties info )
        throws SQLException {
        URI uri;
        try {
            uri = new URI( urlString.substring( JDBC_URL_START.length() ) );
        }
        catch ( URISyntaxException e ) {
            throw new SQLException( "Invalid JDBC URL: " + urlString, e );
        }
        if ( uri.getHost() == null || uri.getHost().isEmpty() ) {
            throw new SQLException( "No host specified: " + urlString );
        }

        if ( uri.getPort() == -1 ) {
            throw new SQLException( "No port number specified: " + urlString );
        }

        if ( ( uri.getPort() < 1 ) || ( uri.getPort() > 65535 ) ) {
            throw new SQLException( "Invalid port number: " + urlString );
        }

        String query = uri.getQuery();
        if ( query != null ) {
            String[] kvps = query.split( "&" );
            for ( String kvp : kvps ) {
                int eqIdx = kvp.indexOf( '=' );
                if ( eqIdx == -1 )
                    continue;
                String key = kvp.substring( 0, eqIdx );
                String value = kvp.substring( eqIdx + 1, kvp.length() );
                info.put( key, value );
            }
        }

        return uri;
    }

}
