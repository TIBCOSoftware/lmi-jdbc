/*
 * Copyright (c) 2014-2018 TIBCO Software Inc. All Rights Reserved.
 * Licensed under a BSD-type license. See TIBCO LICENSE.txt for license text.
 */
package com.tibco.loglogic.lmi.jdbc;

import static java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.CONCUR_UPDATABLE;
import static java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE;
import static java.sql.ResultSet.TYPE_SCROLL_SENSITIVE;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;

import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContextBuilder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * JDBC connection to Apollo.
 */
public class LmiConnection
    implements java.sql.Connection {

    /**
     * Name to use for username property when creating the connection
     */
    @SuppressWarnings("WeakerAccess")
    public static final String PROPERTY_USER = "user";

    /**
     * Name to use for password property when creating the connection
     */
    @SuppressWarnings("WeakerAccess")
    public static final String PROPERTY_PASSWORD = "password";

    /**
     * Name to use for network timeout property when creating the connection. This is how long (in millis) the client
     * will wait for a reply from the server.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String PROPERTY_NETWORK_TIMEOUT = "networkTimeoutMillis";

    /**
     * Timeout after which a query will be removed if there is no activity (seconds)
     */
    @SuppressWarnings("WeakerAccess")
    public static final String PROPERTY_QUERY_TIMEOUT = "queryTimeout";

    /**
     * Name to use for the maximum batch size to use when fetching data.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String PROPERTY_BATCH_SIZE = "batchSize";

    @SuppressWarnings("WeakerAccess")
    public static final String PROPERTY_ACCEPTED_CERTIFICATE_FINGERPRINTS = "acceptedCertificateFingerprints";

    @SuppressWarnings("WeakerAccess")
    public static final String PROPERTY_KEYSTORE_URL = "keystoreURL";

    @SuppressWarnings("WeakerAccess")
    public static final String PROPERTY_KEYSTORE_PASSWORD = "keystorePassword";

    @SuppressWarnings("WeakerAccess")
    public static final String PROPERTY_NO_HOSTNAME_VERIFICATION = "noHostnameVerification";

    @SuppressWarnings("WeakerAccess")
    public static final String PROPERTY_INSECURE_MODE = "insecureMode";

    @SuppressWarnings("WeakerAccess")
    public static final String PROPERTY_POLLING_TIMEOUT = "pollingTimeout";

    @SuppressWarnings("WeakerAccess")
    public static final String PROPERTY_POLLING_PERIOD = "pollingPeriod";

    @SuppressWarnings("WeakerAccess")
    public static final String PROPERTY_CONCURRENT_STATEMENTS = "concurrentStatements";

    // defaults for the above properties
    static final String DEFAULT_NETWORK_TIMEOUT_MILIS = "600000";

    static final String DEFAULT_BATCH_SIZE = "5000";

    static final String DEFAULT_QUERY_TIMEOUT = "3600";

    static final String DEFAULT_NO_HOSTNAME_VERIFICATION = "false";

    static final String DEFAULT_INSECURE_MODE = "false";

    static final String DEFAULT_POLL_PERIOD = "10000";

    static final String DEFAULT_CONCURRENT_STATEMENTS = "30";

    /**
     * thread pool for background communication with QueryNode
     */
    private final ExecutorService executor;

    /**
     * Boolean variable to indicate state of connection
     */
    boolean closed;

    private final String username;

    private final String password;

    private final String hostname;

    private final int port;

    private final int networkTimeoutMillis;

    private final int queryTimeout;

    private final int batchSize;

    private final String baseUrl;

    private final String queryUrl;

    private final CloseableHttpClient client;

    private final boolean noHostnameVerification;

    private final boolean insecureMode;

    private final int pollingPeriod;

    private final int pollingTimeout;

    private boolean autoCommit = true;

    private final int concurrentStatements;

    private String buildVersion;

    CloseableHttpClient getHttpClient()
        throws SQLException {
        checkClosed();
        return this.client;
    }

    private final Properties clientInfo;

    private final ObjectMapper objectMapper;

    /**
     * The Constructor.
     *
     * @param hostname of the server
     * @param port number of the server
     * @param info set of properties, including required 'user' and 'password'.
     */
    LmiConnection( String hostname, int port, Properties info )
        throws SQLException {

        this.clientInfo = info;
        this.username = info.getProperty( PROPERTY_USER );
        this.password = info.getProperty( PROPERTY_PASSWORD );
        this.hostname = hostname;
        this.port = port;
        this.networkTimeoutMillis = Integer
            .parseInt( info.getProperty( PROPERTY_NETWORK_TIMEOUT, DEFAULT_NETWORK_TIMEOUT_MILIS ) );
        this.batchSize = Integer.parseInt( info.getProperty( PROPERTY_BATCH_SIZE, DEFAULT_BATCH_SIZE ) );
        this.queryTimeout = Integer.parseInt( info.getProperty( PROPERTY_QUERY_TIMEOUT, DEFAULT_QUERY_TIMEOUT ) );
        this.noHostnameVerification = Boolean
            .parseBoolean( info.getProperty( PROPERTY_NO_HOSTNAME_VERIFICATION, DEFAULT_NO_HOSTNAME_VERIFICATION ) );
        this.insecureMode = Boolean
            .parseBoolean( ( info.getProperty( PROPERTY_INSECURE_MODE, DEFAULT_INSECURE_MODE ) ) );
        this.pollingPeriod = Integer.parseInt( ( info.getProperty( PROPERTY_POLLING_PERIOD, DEFAULT_POLL_PERIOD ) ) );
        this.pollingTimeout = Integer
            .parseInt( ( info.getProperty( PROPERTY_POLLING_TIMEOUT, Integer.toString( queryTimeout ) ) ) );
        this.concurrentStatements = Integer
            .parseInt( info.getProperty( PROPERTY_CONCURRENT_STATEMENTS, DEFAULT_CONCURRENT_STATEMENTS ) );

        this.executor = Executors.newCachedThreadPool();

        this.baseUrl = "https://" + hostname + ":" + port;

        this.queryUrl = baseUrl + "/api/v2/query";

        this.objectMapper = new ObjectMapper();

        this.client = initHttpClient();

        checkConnection( networkTimeoutMillis );
    }

    private void checkConnection( int timeout )
        throws SQLException {
        HttpGet getRequest = new HttpGet( baseUrl + "/api/v1/configuration" );
        RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout( timeout ).setConnectTimeout( timeout )
            .build();
        getRequest.setConfig( requestConfig );
        getRequest.setHeader( "Accept", "application/json, text/plain " );
        getRequest.setHeader( "Content-Type", "application/json;charset=\"UTF-8\"" );

        try {
            HttpResponse httpResponse = getHttpClient().execute( getRequest );
            if ( httpResponse.getStatusLine().getStatusCode() == 401 ) {
                throw new SQLException( "Authentication failed" );
            }
            else if ( httpResponse.getStatusLine().getStatusCode() == 200 ) {
                // extract config object
                JsonNode actualObj = objectMapper.readTree( httpResponse.getEntity().getContent() );
                buildVersion = actualObj.findValue( "build" ).findValue( "version" ).asText();
                System.out.println( actualObj );
                System.out.println( "build.version=" + buildVersion );
            }
            else {
                throw new SQLException( "Bad status code: " + httpResponse.getStatusLine().getStatusCode() );
            }
        }
        catch ( IOException e ) {
            throw new SQLException( e );
        }
    }

    int getBatchSize() {
        return batchSize;
    }

    int getPollingPeriod() {
        return pollingPeriod;
    }

    int getPollingTimeout() {
        return pollingTimeout;
    }

    static final class CertificateFingerprint {
        final String algorithm;

        final byte[] signature;

        CertificateFingerprint( String textFingerprint ) {
            String[] parts = textFingerprint.split( ":" );
            algorithm = parts[0];
            signature = new byte[parts.length - 1];
            for ( int i = 0; i < signature.length; i++ ) {
                signature[i] = (byte) ( Integer.parseInt( parts[i + 1], 16 ) & 0xff );
            }
        }
    }

    private CloseableHttpClient initHttpClient()
        throws SQLException {
        try {
            SSLContextBuilder builder = new SSLContextBuilder();
            if ( clientInfo.containsKey( PROPERTY_ACCEPTED_CERTIFICATE_FINGERPRINTS ) ) {
                String acceptedCertificateFingerprintsString = (String) clientInfo
                    .get( PROPERTY_ACCEPTED_CERTIFICATE_FINGERPRINTS );
                String[] acceptedCertificateFingerprintsArray = acceptedCertificateFingerprintsString.split( "," );
                final List<CertificateFingerprint> acceptedCertificateFingerprints = new ArrayList<>();
                for ( String acceptedCertificateFingerprint : acceptedCertificateFingerprintsArray ) {
                    acceptedCertificateFingerprints.add( new CertificateFingerprint( acceptedCertificateFingerprint ) );
                }
                builder.loadTrustMaterial( new TrustStrategy() {
                    public boolean isTrusted( X509Certificate[] x509Certificates, String authType )
                        throws CertificateException {
                        x509Certificates[0].checkValidity();
                        for ( CertificateFingerprint certificateFingerprint : acceptedCertificateFingerprints ) {
                            try {
                                if ( Arrays.equals( certificateFingerprint.signature,
                                                    getFingerprint( certificateFingerprint.algorithm,
                                                                    x509Certificates[0] ) ) ) {
                                    return true;
                                }
                            }
                            catch ( NoSuchAlgorithmException e ) {
                                throw new CertificateException( "Unknown fingerprint algorithm: "
                                    + certificateFingerprint.algorithm );
                            }
                        }
                        throw new CertificateException( "Unaccepted Certificate Signature" );
                    }
                } );
            }
            else if ( clientInfo.containsKey( PROPERTY_KEYSTORE_URL )
                && clientInfo.containsKey( PROPERTY_KEYSTORE_PASSWORD ) ) {
                URL url;
                try {
                    String keystoreURL = clientInfo.getProperty( PROPERTY_KEYSTORE_URL );
                    if ( keystoreURL.startsWith( "/" ) )
                        keystoreURL = "file://" + keystoreURL;
                    url = new URL( keystoreURL );
                }
                catch ( MalformedURLException e ) {
                    throw new SQLException( e );
                }
                String pass = clientInfo.getProperty( PROPERTY_KEYSTORE_PASSWORD );
                try {
                    builder.loadTrustMaterial( url, pass.toCharArray() );
                }
                catch ( CertificateException | IOException e ) {
                    throw new SQLException( e );
                }
            }
            else if ( insecureMode ) {
                builder.loadTrustMaterial( new TrustStrategy() {
                    public boolean isTrusted( X509Certificate[] x509Certificates, String authType )
                        throws CertificateException {
                        return true;
                    }
                } );
            }
            else {
                throw new SQLException( "Cannot find TLS trust material, use keystore, certificate fingerprints, or set insecureMode to true" );
            }

            HostnameVerifier hostnameVerifier;

            if ( insecureMode || noHostnameVerification ) {
                hostnameVerifier = new HostnameVerifier() {
                    @Override
                    public boolean verify( String s, SSLSession sslSession ) {
                        return true;
                    }
                };
            }
            else {
                hostnameVerifier = SSLConnectionSocketFactory.getDefaultHostnameVerifier();
            }

            SSLConnectionSocketFactory sslConnectionSocketFactory = new SSLConnectionSocketFactory( builder.build(),
                                                                                                    hostnameVerifier );
            CredentialsProvider credsProvider = new BasicCredentialsProvider();
            credsProvider.setCredentials( new AuthScope( hostname, port ),
                                          new UsernamePasswordCredentials( username, password ) );

            RegistryBuilder registryBuilder = RegistryBuilder.create();
            registryBuilder.register( "https", sslConnectionSocketFactory );

            PoolingHttpClientConnectionManager httpClientConnectionManager = new PoolingHttpClientConnectionManager( registryBuilder
                .build() );
            httpClientConnectionManager.setDefaultMaxPerRoute( concurrentStatements );

            return HttpClients.custom().setConnectionManager( httpClientConnectionManager )
                .setDefaultCredentialsProvider( credsProvider ).build();
        }
        catch ( NoSuchAlgorithmException | KeyStoreException | KeyManagementException e ) {
            throw new SQLException( "Cannot initialize http client", e );
        }

    }

    /** access to the executor service for the connection */
    ExecutorService getExecutor() {
        return this.executor;
    }

    private void checkClosed()
        throws SQLException {
        if ( isClosed() ) {
            System.err.println( "Try to do something on a closed connection" );
            throw new SQLException( "Connection is closed" );
        }
    }

    public Statement createStatement()
        throws SQLException {
        return createStatement( TYPE_FORWARD_ONLY, CONCUR_READ_ONLY );
    }

    public PreparedStatement prepareStatement( String sql )
        throws SQLException {
        return prepareStatement( sql, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY );
    }

    public CallableStatement prepareCall( String sql )
        throws SQLException {
        checkClosed();
        throw new UnsupportedOperationException( "prepareCall" );
    }

    public String nativeSQL( String sql )
        throws SQLException {
        checkClosed();
        return sql;
    }

    public void setAutoCommit( boolean autoCommit )
        throws SQLException {
        checkClosed();
        this.autoCommit = autoCommit;
    }

    public boolean getAutoCommit()
        throws SQLException {
        checkClosed();
        return autoCommit;
    }

    public void commit()
        throws SQLException {
        checkClosed();
        if ( autoCommit ) {
            throw new SQLException( "Connection is in auto-commit mode" );
        }
    }

    public void rollback()
        throws SQLException {
        checkClosed();
        if ( autoCommit ) {
            throw new SQLException( "Connection is in auto-commit mode" );
        }
    }

    public void close()
        throws SQLException {
        if ( closed )
            return;
        closed = true;
        try {
            client.close();
        }
        catch ( IOException e ) {
            throw new SQLException( "Failed to close the connection.", e );
        }
        finally {
            this.executor.shutdown();
        }
    }

    public boolean isClosed()
        throws SQLException {
        return closed;
    }

    public DatabaseMetaData getMetaData()
        throws SQLException {
        checkClosed();
        return new LmiDatabaseMetaData( this );
    }

    public void setReadOnly( boolean readOnly )
        throws SQLException {
        checkClosed();
        if ( !readOnly ) {
            throw new SQLFeatureNotSupportedException( "Disabling read-only mode not supported" );
        }
    }

    public boolean isReadOnly()
        throws SQLException {
        checkClosed();
        return true;
    }

    public void setCatalog( String catalog )
        throws SQLException {
        checkClosed();
    }

    public String getCatalog()
        throws SQLException {
        checkClosed();
        return "DEFAULT";
    }

    public void setTransactionIsolation( int level )
        throws SQLException {
        checkClosed();
        switch ( level ) {
            case TRANSACTION_READ_UNCOMMITTED:
            case TRANSACTION_READ_COMMITTED:
            case TRANSACTION_REPEATABLE_READ:
            case TRANSACTION_SERIALIZABLE:
                throw new SQLFeatureNotSupportedException( "Transactions are not supported" );
            case TRANSACTION_NONE:
                throw new SQLFeatureNotSupportedException( "TRANSACTION_NONE is not a valid transaction level" );
            default:
                throw new SQLException( "Invalid transaction level: " + level );
        }
    }

    public int getTransactionIsolation()
        throws SQLException {
        checkClosed();
        return TRANSACTION_NONE;
    }

    public SQLWarning getWarnings()
        throws SQLException {
        checkClosed();
        return null;
    }

    public void clearWarnings()
        throws SQLException {
        checkClosed();
    }

    public Statement createStatement( int resultSetType, int resultSetConcurrency )
        throws SQLException {
        return createStatement( resultSetType, resultSetConcurrency, HOLD_CURSORS_OVER_COMMIT );
    }

    public PreparedStatement prepareStatement( String sql, int resultSetType, int resultSetConcurrency )
        throws SQLException {
        return prepareStatement( sql, resultSetType, resultSetConcurrency, HOLD_CURSORS_OVER_COMMIT );
    }

    public CallableStatement prepareCall( String sql, int resultSetType, int resultSetConcurrency )
        throws SQLException {
        throw new UnsupportedOperationException( "prepareCall" );
    }

    public Map<String, Class<?>> getTypeMap()
        throws SQLException {
        throw new UnsupportedOperationException( "getTypeMap" );
    }

    public void setTypeMap( Map<String, Class<?>> map )
        throws SQLException {
        throw new UnsupportedOperationException( "setTypeMap" );

    }

    public void setHoldability( int holdability )
        throws SQLException {
        checkClosed();
        switch ( holdability ) {
            case HOLD_CURSORS_OVER_COMMIT:
                break;
            case CLOSE_CURSORS_AT_COMMIT:
                throw new SQLFeatureNotSupportedException( "CLOSE_CURSORS_AT_COMMIT not supported" );
            default:
                throw new SQLException( "Invalid cursor holdability: " + holdability );
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see java.sql.Connection#getHoldability() HOLD_CURSORS_OVER_COMMIT: ResultSet cursors are not closed; they are
     * holdable: they are held open when the method commit is called. Holdable cursors might be ideal if your
     * application uses mostly read-only ResultSet objects.
     */
    public int getHoldability()
        throws SQLException {
        checkClosed();
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    public Savepoint setSavepoint()
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "setSavepoint" );
    }

    public Savepoint setSavepoint( String name )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "setSavepoint(String)" );
    }

    public void rollback( Savepoint savepoint )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "rollback(Savepoint)" );
    }

    public void releaseSavepoint( Savepoint savepoint )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "releaseSavepoint" );
    }

    public Statement createStatement( int resultSetType, int resultSetConcurrency, int resultSetHoldability )
        throws SQLException {
        checkClosed();
        switch ( resultSetType ) {
            case TYPE_FORWARD_ONLY:
                break;
            case TYPE_SCROLL_SENSITIVE:
            case TYPE_SCROLL_INSENSITIVE:
                throw new SQLFeatureNotSupportedException( "Resultset scrolling type not supported" );
            default:
                throw new SQLException( "Invalid result set type: " + resultSetType );
        }
        switch ( resultSetConcurrency ) {
            case CONCUR_READ_ONLY:
                break;
            case CONCUR_UPDATABLE:
                throw new SQLFeatureNotSupportedException( "Resultset concurrency UPDATABLE not supported" );
            default:
                throw new SQLException( "Invalid result set concurrency: " + resultSetConcurrency );
        }
        switch ( resultSetHoldability ) {
            case HOLD_CURSORS_OVER_COMMIT:
                break;
            case CLOSE_CURSORS_AT_COMMIT:
                throw new SQLFeatureNotSupportedException( "CLOSE_CURSORS_AT_COMMIT not supported" );
            default:
                throw new SQLException( "Invalid cursor holdability: " + resultSetHoldability );
        }
        return new LmiStatement( this, resultSetType );
    }

    public PreparedStatement prepareStatement( String sql, int resultSetType, int resultSetConcurrency,
                                               int resultSetHoldability )
        throws SQLException {
        checkClosed();
        switch ( resultSetType ) {
            case TYPE_FORWARD_ONLY:
                break;
            case TYPE_SCROLL_SENSITIVE:
            case TYPE_SCROLL_INSENSITIVE:
                throw new SQLFeatureNotSupportedException( "Resultset scrolling type not supported" );
            default:
                throw new SQLException( "Invalid result set type: " + resultSetType );
        }
        switch ( resultSetConcurrency ) {
            case CONCUR_READ_ONLY:
                break;
            case CONCUR_UPDATABLE:
                throw new SQLFeatureNotSupportedException( "Resultset concurrency UPDATABLE not supported" );
            default:
                throw new SQLException( "Invalid result set concurrency: " + resultSetConcurrency );
        }
        switch ( resultSetHoldability ) {
            case HOLD_CURSORS_OVER_COMMIT:
                break;
            case CLOSE_CURSORS_AT_COMMIT:
                throw new SQLFeatureNotSupportedException( "CLOSE_CURSORS_AT_COMMIT not supported" );
            default:
                throw new SQLException( "Invalid cursor holdability: " + resultSetHoldability );
        }
        return new LmiPreparedStatement( this, sql, resultSetType );
    }

    public CallableStatement prepareCall( String sql, int resultSetType, int resultSetConcurrency,
                                          int resultSetHoldability )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "prepareCall" );
    }

    public PreparedStatement prepareStatement( String sql, int autoGeneratedKeys )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "prepareStatement" );
    }

    public PreparedStatement prepareStatement( String sql, int[] columnIndexes )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "prepareStatement" );
    }

    public PreparedStatement prepareStatement( String sql, String[] columnNames )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "prepareStatement" );
    }

    public Clob createClob()
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "createClob" );
    }

    public Blob createBlob()
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "createBlob" );
    }

    public NClob createNClob()
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "createNClob" );
    }

    public SQLXML createSQLXML()
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "createSQLXML" );
    }

    public boolean isValid( int timeout )
        throws SQLException {
        if ( closed )
            return false;
        try {
            checkConnection( timeout );
        }
        catch ( Exception e ) {
            return false;
        }
        return true;
    }

    public void setClientInfo( String name, String value )
        throws SQLClientInfoException {
        if ( value != null ) {
            this.clientInfo.put( name, value );
        }
        else {
            this.clientInfo.remove( name );
        }
    }

    public void setClientInfo( Properties properties )
        throws SQLClientInfoException {
        clientInfo.clear();
        clientInfo.putAll( properties );
    }

    public String getClientInfo( String name )
        throws SQLException {
        return String.valueOf( clientInfo.get( name ) );
    }

    public Properties getClientInfo()
        throws SQLException {
        Properties properties = new Properties();
        properties.putAll( clientInfo );
        return properties;
    }

    public Array createArrayOf( String typeName, Object[] elements )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "createArrayOf" );
    }

    public Struct createStruct( String typeName, Object[] attributes )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "createStruct" );
    }

    public void setSchema( String schema )
        throws SQLException {
        checkClosed();
    }

    public String getSchema()
        throws SQLException {
        checkClosed();
        return "DEFAULT";
    }

    public void abort( Executor executor )
        throws SQLException {
        close();
    }

    public void setNetworkTimeout( Executor executor, int milliseconds )
        throws SQLException {
        throw new SQLFeatureNotSupportedException( "setNetworkTimeout" );
    }

    public int getNetworkTimeout() {
        return this.networkTimeoutMillis;
    }

    @SuppressWarnings("unchecked")
    public <T> T unwrap( Class<T> iface )
        throws SQLException {
        if ( isWrapperFor( iface ) ) {
            return (T) this;
        }
        throw new SQLException( "No wrapper for " + iface );
    }

    public boolean isWrapperFor( Class<?> iface )
        throws SQLException {
        return iface.isInstance( this );
    }

    String getUrl() {
        return this.queryUrl;
    }

    ObjectMapper getObjectMapper() {
        return this.objectMapper;
    }

    String getUsername() {
        return this.username;
    }

    private static byte[] getFingerprint( String algorithm, X509Certificate cert )
        throws NoSuchAlgorithmException, CertificateEncodingException {
        MessageDigest md = MessageDigest.getInstance( algorithm );
        byte[] der = cert.getEncoded();
        md.update( der );
        return md.digest();
    }

    int getQueryTimeout() {
        return queryTimeout;
    }

    public String getBuildVersion() {
        return buildVersion;
    }
}
