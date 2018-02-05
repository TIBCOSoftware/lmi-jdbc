/*
 * Copyright (c) 2014-2018 TIBCO Software Inc. All Rights Reserved.
 * Licensed under a BSD-type license. See TIBCO LICENSE.txt for license text.
 */
package com.tibco.loglogic.lmi.jdbc;

import static java.sql.Connection.TRANSACTION_NONE;
import static java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;
import static java.sql.RowIdLifetime.ROWID_UNSUPPORTED;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Types;

import com.tibco.loglogic.lmi.jdbc.QueryPostExecutor.ColumnDesc;

/**
 * JDBC meta-data of the Apollo System
 */
public class LmiDatabaseMetaData
    implements java.sql.DatabaseMetaData {

    private final LmiConnection lmiConnection;

    public LmiDatabaseMetaData( LmiConnection lmiConnection ) {
        this.lmiConnection = lmiConnection;
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
    public boolean allProceduresAreCallable()
        throws SQLException {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable()
        throws SQLException {
        return true;
    }

    @Override
    public String getURL()
        throws SQLException {
        return lmiConnection.getUrl();
    }

    @Override
    public String getUserName()
        throws SQLException {
        return lmiConnection.getUsername();
    }

    @Override
    public boolean isReadOnly()
        throws SQLException {
        return true;
    }

    @Override
    public boolean nullsAreSortedHigh()
        throws SQLException {
        return true;
    }

    @Override
    public boolean nullsAreSortedLow()
        throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtStart()
        throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd()
        throws SQLException {
        return false;
    }

    @Override
    public String getDatabaseProductName()
        throws SQLException {
        return "TIBCO LogLogic LMI";
    }

    @Override
    public String getDatabaseProductVersion()
        throws SQLException {
        return lmiConnection.getBuildVersion();
    }

    @Override
    public String getDriverName()
        throws SQLException {
        return "TIBCO LogLogic LMI JDBC Driver";
    }

    @Override
    public String getDriverVersion()
        throws SQLException {
        return LmiJdbcDriver.getDriverVersion();
    }

    @Override
    public int getDriverMajorVersion() {
        return LmiJdbcDriver.VERSION_MAJOR;
    }

    @Override
    public int getDriverMinorVersion() {
        return LmiJdbcDriver.VERSION_MINOR;
    }

    @Override
    public boolean usesLocalFiles()
        throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable()
        throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers()
        throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseIdentifiers()
        throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers()
        throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers()
        throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers()
        throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers()
        throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers()
        throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers()
        throws SQLException {
        return false;
    }

    @Override
    public String getIdentifierQuoteString()
        throws SQLException {
        return "`";
    }

    @Override
    public String getSQLKeywords()
        throws SQLException {
        // Retrieves a comma-separated list of all of this database's SQL keywords that are NOT also SQL:2003 keywords.
        return "";
        // TODO implement properly
    }

    @Override
    public String getNumericFunctions()
        throws SQLException {
        // Retrieves a comma-separated list of math functions available with this database. These are the Open /Open CLI
        // math function names used in the JDBC function escape clause.
        return "";
        // TODO implement properly
    }

    @Override
    public String getStringFunctions()
        throws SQLException {
        // Retrieves a comma-separated list of string functions available with this database. These are the Open Group
        // CLI string function names used in the JDBC function escape clause.
        return "";
        // TODO implement properly
    }

    @Override
    public String getSystemFunctions()
        throws SQLException {
        // Retrieves a comma-separated list of system functions available with this database. These are the Open Group
        // CLI system function names used in the JDBC function escape clause.
        return "";
    }

    @Override
    public String getTimeDateFunctions()
        throws SQLException {
        // Retrieves a comma-separated list of the time and date functions available with this database.
        return "";
        // TODO implement properly
    }

    @Override
    public String getSearchStringEscape()
        throws SQLException {
        return "\\";
    }

    @Override
    public String getExtraNameCharacters()
        throws SQLException {
        // Retrieves all the "extra" characters that can be used in unquoted identifier names (those beyond a-z, A-Z,
        // 0-9 and _).
        return "";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn()
        throws SQLException {
        return false;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn()
        throws SQLException {
        return false;
    }

    @Override
    public boolean supportsColumnAliasing()
        throws SQLException {
        return true;
    }

    @Override
    public boolean nullPlusNonNullIsNull()
        throws SQLException {
        return true;
    }

    @Override
    public boolean supportsConvert()
        throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert( int fromType, int toType )
        throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames()
        throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames()
        throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy()
        throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOrderByUnrelated()
        throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupBy()
        throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated()
        throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByBeyondSelect()
        throws SQLException {
        return true;
    }

    @Override
    public boolean supportsLikeEscapeClause()
        throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleResultSets()
        throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions()
        throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns()
        throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMinimumSQLGrammar()
        throws SQLException {
        // Retrieves whether this database supports the ODBC Minimum SQL grammar.
        return false;
    }

    @Override
    public boolean supportsCoreSQLGrammar()
        throws SQLException {
        // Retrieves whether this database supports the ODBC Core SQL grammar.
        return false;
    }

    @Override
    public boolean supportsExtendedSQLGrammar()
        throws SQLException {
        // Retrieves whether this database supports the ODBC Extended SQL grammar.
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL()
        throws SQLException {
        // Retrieves whether this database supports the ANSI92 entry level SQL grammar.
        return false;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL()
        throws SQLException {
        // Retrieves whether this database supports the ANSI92 intermediate SQL grammar supported.
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL()
        throws SQLException {
        // Retrieves whether this database supports the ANSI92 full SQL grammar supported.
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility()
        throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOuterJoins()
        throws SQLException {
        return false;
    }

    @Override
    public boolean supportsFullOuterJoins()
        throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins()
        throws SQLException {
        return false;
    }

    @Override
    public String getSchemaTerm()
        throws SQLException {
        return "";
    }

    @Override
    public String getProcedureTerm()
        throws SQLException {
        return "";
    }

    @Override
    public String getCatalogTerm()
        throws SQLException {
        return "";
    }

    @Override
    public boolean isCatalogAtStart()
        throws SQLException {
        return true;
    }

    @Override
    public String getCatalogSeparator()
        throws SQLException {
        return ".";
    }

    @Override
    public boolean supportsSchemasInDataManipulation()
        throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls()
        throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions()
        throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions()
        throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions()
        throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation()
        throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls()
        throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions()
        throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions()
        throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions()
        throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete()
        throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate()
        throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate()
        throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures()
        throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons()
        throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInExists()
        throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns()
        throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds()
        throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries()
        throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnion()
        throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnionAll()
        throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit()
        throws SQLException {
        return true;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback()
        throws SQLException {
        return true;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit()
        throws SQLException {
        return true;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback()
        throws SQLException {
        return true;
    }

    @Override
    public int getMaxBinaryLiteralLength()
        throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength()
        throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength()
        throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInGroupBy()
        throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex()
        throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy()
        throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect()
        throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable()
        throws SQLException {
        return 0;
    }

    @Override
    public int getMaxConnections()
        throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength()
        throws SQLException {
        return 0;
    }

    @Override
    public int getMaxIndexLength()
        throws SQLException {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength()
        throws SQLException {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength()
        throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength()
        throws SQLException {
        return 0;
    }

    @Override
    public int getMaxRowSize()
        throws SQLException {
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs()
        throws SQLException {
        return false;
    }

    @Override
    public int getMaxStatementLength()
        throws SQLException {
        return 0;
    }

    @Override
    public int getMaxStatements()
        throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTableNameLength()
        throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTablesInSelect()
        throws SQLException {
        return 1;
    }

    @Override
    public int getMaxUserNameLength()
        throws SQLException {
        return 0;
    }

    @Override
    public int getDefaultTransactionIsolation()
        throws SQLException {
        return TRANSACTION_NONE;
    }

    @Override
    public boolean supportsTransactions()
        throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel( int level )
        throws SQLException {
        return level == TRANSACTION_NONE;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions()
        throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly()
        throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit()
        throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions()
        throws SQLException {
        return false;
    }

    @Override
    public ResultSet getProcedures( String catalog, String schemaPattern, String procedureNamePattern )
        throws SQLException {

        System.out.println( "getProcedures" );

        ColumnDesc[] columnDescs = new ColumnDesc[] {
            new ColumnDesc( "PROCEDURE_CAT", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "PROCEDURE_SCHEM", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "PROCEDURE_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "RESERVED_1", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "RESERVED_2", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "RESERVED_3", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "REMARKS", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "PROCEDURE_TYPE", Integer.toString( Types.SMALLINT ) ),
            new ColumnDesc( "SPECIFIC_NAME", Integer.toString( Types.VARCHAR ) ), };

        String[][] values = new String[][] {};

        return new LmiResultSet( columnDescs, values );
    }

    @Override
    public ResultSet getProcedureColumns( String catalog, String schemaPattern, String procedureNamePattern,
                                          String columnNamePattern )
        throws SQLException {

        System.out.println( "getProcedureColumns" );

        ColumnDesc[] columnDescs = new ColumnDesc[] {
            new ColumnDesc( "PROCEDURE_CAT", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "PROCEDURE_SCHEM", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "PROCEDURE_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "COLUMN_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "COLUMN_TYPE", Integer.toString( Types.SMALLINT ) ),
            new ColumnDesc( "DATA_TYPE", Integer.toString( Types.INTEGER ) ),
            new ColumnDesc( "TYPE_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "PRECISION", Integer.toString( Types.INTEGER ) ),
            new ColumnDesc( "LENGTH", Integer.toString( Types.INTEGER ) ),
            new ColumnDesc( "SCALE", Integer.toString( Types.SMALLINT ) ),
            new ColumnDesc( "RADIX", Integer.toString( Types.SMALLINT ) ),
            new ColumnDesc( "NULLABLE", Integer.toString( Types.SMALLINT ) ),
            new ColumnDesc( "REMARKS", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "COLUMN_DEF", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "SQL_DATA_TYPE", Integer.toString( Types.INTEGER ) ),
            new ColumnDesc( "SQL_DATETIME_SUB", Integer.toString( Types.INTEGER ) ),
            new ColumnDesc( "CHAR_OCTET_LENGTH", Integer.toString( Types.INTEGER ) ),
            new ColumnDesc( "ORDINAL_POSITION", Integer.toString( Types.INTEGER ) ),
            new ColumnDesc( "IS_NULLABLE", Integer.toString( Types.VARCHAR ) ),

            new ColumnDesc( "SPECIFIC_NAME", Integer.toString( Types.VARCHAR ) ), };

        String[][] values = new String[][] {};

        return new LmiResultSet( columnDescs, values );
    }

    @Override
    public ResultSet getTables( String catalog, String schemaPattern, String tableNamePattern, String[] types )
        throws SQLException {

        System.out.println( "getTables" );

        // ignore catalog and schema, and return the tables for the specified table pattern
        final String useStatement = "USE LogLogic_Config_Models ";
        final String columnsStatement = "| COLUMNS " + "ToString('DEFAULT') AS TABLE_CAT, "
            + "ToString('DEFAULT') AS TABLE_SCHEM," + "llc_name AS TABLE_NAME," + "ToString('TABLE') AS TABLE_TYPE,"
            + "ToNull() AS REMARKS," + "ToNull() AS TYPE_CAT," + "ToNull() AS TYPE_SCHEM," + "ToNull() AS TYPE_NAME,"
            + "ToNull() AS SELF_REFERENCING_COL_NAME," + "ToNull() AS REF_GENERATION";
        final String tableCondition = tableNamePattern != null ? "| llc_name LIKE ? " : "";
        final String sortStatement = " | SORT BY llc_name ";

        final String query = useStatement + columnsStatement + tableCondition + sortStatement;
        final PreparedStatement ps = getConnection().prepareStatement( query );
        if ( tableNamePattern != null ) {
            ps.setString( 1, tableNamePattern );
        }

        ResultSet rs = ps.executeQuery();

        return rs;
    }

    @Override
    public ResultSet getSchemas()
        throws SQLException {

        System.out.println( "getSchemas" );

        // return just one schema, called default, belonging to catalog default
        ColumnDesc[] columnDescs = new ColumnDesc[] {
            new ColumnDesc( "TABLE_SCHEM", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "TABLE_CATALOG", Integer.toString( Types.VARCHAR ) ), };

        String[][] values = new String[][] { new String[] { "DEFAULT", "DEFAULT" } };

        return new LmiResultSet( columnDescs, values );
    }

    @Override
    public ResultSet getCatalogs()
        throws SQLException {
        // return just one catalog, called default

        System.out.println( "getCatalogs" );

        ColumnDesc[] columnDescs = new ColumnDesc[] {
            new ColumnDesc( "TABLE_CAT", Integer.toString( Types.VARCHAR ) ), };

        String[][] values = new String[][] { new String[] { "DEFAULT" } };
        return new LmiResultSet( columnDescs, values );
    }

    @Override
    public ResultSet getTableTypes()
        throws SQLException {
        // return just one table type, called table

        System.out.println( "getTableTypes" );

        ColumnDesc[] columnDescs = new ColumnDesc[] {
            new ColumnDesc( "TABLE_TYPE", Integer.toString( Types.VARCHAR ) ), };

        String[][] values = new String[][] { new String[] { "TABLE" } };
        return new LmiResultSet( columnDescs, values );
    }

    @Override
    public ResultSet getColumns( String catalog, String schemaPattern, String tableNamePattern,
                                 String columnNamePattern )
        throws SQLException {

        System.out.println( "getColumns" );

        // ignore catalog and schema, and return the columns for the specified table and column patterns
        final String useStatement = "USE LogLogic_Config_Model_Columns ";
        final String columnsStatement = "| COLUMNS " + "ToString('DEFAULT') AS TABLE_CAT, "
            + "ToString('PUBLIC') AS TABLE_SCHEM," + "llc_model AS TABLE_NAME," + "llc_name AS COLUMN_NAME,"
            + "IIF(llc_type='STRING',12,IIF(llc_type='TIMESTAMP',93,IIF(llc_type='INT',4,IIF(llc_type='LONG',-5,IIF(llc_type='DOUBLE',8,IIF(llc_type='BOOLEAN',16,IIF(llc_type='INET_ADDR',12,ToNull()))))))) AS DATA_TYPE,"
            + "llc_type AS TYPE_NAME," + "ToNull() AS COLUMN_SIZE," + "ToNull() AS BUFFER_LENGTH,"
            + "ToNull() AS DECIMAL_DIGITS," + "ToNull() AS NUM_PREC_RADIX," + "ToInt(1) AS NULLABLE,"
            + "llc_description AS REMARKS," + "ToNull() AS COLUMN_DEF," + "ToNull() AS SQL_DATA_TYPE,"
            + "ToNull() AS SQL_DATETIME_SUB," + "ToNull() AS CHAR_OCTET_LENGTH," + "ToNull() AS ORDINAL_POSITION,"
            + "ToString('YES') AS IS_NULLABLE," + "ToNull() AS SCOPE_CATALOG," + "ToNull() AS SCOPE_SCHEMA,"
            + "ToNull() AS SCOPE_TABLE," + "ToNull() AS SOURCE_DATA_TYPE," + "ToString('NO') AS IS_AUTOINCREMENT,"
            + "ToString('NO') AS IS_GENERATEDCOLUMN";
        final String tableCondition = tableNamePattern != null ? "| llc_model LIKE ? " : "";
        final String columnCondition = columnNamePattern != null ? "| llc_name LIKE ? " : "";
        final String sortStatement = " | SORT BY llc_name ";

        final String query = useStatement + columnsStatement + tableCondition + columnCondition + sortStatement;
        final PreparedStatement ps = getConnection().prepareStatement( query );
        if ( tableNamePattern != null ) {
            ps.setString( 1, tableNamePattern );
        }
        if ( columnNamePattern != null ) {
            ps.setString( tableNamePattern == null ? 1 : 2, columnNamePattern );
        }

        ResultSet rs = ps.executeQuery();

        return rs;

    }

    @Override
    public ResultSet getColumnPrivileges( String catalog, String schema, String table, String columnNamePattern )
        throws SQLException {

        System.out.println( "getColumnPrivileges" );

        // ignore catalog and schema, and return the columns for the specified table and column patterns
        final String useStatement = "USE LogLogic_Config_Model_Columns ";
        final String columnsStatement = "| COLUMNS " + "ToString('DEFAULT') AS TABLE_CAT, "
            + "ToString('PUBLIC') AS TABLE_SCHEM," + "llc_model AS TABLE_NAME," + "llc_name AS COLUMN_NAME,"
            + "ToNull() AS GRANTOR," + "ToString('ALL') AS GRANTEE," + "ToString('SELECT') AS PRIVILEGE,"
            + "ToString('No') AS IS_GRANTABLE";
        final String tableCondition = table != null ? "| llc_model EQUALS ? " : "";
        final String columnCondition = columnNamePattern != null ? "| llc_name EQUALS ? " : "";
        final String sortStatement = " | SORT BY llc_name ";

        final String query = useStatement + columnsStatement + tableCondition + columnCondition + sortStatement;
        final PreparedStatement ps = getConnection().prepareStatement( query );
        if ( table != null ) {
            ps.setString( 1, table );
        }
        if ( columnNamePattern != null ) {
            ps.setString( table == null ? 1 : 2, columnNamePattern );
        }

        final ResultSet rs = ps.executeQuery();

        return rs;
    }

    @Override
    public ResultSet getTablePrivileges( String catalog, String schemaPattern, String tableNamePattern )
        throws SQLException {

        System.out.println( "getTablePrivileges" );

        // ignore catalog and schema, and return the tables for the specified table pattern
        final String useStatement = "USE LogLogic_Config_Models ";
        final String columnsStatement = "| COLUMNS " + "ToString('DEFAULT') AS TABLE_CAT, "
            + "ToString('DEFAULT') AS TABLE_SCHEM," + "llc_name AS TABLE_NAME," + "ToNull() AS GRANTOR,"
            + "ToString('ALL') AS GRANTEE," + "ToString('SELECT') AS PRIVILEGE," + "ToString('No') AS IS_GRANTABLE";;
        final String tableCondition = tableNamePattern != null ? "| llc_name LIKE ? " : "";
        final String sortStatement = " | SORT BY llc_name ";

        final String query = useStatement + columnsStatement + tableCondition + sortStatement;
        final PreparedStatement ps = getConnection().prepareStatement( query );
        if ( tableNamePattern != null ) {
            ps.setString( 1, tableNamePattern );
        }

        ResultSet rs = ps.executeQuery();

        return rs;
    }

    @Override
    public ResultSet getBestRowIdentifier( String catalog, String schema, String table, int scope, boolean nullable )
        throws SQLException {

        System.out.println( "getBestRowIdentifier" );

        ColumnDesc[] columnDescs = new ColumnDesc[] {
            new ColumnDesc( "SCOPE", Integer.toString( Types.SMALLINT ) ),
            new ColumnDesc( "COLUMN_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "DATA_TYPE", Integer.toString( Types.INTEGER ) ),
            new ColumnDesc( "TYPE_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "COLUMN_SIZE", Integer.toString( Types.INTEGER ) ),
            new ColumnDesc( "BUFFER_LENGTH", Integer.toString( Types.INTEGER ) ),
            new ColumnDesc( "DECIMAL_DIGITS", Integer.toString( Types.SMALLINT ) ),
            new ColumnDesc( "PSEUDO_COLUMN", Integer.toString( Types.SMALLINT ) ), };

        String[][] values = new String[][] {};

        return new LmiResultSet( columnDescs, values );
    }

    @Override
    public ResultSet getVersionColumns( String catalog, String schema, String table )
        throws SQLException {

        System.out.println( "getVersionColumns" );

        ColumnDesc[] columnDescs = new ColumnDesc[] {
            new ColumnDesc( "SCOPE", Integer.toString( Types.SMALLINT ) ),
            new ColumnDesc( "COLUMN_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "DATA_TYPE", Integer.toString( Types.INTEGER ) ),
            new ColumnDesc( "TYPE_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "COLUMN_SIZE", Integer.toString( Types.INTEGER ) ),
            new ColumnDesc( "BUFFER_LENGTH", Integer.toString( Types.INTEGER ) ),
            new ColumnDesc( "DECIMAL_DIGITS", Integer.toString( Types.SMALLINT ) ),
            new ColumnDesc( "PSEUDO_COLUMN", Integer.toString( Types.SMALLINT ) ), };

        String[][] values = new String[][] {};

        return new LmiResultSet( columnDescs, values );
    }

    @Override
    public ResultSet getPrimaryKeys( String catalog, String schema, String table )
        throws SQLException {

        System.out.println( "getPrimaryKeys" );

        ColumnDesc[] columnDescs = new ColumnDesc[] {
            new ColumnDesc( "TABLE_CAT", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "TABLE_SCHEM", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "TABLE_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "COLUMN_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "KEY_SEQ", Integer.toString( Types.SMALLINT ) ),
            new ColumnDesc( "PK_NAME", Integer.toString( Types.VARCHAR ) ), };

        String[][] values = new String[][] {};

        return new LmiResultSet( columnDescs, values );
    }

    @Override
    public ResultSet getImportedKeys( String catalog, String schema, String table )
        throws SQLException {

        System.out.println( "getImportedKeys" );

        ColumnDesc[] columnDescs = new ColumnDesc[] {
            new ColumnDesc( "PKTABLE_CAT", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "PKTABLE_SCHEM", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "PKTABLE_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "PKCOLUMN_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "FKTABLE_CAT", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "FKTABLE_SCHEM", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "FKTABLE_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "FKCOLUMN_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "KEY_SEQ", Integer.toString( Types.SMALLINT ) ),
            new ColumnDesc( "UPDATE_RULE", Integer.toString( Types.SMALLINT ) ),
            new ColumnDesc( "DELETE_RULE", Integer.toString( Types.SMALLINT ) ),
            new ColumnDesc( "FK_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "PK_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "DEFERRABILITY", Integer.toString( Types.SMALLINT ) ), };

        String[][] values = new String[][] {};

        return new LmiResultSet( columnDescs, values );
    }

    @Override
    public ResultSet getExportedKeys( String catalog, String schema, String table )
        throws SQLException {

        System.out.println( "getExportedKeys" );

        ColumnDesc[] columnDescs = new ColumnDesc[] {
            new ColumnDesc( "PKTABLE_CAT", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "PKTABLE_SCHEM", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "PKTABLE_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "PKCOLUMN_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "FKTABLE_CAT", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "FKTABLE_SCHEM", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "FKTABLE_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "FKCOLUMN_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "KEY_SEQ", Integer.toString( Types.SMALLINT ) ),
            new ColumnDesc( "UPDATE_RULE", Integer.toString( Types.SMALLINT ) ),
            new ColumnDesc( "DELETE_RULE", Integer.toString( Types.SMALLINT ) ),
            new ColumnDesc( "FK_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "PK_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "DEFERRABILITY", Integer.toString( Types.SMALLINT ) ), };

        String[][] values = new String[][] {};

        return new LmiResultSet( columnDescs, values );
    }

    @Override
    public ResultSet getCrossReference( String parentCatalog, String parentSchema, String parentTable,
                                        String foreignCatalog, String foreignSchema, String foreignTable )
        throws SQLException {

        System.out.println( "getCrossReference" );

        ColumnDesc[] columnDescs = new ColumnDesc[] {
            new ColumnDesc( "PKTABLE_CAT", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "PKTABLE_SCHEM", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "PKTABLE_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "PKCOLUMN_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "FKTABLE_CAT", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "FKTABLE_SCHEM", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "FKTABLE_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "FKCOLUMN_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "KEY_SEQ", Integer.toString( Types.SMALLINT ) ),
            new ColumnDesc( "UPDATE_RULE", Integer.toString( Types.SMALLINT ) ),
            new ColumnDesc( "DELETE_RULE", Integer.toString( Types.SMALLINT ) ),
            new ColumnDesc( "FK_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "PK_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "DEFERRABILITY", Integer.toString( Types.SMALLINT ) ), };

        String[][] values = new String[][] {};

        return new LmiResultSet( columnDescs, values );
    }

    @Override
    public ResultSet getTypeInfo()
        throws SQLException {

        System.out.println( "getTypeInfo" );

        ColumnDesc[] columnDescs = new ColumnDesc[] {
            new ColumnDesc( "TYPE_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "DATA_TYPE", Integer.toString( Types.INTEGER ) ),
            new ColumnDesc( "PRECISION", Integer.toString( Types.INTEGER ) ),
            new ColumnDesc( "LITERAL_PREFIX", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "LITERAL_SUFFIX", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "CREATE_PARAMS", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "NULLABLE", Integer.toString( Types.SMALLINT ) ),
            new ColumnDesc( "CASE_SENSITIVE", Integer.toString( Types.BOOLEAN ) ),
            new ColumnDesc( "SEARCHABLE", Integer.toString( Types.SMALLINT ) ),
            new ColumnDesc( "UNSIGNED_ATTRIBUTE", Integer.toString( Types.BOOLEAN ) ),
            new ColumnDesc( "FIXED_PREC_SCALE", Integer.toString( Types.BOOLEAN ) ),
            new ColumnDesc( "AUTO_INCREMENT", Integer.toString( Types.BOOLEAN ) ),
            new ColumnDesc( "LOCAL_TYPE_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "MINIMUM_SCALE", Integer.toString( Types.SMALLINT ) ),
            new ColumnDesc( "MAXIMUM_SCALE", Integer.toString( Types.SMALLINT ) ),
            new ColumnDesc( "SQL_DATA_TYPE", Integer.toString( Types.INTEGER ) ),
            new ColumnDesc( "SQL_DATETIME_SUB", Integer.toString( Types.SMALLINT ) ),
            new ColumnDesc( "NUM_PREC_RADIX", Integer.toString( Types.INTEGER ) ), };

        String[][] values = new String[][] {
            new String[] {
                "STRING",
                "12",
                null,
                "'",
                "'",
                null,
                Integer.toString( typeNullable ),
                "false",
                Integer.toString( typeSearchable ),
                "false",
                "false",
                "false",
                null,
                "0",
                "1000",
                null,
                null,
                "10" },
            new String[] {
                "TIMESTAMP",
                "93",
                "20",
                "'",
                "'",
                null,
                Integer.toString( typeNullable ),
                "false",
                Integer.toString( typeSearchable ),
                "false",
                "false",
                "false",
                null,
                "0",
                "1000",
                null,
                null,
                "10" },
            new String[] {
                "INT",
                "4",
                "20",
                null,
                null,
                null,
                Integer.toString( typeNullable ),
                "false",
                Integer.toString( typeSearchable ),
                "false",
                "false",
                "false",
                null,
                "0",
                "1000",
                null,
                null,
                "10" },
            new String[] {
                "LONG",
                "-5",
                "40",
                null,
                null,
                null,
                Integer.toString( typeNullable ),
                "false",
                Integer.toString( typeSearchable ),
                "false",
                "false",
                "false",
                null,
                "0",
                "1000",
                null,
                null,
                "10" },
            new String[] {
                "DOUBLE",
                "8",
                "40",
                null,
                null,
                null,
                Integer.toString( typeNullable ),
                "false",
                Integer.toString( typeSearchable ),
                "false",
                "false",
                "false",
                null,
                "0",
                "1000",
                null,
                null,
                "10" },
            new String[] {
                "BOOLEAN",
                "16",
                "2",
                null,
                null,
                null,
                Integer.toString( typeNullable ),
                "false",
                Integer.toString( typeSearchable ),
                "false",
                "false",
                "false",
                null,
                "0",
                "1000",
                null,
                null,
                "10" },
            new String[] {
                "INET_ADDR",
                "-10",
                "40",
                "'",
                "'",
                null,
                Integer.toString( typeNullable ),
                "false",
                Integer.toString( typeSearchable ),
                "false",
                "false",
                "false",
                null,
                "0",
                "1000",
                null,
                null,
                "10" } };

        return new LmiResultSet( columnDescs, values );
    }

    @Override
    public ResultSet getIndexInfo( String catalog, String schema, String table, boolean unique, boolean approximate )
        throws SQLException {

        System.out.println( "getIndexInfo" );

        ColumnDesc[] columnDescs = new ColumnDesc[] {
            new ColumnDesc( "TABLE_CAT", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "TABLE_SCHEM", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "TABLE_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "NON_UNIQUE", Integer.toString( Types.BOOLEAN ) ),
            new ColumnDesc( "INDEX_QUALIFIER", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "INDEX_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "TYPE", Integer.toString( Types.SMALLINT ) ),
            new ColumnDesc( "ORDINAL_POSITION", Integer.toString( Types.SMALLINT ) ),
            new ColumnDesc( "COLUMN_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "ASC_OR_DESC", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "CARDINALITY", Integer.toString( Types.INTEGER ) ),
            new ColumnDesc( "PAGES", Integer.toString( Types.INTEGER ) ),
            new ColumnDesc( "FILTER_CONDITION", Integer.toString( Types.VARCHAR ) ), };

        String[][] values = new String[][] {};

        return new LmiResultSet( columnDescs, values );
    }

    @Override
    public boolean supportsResultSetType( int type )
        throws SQLException {
        switch ( type ) {
            case ResultSet.TYPE_FORWARD_ONLY:
                return true;
            case ResultSet.TYPE_SCROLL_INSENSITIVE:
            case ResultSet.TYPE_SCROLL_SENSITIVE:
                return false;
            default:
                return false;
        }
    }

    @Override
    public boolean supportsResultSetConcurrency( int type, int concurrency )
        throws SQLException {
        switch ( type ) {
            case ResultSet.TYPE_SCROLL_INSENSITIVE:
            case ResultSet.TYPE_SCROLL_SENSITIVE:
                return false;
            case ResultSet.TYPE_FORWARD_ONLY:
                switch ( concurrency ) {
                    case ResultSet.CONCUR_READ_ONLY:
                        return true;
                    default:
                        return false;
                }
            default:
                return false;
        }
    }

    @Override
    public boolean ownUpdatesAreVisible( int type )
        throws SQLException {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible( int type )
        throws SQLException {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible( int type )
        throws SQLException {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible( int type )
        throws SQLException {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible( int type )
        throws SQLException {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible( int type )
        throws SQLException {
        return false;
    }

    @Override
    public boolean updatesAreDetected( int type )
        throws SQLException {
        return false;
    }

    @Override
    public boolean deletesAreDetected( int type )
        throws SQLException {
        return false;
    }

    @Override
    public boolean insertsAreDetected( int type )
        throws SQLException {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates()
        throws SQLException {
        return false;
    }

    @Override
    public ResultSet getUDTs( String catalog, String schemaPattern, String typeNamePattern, int[] types )
        throws SQLException {

        System.out.println( "getUDTs" );

        ColumnDesc[] columnDescs = new ColumnDesc[] {
            new ColumnDesc( "TYPE_CAT", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "TYPE_SCHEM", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "TYPE_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "CLASS_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "DATA_TYPE", Integer.toString( Types.INTEGER ) ),
            new ColumnDesc( "REMARKS", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "BASE_TYPE", Integer.toString( Types.SMALLINT ) ), };

        String[][] values = new String[][] {};

        return new LmiResultSet( columnDescs, values );
    }

    @Override
    public Connection getConnection()
        throws SQLException {
        return lmiConnection;
    }

    @Override
    public boolean supportsSavepoints()
        throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNamedParameters()
        throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults()
        throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys()
        throws SQLException {
        return false;
    }

    @Override
    public ResultSet getSuperTypes( String catalog, String schemaPattern, String typeNamePattern )
        throws SQLException {

        System.out.println( "getSuperTypes" );

        ColumnDesc[] columnDescs = new ColumnDesc[] {
            new ColumnDesc( "TYPE_CAT", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "TYPE_SCHEM", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "TYPE_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "SUPERTYPE_CAT", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "SUPERTYPE_SCHEM", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "SUPERTYPE_NAME", Integer.toString( Types.VARCHAR ) ), };

        String[][] values = new String[][] {};

        return new LmiResultSet( columnDescs, values );
    }

    @Override
    public ResultSet getSuperTables( String catalog, String schemaPattern, String tableNamePattern )
        throws SQLException {

        System.out.println( "getSuperTables" );

        ColumnDesc[] columnDescs = new ColumnDesc[] {
            new ColumnDesc( "TABLE_CAT", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "TABLE_CAT", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "TABLE_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "SUPERTABLE_NAME", Integer.toString( Types.VARCHAR ) ), };

        String[][] values = new String[][] {};

        return new LmiResultSet( columnDescs, values );
    }

    @Override
    public ResultSet getAttributes( String catalog, String schemaPattern, String typeNamePattern,
                                    String attributeNamePattern )
        throws SQLException {

        System.out.println( "getAttributes" );

        ColumnDesc[] columnDescs = new ColumnDesc[] {
            new ColumnDesc( "TYPE_CAT", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "TYPE_SCHEM", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "TYPE_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "ATTR_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "DATA_TYPE", Integer.toString( Types.INTEGER ) ),
            new ColumnDesc( "ATTR_TYPE_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "ATTR_SIZE", Integer.toString( Types.INTEGER ) ),
            new ColumnDesc( "DECIMAL_DIGITS", Integer.toString( Types.INTEGER ) ),
            new ColumnDesc( "NUM_PREC_RADIX", Integer.toString( Types.INTEGER ) ),
            new ColumnDesc( "NULLABLE", Integer.toString( Types.INTEGER ) ),
            new ColumnDesc( "REMARKS", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "ATTR_DEF", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "SQL_DATA_TYPE", Integer.toString( Types.INTEGER ) ),
            new ColumnDesc( "SQL_DATETIME_SUB", Integer.toString( Types.INTEGER ) ),
            new ColumnDesc( "CHAR_OCTET_LENGTH", Integer.toString( Types.INTEGER ) ),
            new ColumnDesc( "ORDINAL_POSITION", Integer.toString( Types.INTEGER ) ),
            new ColumnDesc( "IS_NULLABLE", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "SCOPE_CATALOG", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "SCOPE_SCHEMA", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "SCOPE_TABLE", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "SOURCE_DATA_TYPE", Integer.toString( Types.SMALLINT ) ), };

        String[][] values = new String[][] {};

        return new LmiResultSet( columnDescs, values );
    }

    @Override
    public boolean supportsResultSetHoldability( int holdability )
        throws SQLException {
        return holdability == HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getResultSetHoldability()
        throws SQLException {
        return HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getDatabaseMajorVersion()
        throws SQLException {
        String[] p = lmiConnection.getBuildVersion().split( "\\." );
        return Integer.parseInt( p[0] );
    }

    @Override
    public int getDatabaseMinorVersion()
        throws SQLException {
        String[] p = lmiConnection.getBuildVersion().split( "\\." );
        return Integer.parseInt( p[1] );
    }

    @Override
    public int getJDBCMajorVersion()
        throws SQLException {
        return LmiJdbcDriver.JDBC_VERSION_MAJOR;
    }

    @Override
    public int getJDBCMinorVersion()
        throws SQLException {
        return LmiJdbcDriver.JDBC_VERSION_MINOR;
    }

    @Override
    public int getSQLStateType()
        throws SQLException {
        return sqlStateSQL;
    }

    @Override
    public boolean locatorsUpdateCopy()
        throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStatementPooling()
        throws SQLException {
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime()
        throws SQLException {
        return ROWID_UNSUPPORTED;
    }

    @Override
    public ResultSet getSchemas( String catalog, String schemaPattern )
        throws SQLException {

        System.out.println( "getSchemas" );

        // return just one schema, called default, belonging to catalog default
        ColumnDesc[] columnDescs = new ColumnDesc[] {
            new ColumnDesc( "TABLE_SCHEM", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "TABLE_CATALOG", Integer.toString( Types.VARCHAR ) ), };

        String[][] values = new String[][] { new String[] { "DEFAULT", "DEFAULT" } };

        return new LmiResultSet( columnDescs, values );
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax()
        throws SQLException {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets()
        throws SQLException {
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties()
        throws SQLException {
        return null;
    }

    @Override
    public ResultSet getFunctions( String catalog, String schemaPattern, String functionNamePattern )
        throws SQLException {

        System.out.println( "getFunctions" );

        ColumnDesc[] columnDescs = new ColumnDesc[] {
            new ColumnDesc( "FUNCTION_CAT", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "FUNCTION_SCHEM", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "FUNCTION_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "REMARKS", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "FUNCTION_TYPE", Integer.toString( Types.SMALLINT ) ),
            new ColumnDesc( "SPECIFIC_NAME", Integer.toString( Types.VARCHAR ) ), };

        String[][] values = new String[][] {};

        return new LmiResultSet( columnDescs, values );
    }

    @Override
    public ResultSet getFunctionColumns( String catalog, String schemaPattern, String functionNamePattern,
                                         String columnNamePattern )
        throws SQLException {

        System.out.println( "getFunctionColumns" );

        ColumnDesc[] columnDescs = new ColumnDesc[] {
            new ColumnDesc( "FUNCTION_CAT", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "FUNCTION_SCHEM", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "FUNCTION_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "COLUMN_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "COLUMN_TYPE", Integer.toString( Types.SMALLINT ) ),
            new ColumnDesc( "DATA_TYPE", Integer.toString( Types.INTEGER ) ),
            new ColumnDesc( "TYPE_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "PRECISION", Integer.toString( Types.INTEGER ) ),
            new ColumnDesc( "LENGTH", Integer.toString( Types.INTEGER ) ),
            new ColumnDesc( "SCALE", Integer.toString( Types.SMALLINT ) ),
            new ColumnDesc( "RADIX", Integer.toString( Types.SMALLINT ) ),
            new ColumnDesc( "NULLABLE", Integer.toString( Types.SMALLINT ) ),
            new ColumnDesc( "REMARKS", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "CHAR_OCTET_LENGTH", Integer.toString( Types.INTEGER ) ),
            new ColumnDesc( "ORDINAL_POSITION", Integer.toString( Types.INTEGER ) ),
            new ColumnDesc( "IS_NULLABLE", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "SPECIFIC_NAME", Integer.toString( Types.INTEGER ) ), };

        String[][] values = new String[][] {};

        return new LmiResultSet( columnDescs, values );
    }

    @Override
    public ResultSet getPseudoColumns( String catalog, String schemaPattern, String tableNamePattern,
                                       String columnNamePattern )
        throws SQLException {

        System.out.println( "getPseudoColumns" );

        ColumnDesc[] columnDescs = new ColumnDesc[] {
            new ColumnDesc( "TABLE_CAT", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "TABLE_SCHEM", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "TABLE_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "COLUMN_NAME", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "DATA_TYPE", Integer.toString( Types.INTEGER ) ),
            new ColumnDesc( "DECIMAL_DIGITS", Integer.toString( Types.INTEGER ) ),
            new ColumnDesc( "NUM_PREC_RADIX", Integer.toString( Types.INTEGER ) ),
            new ColumnDesc( "COLUMN_USAGE", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "REMARKS", Integer.toString( Types.VARCHAR ) ),
            new ColumnDesc( "CHAR_OCTET_LENGTH", Integer.toString( Types.INTEGER ) ),
            new ColumnDesc( "IS_NULLABLE", Integer.toString( Types.VARCHAR ) ), };

        String[][] values = new String[][] {};

        return new LmiResultSet( columnDescs, values );
    }

    @Override
    public boolean generatedKeyAlwaysReturned()
        throws SQLException {
        return false;
    }

}
