# TIBCO (c) LogLogic LMI JDBC Driver

## Description

This JDBC driver aims at querying log data stored in LMI appliances using the same query language as the advanced search feature, while retrieving the results in a standard JDBC fashion.

The driver is self-contained in a single JAR file: lmi-jdbc-driver-1.0-single.jar that should be placed at the relevant location depending on the application using it.

## Prerequisites

This driver works with LMI version 6.2.0 or greater.

## Usage guide


### URL format and default port
The URL template to use for connecting to an LMI host is:
jdbc:lmi:<host/IP>:<port>

Default port is 9681

### Supported parameters for connection

Here is the list of the options that can be passed in the connection string URL itself, or programmatically through the JDBC driver standard mechanisms.

|Parameter|	Mandatory|	Default Value|	Comment|
|---------|----------|---------------|---------|
|user|	Yes|	n/a|	The username of an LMI user with query privileges|
|password|	Yes|	n/a	|The password|
|networkTimeoutMillis|	No|	600000|	Timeout for all network operations (milliseconds)|
|batchSize|	No|	5000|	Size of the batches for results retrieval|
|queryTimeout|	No|	3600|	Query is deleted after expiration of this time (seconds)|
|insecureMode|	No|	No|	When true, disable all security checks on the server certificate (not for production use)|
|noHostnameVerification|	No|	No|	When true, do no verify that the hostname present in the certificate is the one used in the URL.|
|pollingTimeout|	No|	3600|	Abort query if no results retrieved within that time (seconds).|
|acceptedCertificateFingerprints|	No|	n/a|	A string representing the list of the certificate fingerprints accepted, delimited with comas.| 
|keyStoreURL|	No|	n/a|	URL of the keystore file containing the anchors of trust|
|keyStorePassword|	No|	n/a|	The password of the keystore file.|
