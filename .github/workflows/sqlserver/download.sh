#!/bin/bash

set -ev

# Download SQL Server JDBC driver from Maven Central
# (download.microsoft.com TLS certificate cannot be verified on GitHub runners)
SQLSERVER_JDBC_VERSION=7.2.2.jre8
wget https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/${SQLSERVER_JDBC_VERSION}/mssql-jdbc-${SQLSERVER_JDBC_VERSION}.jar
cp mssql-jdbc-${SQLSERVER_JDBC_VERSION}.jar test/support/jars/
