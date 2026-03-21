# Creating test database

By default unit tests use MySQL database but PostgreSQL, Oracle and SQL Server databases are supported as well. Set `MONDRIAN_DRIVER` environment variable to `mysql` (default), `postgresql`, `oracle`, `sqlserver` (Microsoft JDBC), `vertica`, `snowflake`, `clickhouse`, `mariadb` to specify database driver that should be used.

If using a MySQL, PostgreSQL or MS SQL Server database then create database user `mondrian_test` with password `mondrian_test`, create database `mondrian_test` and grant full access to this database for `mondrian_test` user. By default it is assumed that database is located on localhost (can be overridden with `DATABASE_HOST` environment variable).

If using Oracle database then create database user `mondrian_test` with password `mondrian_test`. By default it is assumed that database `orcl` is located on localhost (can be overridden with `DATABASE_NAME` and `DATABASE_HOST` environment variables).

If using Vertica then specify environment variables `VERTICA_DATABASE_HOST`, `VERTICA_DATABASE_NAME`, `VERTICA_DATABASE_USER`, `VERTICA_DATABASE_PASSWORD` and create the `mondrian_test` schema.

If using Snowflake then create `MONDRIAN_TEST` user, database and schema and specify environment variables `SNOWFLAKE_DATABASE_HOST` and `SNOWFLAKE_DATABASE_PASSWORD`.

If using ClickHouse then create database user `mondrian_test` with password `mondrian_test`, create database `mondrian_test` and grant full access to this database for `mondrian_test` user. Specify environment variable `CLICKHOUSE_DATABASE_HOST`. Download the latest `clickhouse-jdbc-*-all.jar` and copy to `test/support/jars`.

If using MariaDB ColumnStore then create database user `mondrian_test` with password `mondrian_test`, create database `mondrian_test` and grant full access to this database for `mondrian_test` user. Specify environment variable `MARIADB_DATABASE_HOST`. Download `mariadb-java-client-*.jar` and copy to `test/support/jars`.

See `test/test_helper.rb` for details of default connection parameters and how to override them.

# Creating test data

Install necessary gems with

    bundle install

Create tables with test data using

    rake db:create_data

or specify which database driver to use

    rake db:create_data MONDRIAN_DRIVER=mysql
    rake db:create_data MONDRIAN_DRIVER=postgresql
    rake db:create_data MONDRIAN_DRIVER=oracle
    rake db:create_data MONDRIAN_DRIVER=sqlserver

In case of Vertica, Snowflake, ClickHouse, MariaDB ColumnStore at first create test data in MySQL and export test data into CSV files and then import CSV files into the analytical database (because inserting individual records into these analytical databases is very slow):

    rake db:export_data MONDRIAN_DRIVER=mysql
    rake db:create_data MONDRIAN_DRIVER=vertica
    rake db:create_data MONDRIAN_DRIVER=snowflake
    rake db:create_data MONDRIAN_DRIVER=clickhouse
    rake db:create_data MONDRIAN_DRIVER=mariadb

# Running tests

Run tests with

    rake test

or specify which database driver to use

    rake test MONDRIAN_DRIVER=mysql
    rake test MONDRIAN_DRIVER=postgresql
    rake test MONDRIAN_DRIVER=oracle
    rake test MONDRIAN_DRIVER=sqlserver
    rake test MONDRIAN_DRIVER=vertica
    rake test MONDRIAN_DRIVER=snowflake
    rake test MONDRIAN_DRIVER=clickhouse
    rake test MONDRIAN_DRIVER=mariadb

or also alternatively with

    rake test:mysql
    rake test:postgresql
    rake test:oracle
    rake test:sqlserver
    rake test:vertica
    rake test:snowflake
    rake test:clickhouse
    rake test:mariadb

You can also run all tests on all standard databases (mysql jdbc_mysql postgresql oracle sqlserver) with

    rake test:all

# JRuby versions

It is recommended to use mise to run tests with different JRuby implementations. mondrian-olap is being tested with latest versions of JRuby 9.4 and 10 on Java 8 (deprecated), 11, 17, 21, and 25.
