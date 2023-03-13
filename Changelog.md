### Master

* New features
  * Upgrade to the latest Mondrian version 9.3.0.0 with additional patches
  * Additional Mondrian patches and improvements
    * Support for ClickHouse database
    * Support for MariaDB ColumnStore engine
    * Improve Aggregate performance of large compound slicers when mondrian.rolap.EnableInMemoryRollup=false
    * Improve performance of Mondrian member property value lookup
    * Set dynamic Mondrian connection pool size based on mondrian.rolap.maxSqlThreads property
    * Skip registration of MondrianOlap4jDriver if mondrian.olap4j.registerDriver=false
    * Enable Mondrian supportsMultiValueInExpr for PostgreSQL and Oracle
  * Upgrade to log4j2
  * Support instance parameter for SQL Server connection
  * Allow to specify high_cardinality for dimension
  * Remove deprecated jTDS driver support, use MS JDBC driver instead
  * Remove user defined functions and formatters in JavaScript and CoffeeScript as they are not supported since JVM 8
  * Upgrade tests to use the later ActiveRecord and RSpec versions
  * Tested with JRuby 9.4 and JVM 17
* Bug fixes
  * Patch for MONDRIAN-2714 (fixed support for MySQL JDBC driver version 8.0.23)

### 1.2.0 / 2021-03-06

* New features
  * Upgrade to the latest Mondrian version 9.1.0.0
  * Improved Vertica and Snowflake tests to use bulk data loading
  * Remove support of LucidDB (which is abandoned a long time ago)
  * Improve bigint key columns support
  * Add MySQL 8 JDBC driver support
  * Tested with JRuby 9.2.14.0
  * Remove JRuby 1.7 support

### 1.1.0 / 2019-11-09

* New features
  * Upgrade to the latest Mondrian version 8.3.0.5
  * Tested with JRuby 9.2.9.0
  * Set query specific timeout
  * Support for Vertica and Snowflake databases
* Bug fixes
  * Handle and wrap TokenMgrError exception
  * Patch for MONDRIAN-2660
  * Patch for MONDRIAN-2661

### 1.0.0 / 2019-03-04

* New features
  * Upgrade to the latest Mondrian version 8.2.0.4
  * Desupport Java 7, added support for Java 11, tested with JRuby 9.2.6.0
  * Remove deprecated NativeException, Fixnum
  * Added Mondrian query profiling option
  * Added connection flush_schema method and flush_schema(schema_key) class method
  * Added pin_schema_timeout connection parameter
* Bug fixes
  * Fixed parsing of drill through queries with newlines
  * Do not log Mondrian errors on the console
  * Fixed handling of dimension names with escaped ]
  * Patch for MONDRIAN-2641

### 0.9.0 / 2017-10-07

* New features
  * Upgraded to the latest Mondrian version 3.14.0.5
  * Added an annotations_hash method for schema elements
* Bug fixes
  * Fixed drill through SQL query generation
  * Set a single role as a union role - used as a workaround for a Mondrian bug

### 0.8.0 / 2016-10-26

* New features
  * upgraded to the latest Mondrian version 3.12.0.6
  * added flush_region_cache_with_segments and flush_region_cache_with_full_names methods for partial clearing of the cache
  * added Name() and Property() extensions for drill_through return fields
* Bug fixes
  * fixed retrieving of drill through results with Clob values

### 0.7.0 / 2015-12-12

* New features
  * upgraded to latest the latest Mondrian version 3.11
  * removed Java 6 support
  * fixes for JRuby 9.0 support
  * added a query builder method "distinct"
* Improvements
  * added annotations for dimension_usage and virtual_cube_dimension schema elements
* Bug fixes
  * fixed the order of axis aliases - columns, rows, pages, chapters, sections


### 0.6.0 / 2014-11-10

* New features
  * upgraded to latest Mondrian 3.8.0 version
  * connection with generic JDBC driver using jdbc_driver and jdbc_url parameters
  * added hierarchy and parent attributes for calculated member schema definition element
  * added visible attribute for cube, dimension, virtual_cube_dimension, hierarchy and level schema definition elements
  * added query builder generate method
  * added schema parameters and query execution with parameters
  * updated specs to pass on Java 8
* Improvements
  * set defaultRowPrefetch property for Oracle connection
* Bug fixes
  * fixed result drill_through method with just all members selection

### 0.5.0 / 2013-11-29

* New features
  * upgraded to latest Mondrian 3.5 version (build from 2013-07-31)
  * added shutdown_static_mondrian_server! method
  * add schema element annotations in schema definition
  * set connection locale
  * added support for schema elements caption
  * added shared dimension schema definition methods
  * added virtual cube schema definition methods
* Improvements
  * connection execute_drill_through method
  * define shared user defined cell formatters
  * set default hierarchy :has_all and level :unique_members attributes
  * by default use sum aggregator for measure in schema definition
  * support Oracle connection using slash and service name as database name
* Bug fixes
  * render cube XML fragment before calculated members in generated XML schema
  * generate XML with UTF-8 encoding

### 0.4.0 / 2012-12-03

* New features
  * upgraded to latest Mondrian 3.5 version (build from 2012-11-29)
    as well as corresponding olap4j 1.0.1 version
  * support for JRuby 1.7 and Java 7 VM
  * user defined functions and formatters in JavaScript, CoffeeScript and Ruby
  * shared user defined functions in Ruby
  * all exceptions are wrapped in Mondrian::OLAP::Error exception with root_cause_message method
  * drill through from result cell to source measure and dimension table rows
  * support for Mondrian schema roles to limit cube data access
* Improvements
  * get description of cube, dimension, hierarchy and level from schema definition
  * visible? method for measures and calculated members
  * nonempty_crossjoin query builder method
  * schema definition with nested table joins
  * added approx_row_count schema level attribute

### 0.3.0 / 2011-11-12

* New features
  * upgraded to Mondrian 3.3.0 version (latest shapshot with additional bug fixes)
    as well as corresponding olap4j 1.0.0 version
  * support for SQL Server (jTDS and Microsoft drivers)
  * aggregates definition in schema
  * possibility to include XML fragments in schema
    (e.g. to paste XML for aggregates that is generated by Mondrian aggregation designer)
  * define level properties in schema
  * `sql` element for `table` element in schema
    (to define custom WHERE conditions for dimensions or fact table)
  * `view` element in schema
    (to define custom SQL SELECT statements instead of existing table)
  * `measure_expression` element in schema
    (to define custom SQL expression for measure instead of fact column)
  * allow crossjoin of where conditions as well as where with several same dimension members
* Improvements
  * use latest Nokogiri 1.5.0 version

### 0.2.0 / 2011-07-01

* New features
  * support for LucidDB database
* Improvements
  * only set log4j configuration file if not set already (possible to override e.g. Mondrian debugging settings)
  * `result.to_html(:formatted=>true)` will return formatted results
  * set Unicode encoding for mysql connection
  * `format_string` attribute and `formula` element for calculated members
  * `:use_content_checksum` connection option (by default set to true)
  * `key_expression`, `name_expression`, `ordinal_expression` elements with `sql` subelement support for levels
  * `:upcase_data_dictionary` option for schema definition
* Bug fixes
  * fixed examples in README
  * correctly quote `CatalogContent` in connection string (to allow usage of semicolons in generated XML catalog)
  * workarounds for issues with Java classloader when using in production mode with jruby-rack
  * construct correct file path on Windows

### 0.1.0 / 2011-03-18

* Initial release
  * support for MySQL, PostgreSQL and Oracle databases
