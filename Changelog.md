### 0.2.0 / 2011-07-01

* New features
  * support for LucidDB database
  * support for SQL Server (jTDS and Microsoft drivers)
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
