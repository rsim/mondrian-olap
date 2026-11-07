# Changelog

All notable changes to mondrian-olap are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.4.1] - 2026-11-07

### Changed

- The bundled Mondrian jar is updated to mondrian-olap-java 1.4.1.
- The CI tests use the ClickHouse JDBC driver 0.11.0.

### Fixed

- A data access role was not applied to property references, for example,
  `[Store].[USA].[WA].[VALUE]`. The `VALUE` property could return the cell value of a denied member
  or measure, and the property error message showed that a denied member exists.

## [1.4.0] - 2026-11-03

### Added

- `Connection#parse_expression` parses an MDX expression and returns the Mondrian expression object.
- `Cube#connection`, `Cube#mondrian_cube` and `Cube#virtual?` methods.
- `Hierarchy#full_name`, `Level#full_name`, `Level#parent_level`, `Level#child_level` and
  `Level#descendant_level(name)` methods.
- `Member#children_count` returns the child member count without loading and sorting the child
  members, and `Member#level` returns the level of the member.
- The query builder `filter_last` method filters the last set of a crossjoin, like `except` does.
- The Oracle connection `properties` option passes the properties to the Oracle JDBC driver, for
  example, `"javax.net.ssl.keyStore"` and `"javax.net.ssl.keyStorePassword"`. The properties are
  not added to the JDBC URL, so the URL does not show the passwords.
- Support for the ClickHouse JDBC driver version 0.5 and later.
- Support for JRuby 10.
- The `MONDRIAN_OLAP_JAR_PATH` environment variable loads the Mondrian jar from the specified path
  instead of the bundled jar, for example, from a local mondrian-olap-java build.
- Query profiling includes the evaluation time of each calculated member and of `NonEmptyCrossJoin`.
- MDX `Min` and `Max` functions work with date values.
- MDX `+` operator concatenates strings, for example, `"a" + "b"`, as well as a string and a number,
  for example, `"a" + 1`. The `||` operator also accepts numeric operands.
- MDX `IsEmpty` function and `IS EMPTY` operator accept date expressions.
- MDX `CASE` statements accept branches with different expression types.
- MDX `CoalesceEmpty` function accepts date and generic value arguments.
- MDX `Generate` function accepts a string measure as the second argument.
- VBA date functions (`DateAdd`, `DateDiff`, `DatePart`, `Day`, `Month`, `Year`, `Weekday`,
  `DateValue`, `TimeValue` and others) accept date values of calculated members.
- A user defined function in the schema overrides a built-in function with the same name and
  syntax. The `mondrian.olap.fun.skipJavaFunDefs` system property (a comma-separated list of
  function names) skips the registration of the specified VBA and Excel functions.
- The `mondrian.rolap.agg.SegmentCacheManager.actorThreads` system property sets the number of
  segment cache manager threads, so that the queries of different schemas do not wait for each
  other.
- The Mondrian `SchemaReader#getLevelMemberByUniqueKey` and `SchemaReader#hasMemberChildren`
  methods for fast member lookup by the key.

### Changed

- The bundled Mondrian jar is built from the [mondrian-olap-java](https://github.com/rsim/mondrian-olap-java)
  fork of Mondrian 9.3.0.0, which contains all mondrian-olap patches.
- Extraction of the cell values of large results is about two times faster.
- `Result#values` raises `ArgumentError` with the list of valid axis names for an invalid axis name.
- Members are sorted by case-insensitive and accent-insensitive order keys, which do not depend on
  the database collation, when `mondrian.rolap.compareSiblingsByOrderKey` is set. Children of
  parent-child hierarchy members are sorted by the order keys as well.
- Hierarchized level members are cached, which makes repeated level member queries faster.
- `NonEmptyCrossJoin` of a virtual cube skips the SQL queries of the sub-cubes which do not have the
  dimensions of the context members.
- Query cancellation and timeout checks are faster, and named set evaluation can be cancelled.
- The `OutOfMemory` error message does not include the connect string, which can contain the
  database user name and the whole schema XML.
- A calculated member with `Min` or `Max` and without an explicit format string uses the format of
  the value expression and not of a measure in a `Filter` condition.
- Upgraded log4j to 2.25.5, commons-io to 2.18.0, commons-lang3 to 3.18.0 (replaces commons-lang)
  and commons-vfs2 to 2.10.0.
- Migrated the tests from RSpec to Minitest and the CI tests from Travis CI and AppVeyor to GitHub
  Actions.
- Tested with JRuby 9.4 and 10, and Java 8, 11, 17, 21 and 25.

### Deprecated

- Java 8 support.

### Removed

- The Guava library dependency.

### Fixed

- Drill through with a data access role returned fewer than `max_rows` rows, as rows were limited
  before the role restrictions were checked. Drill through also closes the JDBC result set after
  fetching the rows.
- The query builder generated an invalid MDX set from a single `DefaultMember` expression.
- The query builder and drill through raised `NoMethodError` when ActiveSupport was not loaded.
- `Hierarchy#levels` was not memoized.
- `shutdown_static_mondrian_server!` did not remove the Mondrian memory monitor listener, and it
  ignores the `InaccessibleObjectException` on Java 17 and later.
- `NonEmptyCrossJoin` with null slicer members.
- `LinRegR2` and `LinRegVariance` calculation and their handling of NULL arguments.
- `CASE` statements returning a generic value type.
- `TopCount` and `BottomCount` with a native SQL constraint returned wrong results.
- VBA and Excel functions did not treat the MDX NULL value as null.
- VBA `Int` function returned 0 for negative values between -1 and 0 (MONDRIAN-2730).
- VBA `CDate` function parsed date strings differently after a Java version upgrade. It now tries
  fixed date and time patterns before the patterns of the JVM locale.
- Errors of the format string lookup, including a `StackOverflowError` for cyclic references of
  calculated members, failed the whole query.
- `Order` failed with "Comparison method violates its general contract!" for NULL order keys and
  for large sorts with a sort expression which returns a different value on each evaluation, for
  example, with `Now()`.
- Queries of a virtual cube with only calculated measures could not select a base cube.
- Oracle `TIMESTAMP` and `DATE` values were not recognized as dates.
- Concurrent queries could fail with a `NullPointerException` from the virtual cube level cache, or
  with errors from the segment cache indexes.
- Deadlock of the segment cache manager when all query permits (`mondrian.query.limit`) were in use.
- Division by zero in the query timing summary.

## [1.3.0] - 2023-06-02

### Added

- Additional Mondrian patches and improvements:
  - Support for ClickHouse database.
  - Support for MariaDB ColumnStore engine.
  - Skip registration of MondrianOlap4jDriver if `mondrian.olap4j.registerDriver=false`.
- Support `instance` parameter for SQL Server connection.
- Allow to specify `high_cardinality` for dimension.

### Changed

- Upgrade to the latest Mondrian version 9.3.0.0 with additional patches.
- Additional Mondrian patches and improvements:
  - Improve `Aggregate` performance of large compound slicers when
    `mondrian.rolap.EnableInMemoryRollup=false`.
  - Improve performance of Mondrian member property value lookup.
  - Set dynamic Mondrian connection pool size based on `mondrian.rolap.maxSqlThreads` property.
  - Enable Mondrian `supportsMultiValueInExpr` for PostgreSQL and Oracle.
- Upgrade to log4j2.
- Upgrade tests to use the later ActiveRecord and RSpec versions.
- Tested with JRuby 9.4 and JVM 17.

### Removed

- Deprecated jTDS driver support, use MS JDBC driver instead.
- User defined functions and formatters in JavaScript and CoffeeScript as they are not supported
  since JVM 8.

### Fixed

- Patch for MONDRIAN-2714 (fixed support for MySQL JDBC driver version 8.0.23).

## [1.2.0] - 2021-03-06

### Added

- Improve bigint key columns support.
- Add MySQL 8 JDBC driver support.

### Changed

- Upgrade to the latest Mondrian version 9.1.0.0.
- Improved Vertica and Snowflake tests to use bulk data loading.
- Tested with JRuby 9.2.14.0.

### Removed

- Support of LucidDB (which is abandoned a long time ago).
- JRuby 1.7 support.

## [1.1.0] - 2019-11-09

### Added

- Set query specific timeout.
- Support for Vertica and Snowflake databases.

### Changed

- Upgrade to the latest Mondrian version 8.3.0.5.
- Tested with JRuby 9.2.9.0.

### Fixed

- Handle and wrap `TokenMgrError` exception.
- Patch for MONDRIAN-2660.
- Patch for MONDRIAN-2661.

## [1.0.0] - 2019-03-04

### Added

- Support for Java 11, tested with JRuby 9.2.6.0.
- Mondrian query profiling option.
- Connection `flush_schema` method and `flush_schema(schema_key)` class method.
- `pin_schema_timeout` connection parameter.

### Changed

- Upgrade to the latest Mondrian version 8.2.0.4.

### Removed

- Java 7 support.
- Deprecated `NativeException`, `Fixnum`.

### Fixed

- Parsing of drill through queries with newlines.
- Do not log Mondrian errors on the console.
- Handling of dimension names with escaped `]`.
- Patch for MONDRIAN-2641.

## [0.9.0] - 2017-10-07

### Added

- `annotations_hash` method for schema elements.

### Changed

- Upgraded to the latest Mondrian version 3.14.0.5.

### Fixed

- Drill through SQL query generation.
- Set a single role as a union role - used as a workaround for a Mondrian bug.

## [0.8.0] - 2016-10-26

### Added

- `flush_region_cache_with_segments` and `flush_region_cache_with_full_names` methods for partial
  clearing of the cache.
- `Name()` and `Property()` extensions for `drill_through` return fields.

### Changed

- Upgraded to the latest Mondrian version 3.12.0.6.

### Fixed

- Retrieving of drill through results with Clob values.

## [0.7.0] - 2015-12-12

### Added

- Query builder method `distinct`.
- Annotations for `dimension_usage` and `virtual_cube_dimension` schema elements.

### Changed

- Upgraded to the latest Mondrian version 3.11.

### Removed

- Java 6 support.

### Fixed

- JRuby 9.0 support.
- The order of axis aliases - columns, rows, pages, chapters, sections.

## [0.6.0] - 2014-11-10

### Added

- Connection with generic JDBC driver using `jdbc_driver` and `jdbc_url` parameters.
- `hierarchy` and `parent` attributes for calculated member schema definition element.
- `visible` attribute for cube, dimension, virtual_cube_dimension, hierarchy and level schema
  definition elements.
- Query builder `generate` method.
- Schema parameters and query execution with parameters.

### Changed

- Upgraded to latest Mondrian 3.8.0 version.
- Updated specs to pass on Java 8.
- Set `defaultRowPrefetch` property for Oracle connection.

### Fixed

- Result `drill_through` method with just all members selection.

## [0.5.0] - 2013-11-29

### Added

- `shutdown_static_mondrian_server!` method.
- Schema element annotations in schema definition.
- Set connection locale.
- Support for schema elements caption.
- Shared dimension schema definition methods.
- Virtual cube schema definition methods.
- Connection `execute_drill_through` method.
- Define shared user defined cell formatters.
- Support Oracle connection using slash and service name as database name.

### Changed

- Upgraded to latest Mondrian 3.5 version (build from 2013-07-31).
- Set default hierarchy `:has_all` and level `:unique_members` attributes.
- By default use sum aggregator for measure in schema definition.

### Fixed

- Render cube XML fragment before calculated members in generated XML schema.
- Generate XML with UTF-8 encoding.

## [0.4.0] - 2012-12-03

### Added

- Support for JRuby 1.7 and Java 7 VM.
- User defined functions and formatters in JavaScript, CoffeeScript and Ruby.
- Shared user defined functions in Ruby.
- All exceptions are wrapped in `Mondrian::OLAP::Error` exception with `root_cause_message` method.
- Drill through from result cell to source measure and dimension table rows.
- Support for Mondrian schema roles to limit cube data access.
- Get description of cube, dimension, hierarchy and level from schema definition.
- `visible?` method for measures and calculated members.
- `nonempty_crossjoin` query builder method.
- Schema definition with nested table joins.
- `approx_row_count` schema level attribute.

### Changed

- Upgraded to latest Mondrian 3.5 version (build from 2012-11-29) as well as corresponding olap4j
  1.0.1 version.

## [0.3.0] - 2011-11-12

### Added

- Support for SQL Server (jTDS and Microsoft drivers).
- Aggregates definition in schema.
- Possibility to include XML fragments in schema (e.g. to paste XML for aggregates that is
  generated by Mondrian aggregation designer).
- Define level properties in schema.
- `sql` element for `table` element in schema (to define custom WHERE conditions for dimensions or
  fact table).
- `view` element in schema (to define custom SQL SELECT statements instead of existing table).
- `measure_expression` element in schema (to define custom SQL expression for measure instead of
  fact column).
- Allow crossjoin of where conditions as well as where with several same dimension members.

### Changed

- Upgraded to Mondrian 3.3.0 version (latest snapshot with additional bug fixes) as well as
  corresponding olap4j 1.0.0 version.
- Use latest Nokogiri 1.5.0 version.

## [0.2.0] - 2011-07-01

### Added

- Support for LucidDB database.
- `result.to_html(formatted: true)` will return formatted results.
- `format_string` attribute and `formula` element for calculated members.
- `:use_content_checksum` connection option (by default set to true).
- `key_expression`, `name_expression`, `ordinal_expression` elements with `sql` subelement support
  for levels.
- `:upcase_data_dictionary` option for schema definition.

### Changed

- Only set log4j configuration file if not set already (possible to override e.g. Mondrian
  debugging settings).
- Set Unicode encoding for mysql connection.

### Fixed

- Examples in README.
- Correctly quote `CatalogContent` in connection string (to allow usage of semicolons in generated
  XML catalog).
- Workarounds for issues with Java classloader when using in production mode with jruby-rack.
- Construct correct file path on Windows.

## [0.1.0] - 2011-03-18

### Added

- Initial release.
- Support for MySQL, PostgreSQL and Oracle databases.

[1.4.1]: https://github.com/rsim/mondrian-olap/compare/v1.4.0...v1.4.1
[1.4.0]: https://github.com/rsim/mondrian-olap/compare/v1.3.0...v1.4.0
[1.3.0]: https://github.com/rsim/mondrian-olap/compare/v1.2.0...v1.3.0
[1.2.0]: https://github.com/rsim/mondrian-olap/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/rsim/mondrian-olap/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/rsim/mondrian-olap/compare/v0.9.0...v1.0.0
[0.9.0]: https://github.com/rsim/mondrian-olap/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/rsim/mondrian-olap/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/rsim/mondrian-olap/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/rsim/mondrian-olap/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/rsim/mondrian-olap/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/rsim/mondrian-olap/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/rsim/mondrian-olap/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/rsim/mondrian-olap/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/rsim/mondrian-olap/releases/tag/v0.1.0
