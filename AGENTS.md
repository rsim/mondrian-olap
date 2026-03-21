# mondrian-olap

This file provides guidance to AI coding agents working with this repository.

## Overview

mondrian-olap is a JRuby gem for performing multidimensional queries of relational database data using Mondrian OLAP Java library.

## Codebase Structure

### Main Entry Points

- `lib/mondrian-olap.rb` - Main gem entry point that requires all components
- `lib/mondrian/olap.rb` - Core module and Java library initialization
- `lib/mondrian/olap/connection.rb` - Database connection management
- `lib/mondrian/olap/schema.rb` - OLAP schema definition DSL
- `lib/mondrian/olap/cube.rb` - Cube operations and queries
- `lib/mondrian/olap/query.rb` - Query builder for MDX-like queries
- `lib/mondrian/olap/result.rb` - Query result processing

### Key Modules

- `Mondrian::OLAP::Connection` - Manages connections to OLAP data sources
- `Mondrian::OLAP::Schema` - Provides DSL for defining OLAP schemas programmatically
- `Mondrian::OLAP::Cube` - Represents OLAP cubes and provides query interface
- `Mondrian::OLAP::Query` - Builds and executes MDX-like queries
- `Mondrian::OLAP::Result` - Handles query results with axes and cells

### Java Integration

- `lib/mondrian/jars/` - Contains Mondrian OLAP Java library JAR files
- The gem bridges Ruby code with Java Mondrian library using JRuby's Java integration
- Java objects are wrapped in Ruby classes to provide idiomatic Ruby API

## Common Workflows

### Making Changes

1. **Adding new schema features** - Modify `lib/mondrian/olap/schema.rb` and add corresponding tests in
   `test/schema_definition_test.rb`
2. **Extending query capabilities** - Update `lib/mondrian/olap/query.rb` and test in `test/query_test.rb`
3. **Connection enhancements** - Change `lib/mondrian/olap/connection.rb` with tests in `test/connection_test.rb`
4. **Cube operations** - Modify `lib/mondrian/olap/cube.rb` and add tests in `test/cube_test.rb`

### Testing Changes

1. Write or update Minitest tests for the changed functionality.
2. Run specific test file: `ruby -Itest test/cube_test.rb`.
3. Run all tests with default database: `rake test` (with the default `mysql` database).
4. Test with specific databases: `rake test:mysql`, `rake test:postgresql`, `rake test:sqlserver`, `rake test:oracle`.
5. Ensure tests pass with multiple database backends before finalizing changes.

## Technology Stack

- **JRuby** 9.4 or later (compatible with Ruby 3.1+)
- **Java** 8 or later LTS version
- **Mondrian OLAP** Java library from a fork https://github.com/rsim/mondrian-olap-java
- **Databases**: PostgreSQL, MySQL, Oracle, Microsoft SQL Server, ClickHouse or other JDBC compatible databases
- **Testing**: Minitest (with minitest-hooks)

### JRuby-Specific Considerations

- This gem requires JRuby and will not work with standard MRI Ruby.
- Use JRuby's Java integration features to interact with Mondrian Java library.
- Java objects can be accessed directly in JRuby code.
- JDBC drivers are used for database connections instead of native Ruby database adapters.

## Code Style and Guidelines

### General

- Use meaningful semantic names for variables, methods, and classes.
- Use query and command method conventions.
  - Query methods use nouns and do not modify state and do not have side effects. Boolean methods end with `?`.
  - Command methods use verbs that describe the action being taken and may modify state.
- Use consistent naming, use the same terminology throughout the codebase.
- Do not use similar variable or method names for different data or objects.
- Write comments to explain why something is done, not what is done.
- Write comments only when it is not obvious from the code.
- Start full sentence comments with a capital letter.
- Prefer self-explanatory code with semantic names over detailed comments.
- Keep methods small and focused on a single task.
- Write simple readable code. Do not obfuscate simple logic.
- Validate correct spelling for variable, method, class names, as well as for comments.

### Ruby

- Do not modify objects (like Hash and Array) that are referenced by argument variables.
  This might cause unexpected side effects in the caller. It is OK to assign a new object to an argument variable.
- Return collections (arrays or other) from methods with plural names. Do not return nil from methods with plural names,
  return empty collections in such cases.
- Use &:method for collections:
  `collection.map(&:method)` instead of `collection.map { |item| item.method }`.
- Use safe navigation operator `&.` when calling methods on objects that might be nil.
- When continuing the method call on the next line, then end the first line with a dot.
- Use the new hash syntax `key: 'value'` instead of the old syntax `:key => 'value'`.
- Use the old hash syntax only for rake task dependencies, for example, `task :build => :compile`.
- Use simple parentheses declaring an array of strings `%w()` instead of other symbols like `%w[]`.
- Use frozen string literal comments for all Ruby files and ensure that frozen strings are not modified.

### mise

- mise might be used to manage Ruby and Java versions.
- If mise is available then prefix ruby, rake, java calls with `mise exec --` to initialize the correct environment.

### Testing

- Use Minitest for Ruby testing with the minitest-hooks gem for lifecycle hooks.
- Run individual test file with e.g. `ruby -Itest test/cube_test.rb`.
- Run all tests with `rake test` (with the default `mysql` database).
- Run all tests with a specified database:
  `rake test:mysql`, `rake test:postgresql`, `rake test:sqlserver`, `rake test:oracle`.
- Use Minitest Spec-style syntax with `describe` and `it` blocks. Nest `describe` blocks for logical grouping.
- Use `before` and `after` hooks for per-test setup and teardown.
- Use `before(:all)` and `after(:all)` hooks (from minitest-hooks) for expensive setup shared across all tests
  in a `describe` block, such as establishing database connections or defining schemas.
- Use standard Minitest assertions: `assert_equal`, `assert_nil`, `assert_empty`, `assert_kind_of`, `assert_match`.
- Use `assert_raises` with a block for exception testing, for example,
  `error = assert_raises(Mondrian::OLAP::Error) { action }`.
- Use `refute` and `refute_nil` for negation assertions.
- Prefer `assert_equal true, method?` and `assert_equal false, method?` for boolean methods
  instead of simple `assert` and `refute` assertions.
- Use the custom `assert_like` matcher for comparing XML strings with normalized whitespace.
- Use instance variables (`@olap`, `@schema`, `@cube`) for shared state between hooks and tests.
- Conditionally skip tests for specific database drivers using `unless` guards, for example,
  `unless %w(vertica snowflake clickhouse).include?(MONDRIAN_DRIVER)`.
- Test files are located in `test/` directory and follow the naming pattern `*_test.rb`.
- Test helper and database configuration are in `test/test_helper.rb`.
