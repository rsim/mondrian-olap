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

1. **Adding new schema features** - Modify `lib/mondrian/olap/schema.rb` and add corresponding specs in `spec/schema_definition_spec.rb`
2. **Extending query capabilities** - Update `lib/mondrian/olap/query.rb` and test in `spec/query_spec.rb`
3. **Connection enhancements** - Change `lib/mondrian/olap/connection.rb` with tests in `spec/connection_spec.rb`
4. **Cube operations** - Modify `lib/mondrian/olap/cube.rb` and add specs in `spec/cube_spec.rb`

### Testing Changes

1. Write or update RSpec tests for the changed functionality
2. Run specific test file: `rspec spec/cube_spec.rb`
3. Run all tests with default database: `rake spec`
4. Test with specific databases: `rake spec:postgresql`, `rake spec:mysql`, etc.
5. Ensure tests pass with multiple database backends before finalizing changes

## Technology Stack

- **JRuby** 9.4 or later (compatible with Ruby 3.1+)
- **Java** 8 or later LTS version
- **Mondrian OLAP** Java library from a fork https://github.com/rsim/mondrian/tree/9.3.0.0-rsim
- **Databases**: PostgreSQL, MySQL, Oracle, Microsoft SQL Server, ClickHouse or other JDBC compatible databases
- **Testing**: RSpec

### JRuby-Specific Considerations

- This gem requires JRuby and will not work with standard MRI Ruby
- Use JRuby's Java integration features to interact with Mondrian Java library
- Java objects can be accessed directly in JRuby code
- JDBC drivers are used for database connections instead of native Ruby database adapters

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
- Use the new hash syntax `key: 'value'` instead of the old syntax `:key => 'value'`
- Use simple parentheses declaring an array of strings `%w()` instead of other symbols like `%w[]`

### Testing

- Use RSpec for Ruby testing
- Run individual RSpec test file with e.g. `rspec spec/cube_spec.rb`
- Run all RSpec tests with `rake spec` (with the default `mysql` database)
- Run all tests with a specified database:
  `rake spec:mysql`, `rake spec:postgresql`, `rake spec:sqlserver`, `rake spec:oracle`
- In most cases use RSpec should syntax and not expect syntax, for example, `result.should == expected`
- Use RSpec expect syntax only for block expectations, for example, `expect { action }.to raise_error(SomeError)`
- Test data is located in `spec/support/data/` directory
- Database-specific schema fixtures are in `spec/fixtures/` directory
