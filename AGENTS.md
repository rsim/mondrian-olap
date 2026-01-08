# mondrian-olap

This file provides guidance to AI coding agents working with this repository.

## Overview

mondrian-olap is a JRuby gem for performing multidimensional queries of relational database data using Mondrian OLAP Java library.

## Technology Stack

- JRuby 9.4 or later (compatible with Ruby 3.1+)
- Java 8 or later
- Mondrian OLAP Java library from a fork https://github.com/rsim/mondrian/tree/9.3.0.0-rsim
- Databases: PostgreSQL, MySQL, Oracle, Microsoft SQL Server, ClickHouse or other JDBC compatible databases
- Testing: RSpec

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
- Run all RSpec tests with `rake spec` (with the default `mysql` database).
- Run all tests with a specified database:
  `rake spec:mysql`, `rake spec:postgresql`, `rake spec:sqlserver`, `rake spec:oracle`
- In most cases use RSpec should syntax and not expect syntax, for example, `result.should == expected`
- Use RSpec expect syntax only for block expectations, for example, `expect { action }.to raise_error(SomeError)`
