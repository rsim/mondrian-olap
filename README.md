mondrian-olap
=============

JRuby gem for performing multidimensional queries of relational database data using Mondrian OLAP Java library.

DESCRIPTION
-----------

SQL language is good for doing ad-hoc queries from relational databases but it becomes very complicated when doing more complex analytical queries to get summary results. Alternative approach is OLAP (On-Line Analytical Processing) databases and engines that provide easier multidimensional analysis of data at different summary levels.

One of the most popular open-source OLAP engines is [Mondrian](http://mondrian.pentaho.com). Mondrian OLAP engine can be put in front of relational SQL database and it provides MDX multidimensional query language which is much more suited for analytical purposes.

mondrian-olap is JRuby gem which includes Mondrian OLAP engine and provides Ruby DSL for creating OLAP schemas on top of relational database schemas and provides MDX query language and query builder Ruby methods for making analytical queries.

mondrian-olap is used in [eazyBI data analysis and reporting web application](https://eazybi.com). [eazyBI remote setup](https://eazybi.com/help/remote-setup) can be used to create easy-to-use web based reports and dashboards on top of mondrian-olap based backend database. There is also [mondrian-olap demo Rails application for trying MDX queries](https://github.com/rsim/mondrian_demo).

USAGE
-----

### Schema definition

At first you need to define OLAP schema mapping to relational database schema tables and columns. OLAP schema consists of:

* Cubes

  Multidimensional cube is a collection of measures that can be accessed by dimensions. In relational database cubes are stored in fact tables with measure columns and dimension foreign key columns.

* Dimensions

  Dimension can be used in one cube (private) or in many cubes (shared). In relational database dimensions are stored in dimension tables.

* Hierarchies and levels

  Dimension has at least one primary hierarchy and optional additional hierarchies and each hierarchy has one or more levels. In relational database all levels can be stored in the same dimension table as different columns or can be stored also in several tables.

* Members

  Dimension hierarchy level values are called members.

* Measures

  Measures are values which can be accessed at detailed level or aggregated (e.g. as sum or average) at higher dimension hierarchy levels. In relational database measures are stored as columns in cube table.

* Calculated measures

  Calculated measures are not stored in database but calculated using specified formula from other measures.

Read more about about [defining Mondrian OLAP schema](http://mondrian.pentaho.com/documentation/schema.php).

Here is example how to define OLAP schema and its mapping to relational database tables and columns using mondrian-olap:

```ruby
require "rubygems"
require "mondrian-olap"

schema = Mondrian::OLAP::Schema.define do
  cube 'Sales' do
    table 'sales'
    dimension 'Customers', :foreign_key => 'customer_id' do
      hierarchy :has_all => true, :all_member_name => 'All Customers', :primary_key => 'id' do
        table 'customers'
        level 'Country', :column => 'country', :unique_members => true
        level 'State Province', :column => 'state_province', :unique_members => true
        level 'City', :column => 'city', :unique_members => false
        level 'Name', :column => 'fullname', :unique_members => true
      end
    end
    dimension 'Products', :foreign_key => 'product_id' do
      hierarchy :has_all => true, :all_member_name => 'All Products',
                :primary_key => 'id', :primary_key_table => 'products' do
        join :left_key => 'product_class_id', :right_key => 'id' do
          table 'products'
          table 'product_classes'
        end
        level 'Product Family', :table => 'product_classes', :column => 'product_family', :unique_members => true
        level 'Brand Name', :table => 'products', :column => 'brand_name', :unique_members => false
        level 'Product Name', :table => 'products', :column => 'product_name', :unique_members => true
      end
    end
    dimension 'Time', :foreign_key => 'time_id', :type => 'TimeDimension' do
      hierarchy :has_all => false, :primary_key => 'id' do
        table 'time'
        level 'Year', :column => 'the_year', :type => 'Numeric', :unique_members => true, :level_type => 'TimeYears'
        level 'Quarter', :column => 'quarter', :unique_members => false, :level_type => 'TimeQuarters'
        level 'Month', :column => 'month_of_year', :type => 'Numeric', :unique_members => false, :level_type => 'TimeMonths'
      end
      hierarchy 'Weekly', :has_all => false, :primary_key => 'id' do
        table 'time'
        level 'Year', :column => 'the_year', :type => 'Numeric', :unique_members => true, :level_type => 'TimeYears'
        level 'Week', :column => 'week_of_year', :type => 'Numeric', :unique_members => false, :level_type => 'TimeWeeks'
      end
    end
    measure 'Unit Sales', :column => 'unit_sales', :aggregator => 'sum'
    measure 'Store Sales', :column => 'store_sales', :aggregator => 'sum'
  end
end
```

### Connection creation

When schema is defined it is necessary to establish OLAP connection to database. Here is example how to connect to MySQL database using the schema object that was defined previously:

```ruby
require "jdbc/mysql"

olap = Mondrian::OLAP::Connection.create(
  :driver => 'mysql',
  :host => 'localhost,
  :database => 'mondrian_test',
  :username => 'mondrian_user',
  :password => 'secret',
  :schema => schema
)
```

### MDX queries

Mondrian OLAP provides MDX query language. [Read more about MDX](http://mondrian.pentaho.com/documentation/mdx.php).
mondrian-olap allows executing of MDX queries, for example query for "Get sales amount and number of units (on columns) of all product families (on rows) sold in California during Q1 of 2010":

```ruby
result = olap.execute <<-MDX
  SELECT  {[Measures].[Unit Sales], [Measures].[Store Sales]} ON COLUMNS,
          {[Products].children} ON ROWS
    FROM  [Sales]
    WHERE ([Time].[2010].[Q1], [Customers].[USA].[CA])
MDX
```

which would correspond to the following SQL query:

    SELECT SUM(unit_sales) unit_sales_sum, SUM(store_sales) store_sales_sum
    FROM sales
      LEFT JOIN products ON sales.product_id = products.id
      LEFT JOIN product_classes ON products.product_class_id = product_classes.id
      LEFT JOIN time ON sales.time_id = time.id
      LEFT JOIN customers ON sales.customer_id = customers.id
    WHERE time.the_year = 2010 AND time.quarter = 'Q1'
      AND customers.country = 'USA' AND customers.state_province = 'CA'
    GROUP BY product_classes.product_family
    ORDER BY product_classes.product_family

and then get axis and cells of result object:

```ruby
result.axes_count         # => 2
result.column_names       # => ["Unit Sales", "Store Sales"]
result.column_full_names  # => ["[Measures].[Unit Sales]", "[Measures].[Store Sales]"]
result.row_names          # => e.g. ["Drink", "Food", "Non-Consumable"]
result.row_full_names     # => e.g. ["[Products].[Drink]", "[Products].[Food]", "[Products].[Non-Consumable]"]
result.values             # => [[..., ...], [..., ...], [..., ...]]
                          # (three rows, each row containing value for "unit sales" and "store sales")
```

### Query builder methods

MDX queries could be built and executed also using Ruby methods in a similar way as ActiveRecord/Arel queries are made.
Previous MDX query can be executed as:

```ruby
olap.from('Sales').
columns('[Measures].[Unit Sales]', '[Measures].[Store Sales]').
rows('[Products].children').
where('[Time].[2010].[Q1]', '[Customers].[USA].[CA]').
execute
```

Here is example of more complex query "Get sales amount and profit % of top 50 products cross-joined with USA and Canada country sales during Q1 of 2010":

```ruby
olap.from('Sales').
with_member('[Measures].[ProfitPct]').
  as('Val((Measures.[Store Sales] - Measures.[Store Cost]) / Measures.[Store Sales])',
  :format_string => 'Percent').
columns('[Measures].[Store Sales]', '[Measures].[ProfitPct]').
rows('[Products].children').crossjoin('[Customers].[Canada]', '[Customers].[USA]').
  top_count(50, '[Measures].[Store Sales]').
where('[Time].[2010].[Q1]').
execute
```

See more examples of queries in `spec/query_spec.rb`.

Currently there are query builder methods just for most frequently used MDX functions, there will be new query builder methods in next releases of mondrian-olap gem.

### Cube dimension and member queries

mondrian-olap provides also methods for querying dimensions and members:

```ruby
cube = olap.cube('Sales')
cube.dimension_names                    # => ['Measures', 'Customers', 'Products', 'Time']
cube.dimensions                         # => array of dimension objects
cube.dimension('Customers')             # => customers dimension object
cube.dimension('Time').hierarchy_names  # => ['Time', 'Time.Weekly']
cube.dimension('Time').hierarchies      # => array of hierarchy objects
cube.dimension('Customers').hierarchy   # => default customers dimension hierarchy
cube.dimension('Customers').hierarchy.level_names
                                        # => ['(All)', 'Country', 'State Province', 'City', 'Name']
cube.dimension('Customers').hierarchy.levels
                                        # => array of hierarchy level objects
cube.dimension('Customers').hierarchy.level('Country').members
                                        # => array of all level members
cube.member('[Customers].[USA].[CA]')   # => lookup member by full name
cube.member('[Customers].[USA].[CA]').children
                                        # => get all children of member in deeper hierarchy level
cube.member('[Customers].[USA]').descendants_at_level('City')
                                        # => get all descendants of member in specified hierarchy level
```

See more examples of dimension and member queries in `spec/cube_spec.rb`.

### User defined MDX functions

You can define new MDX functions using JavaScript, CoffeeScript or Ruby language that you can later use
either in calculated member formulas or in MDX queries. Here are examples of user defined functions in Ruby:

```ruby
schema = Mondrian::OLAP::Schema.define do
  # ... cube definitions ...
  user_defined_function 'Factorial' do
    ruby do
      parameters :numeric
      returns :numeric
      def call(n)
        n <= 1 ? 1 : n * call(n - 1)
      end
    end
  end
  user_defined_function 'UpperName' do
    ruby do
      parameters :member
      returns :string
      syntax :property
      def call(member)
        member.getName.upcase
      end
    end
  end
end
```

See more examples of user defined functions in `spec/schema_definition_spec.rb`.

### Data access roles

In schema you can define data access roles which can be selected for connection and which will limit access just to
subset of measures and dimension members. Here is example of data access role definition:

```ruby
schema = Mondrian::OLAP::Schema.define do
  # ... cube definitions ...
  role 'California manager' do
    schema_grant :access => 'none' do
      cube_grant :cube => 'Sales', :access => 'all' do
        dimension_grant :dimension => '[Measures]', :access => 'all'
        hierarchy_grant :hierarchy => '[Customers]', :access => 'custom',
                        :top_level => '[Customers].[State Province]', :bottom_level => '[Customers].[City]' do
          member_grant :member => '[Customers].[USA].[CA]', :access => 'all'
          member_grant :member => '[Customers].[USA].[CA].[Los Angeles]', :access => 'none'
        end
      end
    end
  end
end
```

See more examples of data access roles in `spec/connection_role_spec.rb`.

REQUIREMENTS
------------

mondrian-olap gem is compatible with JRuby versions 1.6 and 1.7 and Java 6 and 7 VM. mondrian-olap works only with JRuby and not with other Ruby implementations as it includes Mondrian OLAP Java libraries.

mondrian-olap currently supports MySQL, PostgreSQL, Oracle, LucidDB and Microsoft SQL Server databases. When using MySQL, PostgreSQL or LucidDB databases then install jdbc-mysql, jdbc-postgres or jdbc-luciddb gem and require "jdbc/mysql", "jdbc/postgres" or "jdbc/luciddb" to load the corresponding JDBC database driver. When using Oracle then include Oracle JDBC driver (`ojdbc6.jar` for Java 6) in `CLASSPATH` or copy to `JRUBY_HOME/lib` or require it in application manually. When using SQL Server you can choose between the jTDS or Microsoft JDBC drivers. If you use jTDS require "jdbc/jtds". If you use the Microsoft JDBC driver include `sqljdbc.jar` or `sqljdbc4.jar` in `CLASSPATH` or copy to `JRUBY_HOME/lib` or require it in application manually.

INSTALL
-------

Install gem with:

    gem install mondrian-olap

or include in your project's Gemfile:

    gem "mondrian-olap"

LINKS
-----

* Source code: http://github.com/rsim/mondrian-olap
* Bug reports / Feature requests: http://github.com/rsim/mondrian-olap/issues
* General discussions and questions at: http://groups.google.com/group/mondrian-olap
* mondrian-olap demo Rails application: https://github.com/rsim/mondrian_demo

LICENSE
-------

mondrian-olap is released under the terms of MIT license; see LICENSE.txt.

Mondrian OLAP Engine is released under the terms of the Eclipse Public
License v1.0 (EPL); see LICENSE-Mondrian.html.
