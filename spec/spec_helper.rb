require 'rdoc'
require 'rspec'
require 'active_record'
# Patched adapter_java.jar with MySQL 8 JDBC driver support
require_relative 'support/jars/adapter_java.jar'
require 'activerecord-jdbc-adapter'
require 'coffee-script'
require 'rhino'
require 'pry'

# autoload corresponding JDBC driver during require 'jdbc/...'
Java::JavaLang::System.setProperty("jdbc.driver.autoload", "true")

MONDRIAN_DRIVER   = ENV['MONDRIAN_DRIVER']   || 'mysql'
env_prefix = MONDRIAN_DRIVER.upcase

DATABASE_HOST     = ENV["#{env_prefix}_DATABASE_HOST"]     || ENV['DATABASE_HOST']     || 'localhost'
DATABASE_PORT     = ENV["#{env_prefix}_DATABASE_PORT"]     || ENV['DATABASE_PORT']
DATABASE_USER     = ENV["#{env_prefix}_DATABASE_USER"]     || ENV['DATABASE_USER']     || 'mondrian_test'
DATABASE_PASSWORD = ENV["#{env_prefix}_DATABASE_PASSWORD"] || ENV['DATABASE_PASSWORD'] || 'mondrian_test'
DATABASE_NAME     = ENV["#{env_prefix}_DATABASE_NAME"]     || ENV['DATABASE_NAME']     || 'mondrian_test'
DATABASE_INSTANCE = ENV["#{env_prefix}_DATABASE_INSTANCE"] || ENV['DATABASE_INSTANCE']

case MONDRIAN_DRIVER
when 'mysql', 'jdbc_mysql'
  if jdbc_driver_file = Dir[File.expand_path("mysql*.jar", 'spec/support/jars')].first
    require jdbc_driver_file
  else
    require 'jdbc/mysql'
  end
  JDBC_DRIVER = (Java::com.mysql.cj.jdbc.Driver rescue nil) ? 'com.mysql.cj.jdbc.Driver' : 'com.mysql.jdbc.Driver'
when 'postgresql'
  require 'jdbc/postgres'
  JDBC_DRIVER = 'org.postgresql.Driver'
when 'oracle'
  require 'active_record/connection_adapters/oracle_enhanced_adapter'
  CATALOG_FILE = File.expand_path('../fixtures/MondrianTestOracle.xml', __FILE__)
when 'mssql'
  require 'jdbc/jtds'
  JDBC_DRIVER = 'net.sourceforge.jtds.jdbc.Driver'
when 'sqlserver'
  Dir[File.expand_path("{mssql-jdbc,sqljdbc}*.jar", 'spec/support/jars')].each do |jdbc_driver_file|
    require jdbc_driver_file
  end
  JDBC_DRIVER = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
when 'vertica'
  Dir[File.expand_path("vertica*.jar", 'spec/support/jars')].each do |jdbc_driver_file|
    require jdbc_driver_file
  end
  JDBC_DRIVER = 'com.vertica.jdbc.Driver'
  DATABASE_SCHEMA = ENV["#{env_prefix}_DATABASE_SCHEMA"] || ENV['DATABASE_SCHEMA'] || 'mondrian_test'
  # patches for Vertica minimal AR support
  require 'arjdbc/jdbc/adapter'
  ActiveRecord::ConnectionAdapters::JdbcAdapter.class_eval do
    def modify_types(tp)
      # mapping of ActiveRecord data types to Vertica data types
      tp[:primary_key] = "int" # Use int instead of identity as data cannot be loaded into identity columns
      tp[:integer] = "int"
    end
    def type_to_sql(type, limit = nil, precision = nil, scale = nil)
      case type.to_sym
      when :integer, :primary_key
        'int' # All integers are 64-bit in Vertica and limit should be ignored
      else
        super
      end
    end
    # by default Vertica stores table and column names in uppercase
    def quote_table_name(name)
      "\"#{name.to_s}\""
    end
    def quote_column_name(name)
      "\"#{name.to_s}\""
    end
    # exec_insert tries to use Statement.RETURN_GENERATED_KEYS which is not supported by Vertica
    def exec_insert(sql, name, binds, pk = nil, sequence_name = nil)
      exec_update(sql, name, binds)
    end
  end
when 'snowflake'
  Dir[File.expand_path("snowflake*.jar", 'spec/support/jars')].each do |jdbc_driver_file|
    require jdbc_driver_file
  end
  JDBC_DRIVER = 'net.snowflake.client.jdbc.SnowflakeDriver'
  DATABASE_SCHEMA = ENV["#{env_prefix}_DATABASE_SCHEMA"] || ENV['DATABASE_SCHEMA'] || 'mondrian_test'
  WAREHOUSE_NAME = ENV["#{env_prefix}_WAREHOUSE_NAME"] || ENV['WAREHOUSE_NAME'] || 'mondrian_test'
  CATALOG_FILE = File.expand_path('../fixtures/MondrianTestOracle.xml', __FILE__)
  require 'arjdbc/jdbc/adapter'
  ActiveRecord::ConnectionAdapters::JdbcAdapter.class_eval do
    def modify_types(tp)
      # mapping of ActiveRecord data types to Snowflake data types
      tp[:primary_key] = "integer"
      tp[:integer] = "integer"
    end
    # exec_insert tries to use Statement.RETURN_GENERATED_KEYS which is not supported by Snowflake
    def exec_insert(sql, name, binds, pk = nil, sequence_name = nil)
      exec_update(sql, name, binds)
    end
  end
  require 'arjdbc/jdbc/type_converter'
  # Hack to disable :text and :binary types for Snowflake
  ActiveRecord::ConnectionAdapters::JdbcTypeConverter::AR_TO_JDBC_TYPES.delete(:text)
  ActiveRecord::ConnectionAdapters::JdbcTypeConverter::AR_TO_JDBC_TYPES.delete(:binary)
when 'clickhouse'
  Dir[File.expand_path("clickhouse*.jar", 'spec/support/jars')].each do |jdbc_driver_file|
    require jdbc_driver_file
  end
  JDBC_DRIVER = 'cc.blynk.clickhouse.ClickHouseDriver'
  DATABASE_SCHEMA = ENV["#{env_prefix}_DATABASE_SCHEMA"] || ENV['DATABASE_SCHEMA'] || 'mondrian_test'
  # patches for ClickHouse minimal AR support
  require 'arjdbc/jdbc/adapter'
  ActiveRecord::ConnectionAdapters::JdbcAdapter.class_eval do
    NATIVE_DATABASE_TYPES = {
      primary_key: "Int32", # We do not need automatic primary key generation and need to allow inserting PK values
      string: {name: "String"},
      text: {name: "String"},
      integer: {name: "Int32"},
      float: {name: "Float64"},
      numeric: {name: "Decimal"},
      decimal: {name: "Decimal"},
      datetime: {name: "DateTime"},
      timestamp: {name: "DateTime"},
      time: {name: "DateTime"},
      date: {name: "Date"},
      binary: {name: "String"},
      boolean: {name: "Boolean"},
    }
    def native_database_types
      NATIVE_DATABASE_TYPES
    end
    def modify_types(tp)
      # mapping of ActiveRecord data types to ClickHouse data types
      tp[:primary_key] = 'Int32'
      tp[:integer] = 'Int32'
    end
    def type_to_sql(type, limit = nil, precision = nil, scale = nil)
      case type.to_sym
      when :integer, :primary_key
        return 'Int32' unless limit
        case limit.to_i
        when 1 then 'Int8'
        when 2 then 'Int16'
        when 3, 4 then 'Int32'
        when 5..8 then 'Int64'
        else raise(ActiveRecord::ActiveRecordError,
          "No integer type has byte size #{limit}. Use a numeric with precision 0 instead.")
        end
      # Ignore limit for string and text
      when :string, :text
        super(type, nil, nil, nil)
      else
        super
      end
    end
    def quote_table_name(name)
      "`#{name.to_s}`"
    end
    def quote_column_name(name)
      "`#{name.to_s}`"
    end
    def create_table(name, options = {})
      super(name, {options: "ENGINE=MergeTree ORDER BY tuple()"}.merge(options))
    end
    alias_method :exec_update_original, :exec_update
    # exec_insert tries to use Statement.RETURN_GENERATED_KEYS which is not supported by ClickHouse
    def exec_insert(sql, name, binds, pk = nil, sequence_name = nil)
      exec_update_original(sql, name, binds)
    end
    # Modify UPDATE statements for ClickHouse specific syntax
    def exec_update(sql, name, binds)
      if sql =~ /\AUPDATE (.*) SET (.*)\z/
        sql = "ALTER TABLE #{$1} UPDATE #{$2}"
      end
      exec_update_original(sql, name, binds)
    end
  end
when 'mariadb'
  Dir[File.expand_path("mariadb*.jar", 'spec/support/jars')].each do |jdbc_driver_file|
    require jdbc_driver_file
  end
  JDBC_DRIVER = 'org.mariadb.jdbc.Driver'
  # Patches for MariaDB minimal AR support
  require 'arjdbc/jdbc/adapter'
  ActiveRecord::ConnectionAdapters::JdbcAdapter.class_eval do
    def modify_types(tp)
      tp[:primary_key] = "integer"
      tp[:integer] = "integer"
    end
    def type_to_sql(type, limit = nil, precision = nil, scale = nil)
      case type.to_sym
      when :integer, :primary_key
        return 'integer' unless limit
        case limit.to_i
        when 1 then 'tinyint'
        when 2 then 'smallint'
        when 3 then 'mediumint'
        when 4 then 'integer'
        when 5..8 then 'bigint'
        else raise(ActiveRecord::ActiveRecordError,
          "No integer type has byte size #{limit}. Use a numeric with precision 0 instead.")
        end
      when :text
        case limit
        when 0..0xff then 'tinytext'
        when nil, 0x100..0xffff then 'text'
        when 0x10000..0xffffff then'mediumtext'
        when 0x1000000..0xffffffff then 'longtext'
        else raise(ActiveRecordError, "No text type has character length #{limit}")
        end
      else
        super
      end
    end
    def quote_table_name(name)
      "`#{name.to_s}`"
    end
    def quote_column_name(name)
      "`#{name.to_s}`"
    end
    def execute(sql, name = nil, binds = nil)
      exec_update(sql, name, binds)
    end
    def create_table(name, options = {})
      super(name, {options: "ENGINE=Columnstore DEFAULT CHARSET=utf8"}.merge(options))
    end
  end
when 'singlestore'
  # SingleStore recommends MariaDB JDBC driver
  Dir[File.expand_path("mariadb*.jar", 'spec/support/jars')].each do |jdbc_driver_file|
    require jdbc_driver_file
  end
  JDBC_DRIVER = 'org.mariadb.jdbc.Driver'
  # Patches for SingleStore minimal AR support
  require 'arjdbc/jdbc/adapter'
  ActiveRecord::ConnectionAdapters::JdbcAdapter.class_eval do
    def modify_types(tp)
      tp[:primary_key] = "integer"
      tp[:integer] = "integer"
    end
    def type_to_sql(type, limit = nil, precision = nil, scale = nil)
      case type.to_sym
      when :integer, :primary_key
        return 'integer' unless limit
        case limit.to_i
        when 1 then 'tinyint'
        when 2 then 'smallint'
        when 3 then 'mediumint'
        when 4 then 'integer'
        when 5..8 then 'bigint'
        else raise(ActiveRecord::ActiveRecordError,
          "No integer type has byte size #{limit}. Use a numeric with precision 0 instead.")
        end
      when :text
        case limit
        when 0..0xff then 'tinytext'
        when nil, 0x100..0xffff then 'text'
        when 0x10000..0xffffff then'mediumtext'
        when 0x1000000..0xffffffff then 'longtext'
        else raise(ActiveRecordError, "No text type has character length #{limit}")
        end
      else
        super
      end
    end
    def quote_table_name(name)
      "`#{name.to_s}`"
    end
    def quote_column_name(name)
      "`#{name.to_s}`"
    end
    def execute(sql, name = nil, binds = nil)
      exec_update(sql, name, binds)
    end

    class SingleStoreSchemaCreation < ::ActiveRecord::ConnectionAdapters::AbstractAdapter::SchemaCreation
      def visit_TableDefinition(o)
        name = o.name
        create_sql = "CREATE#{' TEMPORARY' if o.temporary} TABLE #{quote_table_name(name)} "
        statements = o.columns.map { |c| accept c }
        statements << "KEY () USING CLUSTERED COLUMNSTORE"
        create_sql << "(#{statements.join(', ')}) " if statements.present?
        create_sql << "#{o.options}"
        create_sql << " AS #{@conn.to_sql(o.as)}" if o.as
        create_sql
      end
    end
    def schema_creation
      SingleStoreSchemaCreation.new self
    end
  end
end

puts "==> Using #{MONDRIAN_DRIVER} driver"

require 'mondrian/olap'
require_relative 'support/matchers/be_like'

RSpec.configure do |config|
  config.include Matchers
end

CATALOG_FILE = File.expand_path('../fixtures/MondrianTest.xml', __FILE__) unless defined?(CATALOG_FILE)

CONNECTION_PARAMS = if MONDRIAN_DRIVER =~ /^jdbc/
  {
    :driver   => 'jdbc',
    :jdbc_url => "jdbc:#{MONDRIAN_DRIVER.split('_').last}://#{DATABASE_HOST}/#{DATABASE_NAME}",
    :jdbc_driver => JDBC_DRIVER,
    :username => DATABASE_USER,
    :password => DATABASE_PASSWORD
  }
else
  {
    # uncomment to test PostgreSQL SSL connection
    # :properties => {'ssl'=>'true','sslfactory'=>'org.postgresql.ssl.NonValidatingFactory'},
    :driver   => MONDRIAN_DRIVER,
    :host     => DATABASE_HOST,
    :port     => DATABASE_PORT,
    :database => DATABASE_NAME,
    :username => DATABASE_USER,
    :password => DATABASE_PASSWORD
  }.compact
end
case MONDRIAN_DRIVER
when 'mysql'
  CONNECTION_PARAMS[:properties] = {useSSL: false, serverTimezone: 'UTC'}
when 'jdbc_mysql'
  CONNECTION_PARAMS[:jdbc_url] << '?useUnicode=true&characterEncoding=UTF-8&useSSL=false&serverTimezone=UTC'
end

case MONDRIAN_DRIVER
when 'mysql', 'postgresql'
  AR_CONNECTION_PARAMS = CONNECTION_PARAMS.slice(:host, :database, :username, :password).merge(
    :adapter => MONDRIAN_DRIVER,
    :properties => CONNECTION_PARAMS[:properties].dup || {}
  )
when 'oracle'
  AR_CONNECTION_PARAMS = {
    :adapter  => 'oracle_enhanced',
    :host     => CONNECTION_PARAMS[:host],
    :database => CONNECTION_PARAMS[:database],
    :username => CONNECTION_PARAMS[:username],
    :password => CONNECTION_PARAMS[:password]
  }
when 'mssql'
  url = "jdbc:jtds:sqlserver://#{CONNECTION_PARAMS[:host]}/#{CONNECTION_PARAMS[:database]}"
  url << ";instance=#{DATABASE_INSTANCE}" if DATABASE_INSTANCE
  AR_CONNECTION_PARAMS = {
    :adapter  => 'jdbc',
    :dialect  => 'Microsoft SQL Server',
    :driver   => JDBC_DRIVER,
    :url      => url,
    :username => CONNECTION_PARAMS[:username],
    :password => CONNECTION_PARAMS[:password],
    :connection_alive_sql => 'SELECT 1'
  }
when 'sqlserver'
  url = "jdbc:sqlserver://#{CONNECTION_PARAMS[:host]};databaseName=#{CONNECTION_PARAMS[:database]};"
  url << ";instanceName=#{DATABASE_INSTANCE}" if DATABASE_INSTANCE
  AR_CONNECTION_PARAMS = {
    :adapter  => 'jdbc',
    :driver   => JDBC_DRIVER,
    :url      => url,
    :username => CONNECTION_PARAMS[:username],
    :password => CONNECTION_PARAMS[:password],
    :connection_alive_sql => 'SELECT 1'
  }
when 'vertica'
  CONNECTION_PARAMS[:properties] = {
    'SearchPath' => DATABASE_SCHEMA
  }
  AR_CONNECTION_PARAMS = {
    adapter: 'jdbc',
    driver:   JDBC_DRIVER,
    url:      "jdbc:#{MONDRIAN_DRIVER}://#{CONNECTION_PARAMS[:host]}/#{CONNECTION_PARAMS[:database]}" \
      "?SearchPath=#{DATABASE_SCHEMA}", # &LogLevel=DEBUG
    username: CONNECTION_PARAMS[:username],
    password: CONNECTION_PARAMS[:password]
  }
when 'snowflake'
  CONNECTION_PARAMS[:database_schema] = DATABASE_SCHEMA
  CONNECTION_PARAMS[:warehouse] = WAREHOUSE_NAME
  CONNECTION_PARAMS[:properties] = {
    # 'tracing' => 'ALL'
  }
  AR_CONNECTION_PARAMS = {
    adapter: 'jdbc',
    driver:   JDBC_DRIVER,
    url:      "jdbc:#{MONDRIAN_DRIVER}://#{CONNECTION_PARAMS[:host]}/?db=#{CONNECTION_PARAMS[:database]}" \
      "&schema=#{DATABASE_SCHEMA}&warehouse=#{WAREHOUSE_NAME}", # &tracing=ALL
    username: CONNECTION_PARAMS[:username],
    password: CONNECTION_PARAMS[:password]
  }
when 'clickhouse'
  # CREATE USER mondrian_test IDENTIFIED WITH plaintext_password BY 'mondrian_test';
  # CREATE DATABASE mondrian_test;
  # GRANT ALL ON mondrian_test.* TO mondrian_test;
  AR_CONNECTION_PARAMS = {
    adapter: 'jdbc',
    driver:   JDBC_DRIVER,
    url:      "jdbc:#{MONDRIAN_DRIVER}://#{CONNECTION_PARAMS[:host]}:8123/#{CONNECTION_PARAMS[:database]}",
    username: CONNECTION_PARAMS[:username],
    password: CONNECTION_PARAMS[:password]
  }
when 'singlestore'
  jdbc_url = "jdbc:mariadb://#{CONNECTION_PARAMS[:host]}" + (CONNECTION_PARAMS[:port] ? ":#{CONNECTION_PARAMS[:port]}" : "") +
      "/#{CONNECTION_PARAMS[:database]}"
  AR_CONNECTION_PARAMS = {
    adapter: 'jdbc',
    driver:   JDBC_DRIVER,
    dialect: 'SingleStore',
    url:      jdbc_url,
    username: CONNECTION_PARAMS[:username],
    password: CONNECTION_PARAMS[:password]
  }
when /jdbc/
  AR_CONNECTION_PARAMS = {
    :adapter  => 'jdbc',
    :driver   => JDBC_DRIVER,
    :url      => CONNECTION_PARAMS[:jdbc_url],
    :username => CONNECTION_PARAMS[:username],
    :password => CONNECTION_PARAMS[:password]
  }
else
  AR_CONNECTION_PARAMS = {
    :adapter  => 'jdbc',
    :driver   => JDBC_DRIVER,
    :url      => "jdbc:#{MONDRIAN_DRIVER}://#{CONNECTION_PARAMS[:host]}" +
      (CONNECTION_PARAMS[:port] ? ":#{CONNECTION_PARAMS[:port]}" : "") + "/#{CONNECTION_PARAMS[:database]}",
    :username => CONNECTION_PARAMS[:username],
    :password => CONNECTION_PARAMS[:password]
  }
end

CONNECTION_PARAMS_WITH_CATALOG = CONNECTION_PARAMS.merge(:catalog => CATALOG_FILE)

ActiveRecord::Base.establish_connection(AR_CONNECTION_PARAMS)
