require 'rdoc'
require 'rspec'
require 'logger'
require 'active_record'
require 'activerecord-jdbc-adapter'
require 'pry'

# Autoload corresponding JDBC driver during require 'jdbc/...'
Java::JavaLang::System.setProperty("jdbc.driver.autoload", "true")

MONDRIAN_DRIVER   = ENV['MONDRIAN_DRIVER']   || 'mysql'
env_prefix = MONDRIAN_DRIVER.upcase

DATABASE_HOST     = ENV["#{env_prefix}_DATABASE_HOST"]     || ENV['DATABASE_HOST']     || 'localhost'
DATABASE_PORT     = ENV["#{env_prefix}_DATABASE_PORT"]     || ENV['DATABASE_PORT']
DATABASE_PROTOCOL = ENV["#{env_prefix}_DATABASE_PROTOCOL"] || ENV['DATABASE_PROTOCOL']
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
  require 'arjdbc/postgresql'

when 'oracle'
  Dir[File.expand_path("ojdbc*.jar", 'spec/support/jars')].each do |jdbc_driver_file|
    require jdbc_driver_file
  end

  # PATCH: Fix NameError undefined field 'map' for class 'Java::OrgJruby::RubyObjectSpace::WeakMap'
  # pending release of https://github.com/rsim/oracle-enhanced/pull/2360/files
  begin
    require 'active_record/connection_adapters/oracle_enhanced_adapter'
  rescue NameError => e
    raise e unless e.message =~ /undefined field 'map'/
    $LOADED_FEATURES <<
      File.expand_path("active_record/connection_adapters/oracle_enhanced_adapter.rb", $:.grep(/oracle_enhanced/).first)
  end

  ActiveRecord::ConnectionAdapters::OracleEnhancedAdapter.class_eval do
    # Start primary key sequences from 1 (and not 10000) and take just one next value in each session
    self.default_sequence_start_value = "1 NOCACHE INCREMENT BY 1"
    # PATCH: Restore previous mapping of ActiveRecord datetime to DATE type.
    def supports_datetime_with_precision?; false; end
    # PATCH: Do not send fractional seconds to DATE type.
    def quoted_date(value)
      if value.acts_like?(:time)
        zone_conversion_method = ActiveRecord::Base.default_timezone == :utc ? :getutc : :getlocal
        if value.respond_to?(zone_conversion_method)
          value = value.send(zone_conversion_method)
        end
      end
      value.to_s(:db)
    end
    private
    # PATCH: Restore previous mapping of ActiveRecord datetime to DATE type.
    const_get(:NATIVE_DATABASE_TYPES)[:datetime] = {name: "DATE"}
    alias_method :original_initialize_type_map, :initialize_type_map
    def initialize_type_map(m = type_map)
      original_initialize_type_map(m)
      # PATCH: Map Oracle DATE to DateTime for backwards compatibility
      register_class_with_precision m, %r(date)i,  ActiveRecord::Type::DateTime
    end
  end
  CATALOG_FILE = File.expand_path('../fixtures/MondrianTestOracle.xml', __FILE__)

when 'sqlserver'
  Dir[File.expand_path("mssql-jdbc*.jar", 'spec/support/jars')].each do |jdbc_driver_file|
    require jdbc_driver_file
  end
  require 'arjdbc/jdbc/adapter'
  ActiveRecord::ConnectionAdapters::JdbcAdapter.class_eval do
    def initialize(connection, logger = nil, connection_parameters = nil, config = {})
      super(connection, logger, config.dup)
    end
    def modify_types(types)
      types.merge!(
        primary_key: 'bigint NOT NULL IDENTITY(1,1) PRIMARY KEY',
        integer: {name: 'int'},
        bigint: {name: 'bigint'},
        boolean: {name: 'bit'},
        decimal: {name: 'decimal'},
        date: {name: 'date'},
        datetime: {name: 'datetime'},
        timestamp: {name: 'datetime'},
        string: {name: 'nvarchar', limit: 4000},
        text: {name: 'nvarchar(max)'}
      )
    end
    def quote_table_name(name)
      name.to_s.split('.').map { |n| quote_column_name(n) }.join('.')
    end
    def quote_column_name(name)
      "[#{name.to_s}]"
    end
    def columns(table_name, name = nil)
      select_all(
        "SELECT * FROM information_schema.columns WHERE table_name = #{quote table_name}"
      ).map do |column|
        ActiveRecord::ConnectionAdapters::Column.new(
          column['COLUMN_NAME'],
          column['COLUMN_DEFAULT'],
          fetch_type_metadata(column['DATA_TYPE']),
          column['IS_NULLABLE']
        )
      end
    end
    def write_query?(sql)
      sql =~ /\A(INSERT|UPDATE|DELETE) /
    end
  end
  ::Arel::Visitors::ToSql.class_eval do
    private
    def visit_Arel_Nodes_Limit(o, collector)
      # Do not add LIMIT as it is not supported by MS SQL Server
      collector
    end
  end
  require "active_model/type/integer"
  ActiveModel::Type::Integer::DEFAULT_LIMIT = 8
  JDBC_DRIVER = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'

when 'vertica'
  Dir[File.expand_path("vertica*.jar", 'spec/support/jars')].each do |jdbc_driver_file|
    require jdbc_driver_file
  end
  JDBC_DRIVER = 'com.vertica.jdbc.Driver'
  DATABASE_SCHEMA = ENV["#{env_prefix}_DATABASE_SCHEMA"] || ENV['DATABASE_SCHEMA'] || 'mondrian_test'
  require 'arjdbc/jdbc/adapter'
  ActiveRecord::ConnectionAdapters::JdbcAdapter.class_eval do
    def initialize(connection, logger = nil, connection_parameters = nil, config = {})
      super(connection, logger, config.dup)
    end
    def modify_types(types)
      types[:primary_key] = "int" # Use int instead of identity as data cannot be loaded into identity columns
      types[:integer] = "int"
    end
    def type_to_sql(type, limit: nil, precision: nil, scale: nil, **)
      case type.to_sym
      when :integer, :primary_key
        'int' # All integers are 64-bit in Vertica and limit should be ignored
      else
        super
      end
    end
    # By default Vertica stores table and column names in uppercase
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
    def initialize(connection, logger = nil, connection_parameters = nil, config = {})
      super(connection, logger, config.dup)
    end
    def modify_types(types)
      types[:primary_key] = 'integer'
      types[:integer] = 'integer'
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
  JDBC_DRIVER = 'com.clickhouse.jdbc.ClickHouseDriver'
  DATABASE_SCHEMA = ENV["#{env_prefix}_DATABASE_SCHEMA"] || ENV['DATABASE_SCHEMA'] || 'mondrian_test'
  require 'arjdbc/jdbc/adapter'
  ActiveRecord::ConnectionAdapters::JdbcAdapter.class_eval do
    def initialize(connection, logger = nil, connection_parameters = nil, config = {})
      super(connection, logger, config.dup)
    end
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
    def modify_types(types)
      types[:primary_key] = 'Int32'
      types[:integer] = 'Int32'
    end
    def type_to_sql(type, limit: nil, precision: nil, scale: nil, **)
      case type.to_sym
      when :integer, :primary_key
        return 'Int32' unless limit
        case limit.to_i
        when 1 then 'Int8'
        when 2 then 'Int16'
        when 3, 4 then 'Int32'
        when 5..8 then 'Int64'
        else raise ActiveRecord::ActiveRecordError,
          "No integer type has byte size #{limit}. Use a numeric with precision 0 instead."
        end
      # Ignore limit for string and text
      when :string, :text
        super(type)
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
  require 'arjdbc/jdbc/adapter'
  ActiveRecord::ConnectionAdapters::JdbcAdapter.class_eval do
    def initialize(connection, logger = nil, connection_parameters = nil, config = {})
      super(connection, logger, config.dup)
    end
    def modify_types(types)
      types[:primary_key] = "integer"
      types[:integer] = "integer"
    end
    def type_to_sql(type, limit: nil, precision: nil, scale: nil, **)
      case type.to_sym
      when :integer, :primary_key
        return 'integer' unless limit
        case limit.to_i
        when 1 then 'tinyint'
        when 2 then 'smallint'
        when 3 then 'mediumint'
        when 4 then 'integer'
        when 5..8 then 'bigint'
        else raise ActiveRecord::ActiveRecordError,
          "No integer type has byte size #{limit}. Use a numeric with precision 0 instead."
        end
      when :text
        case limit
        when 0..0xff then 'tinytext'
        when nil, 0x100..0xffff then 'text'
        when 0x10000..0xffffff then'mediumtext'
        when 0x1000000..0xffffffff then 'longtext'
        else raise ActiveRecordError, "No text type has character length #{limit}"
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
end

puts "==> Using #{MONDRIAN_DRIVER} driver"

{
  # Necessary for Aggregate optimizations test
  "mondrian.rolap.EnableInMemoryRollup" => false,
  # Test Mondrian setOrderKey patches
  "mondrian.rolap.compareSiblingsByOrderKey" => true
}.each do |property, value|
  Java::JavaLang::System.setProperty(property, value.to_s)
end

require 'mondrian/olap'
require_relative 'support/matchers/be_like'

RSpec.configure do |config|
  config.include Matchers
  config.expect_with(:rspec) { |c| c.syntax = [:should, :expect] }
end

CATALOG_FILE = File.expand_path('../fixtures/MondrianTest.xml', __FILE__) unless defined?(CATALOG_FILE)

CONNECTION_PARAMS = if MONDRIAN_DRIVER =~ /^jdbc/
  {
    driver: 'jdbc',
    jdbc_url: "jdbc:#{MONDRIAN_DRIVER.split('_').last}://#{DATABASE_HOST}/#{DATABASE_NAME}",
    jdbc_driver: JDBC_DRIVER,
    username: DATABASE_USER,
    password: DATABASE_PASSWORD
  }
else
  {
    # Uncomment to test PostgreSQL SSL connection
    # properties: {'ssl'=>'true','sslfactory'=>'org.postgresql.ssl.NonValidatingFactory'},
    driver: MONDRIAN_DRIVER,
    host: DATABASE_HOST,
    port: DATABASE_PORT,
    protocol: DATABASE_PROTOCOL.presence,
    database: DATABASE_NAME,
    username: DATABASE_USER,
    password: DATABASE_PASSWORD
  }.compact
end
case MONDRIAN_DRIVER
when 'mysql'
  CONNECTION_PARAMS[:properties] = {useSSL: false, serverTimezone: 'UTC', allowPublicKeyRetrieval: true}
when 'jdbc_mysql'
  CONNECTION_PARAMS[:jdbc_url] +=
    '?useUnicode=true&characterEncoding=UTF-8&useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true'
end

case MONDRIAN_DRIVER
when 'mysql', 'postgresql'
  AR_CONNECTION_PARAMS = CONNECTION_PARAMS.slice(:host, :database, :username, :password).merge(
    adapter: MONDRIAN_DRIVER,
    driver: JDBC_DRIVER,
    properties: CONNECTION_PARAMS[:properties].dup || {}
  )
when 'oracle'
  AR_CONNECTION_PARAMS = {
    adapter: 'oracle_enhanced',
    host: CONNECTION_PARAMS[:host],
    database: CONNECTION_PARAMS[:database],
    username: CONNECTION_PARAMS[:username],
    password: CONNECTION_PARAMS[:password],
    nls_numeric_characters: '.,'
  }
when 'sqlserver'
  url = "jdbc:sqlserver://#{CONNECTION_PARAMS[:host]};databaseName=#{CONNECTION_PARAMS[:database]};"
  url += ";instanceName=#{DATABASE_INSTANCE}" if DATABASE_INSTANCE
  AR_CONNECTION_PARAMS = {
    adapter: 'jdbc',
    driver: JDBC_DRIVER,
    url: url,
    username: CONNECTION_PARAMS[:username],
    password: CONNECTION_PARAMS[:password],
    connection_alive_sql: 'SELECT 1',
    sqlserver_version: ENV['SQLSERVER_VERSION'],
    dialect: 'jdbc'
  }
when 'vertica'
  CONNECTION_PARAMS[:properties] = {
    'SearchPath' => DATABASE_SCHEMA
  }
  AR_CONNECTION_PARAMS = {
    adapter: 'jdbc',
    driver: JDBC_DRIVER,
    url: "jdbc:#{MONDRIAN_DRIVER}://#{CONNECTION_PARAMS[:host]}/#{CONNECTION_PARAMS[:database]}" \
      "?SearchPath=#{DATABASE_SCHEMA}", # &LogLevel=DEBUG
    username: CONNECTION_PARAMS[:username],
    password: CONNECTION_PARAMS[:password],
    dialect: 'jdbc'
  }
when 'snowflake'
  CONNECTION_PARAMS[:database_schema] = DATABASE_SCHEMA
  CONNECTION_PARAMS[:warehouse] = WAREHOUSE_NAME
  CONNECTION_PARAMS[:properties] = {
    # 'tracing' => 'ALL'
  }
  AR_CONNECTION_PARAMS = {
    adapter: 'jdbc',
    driver: JDBC_DRIVER,
    url: "jdbc:#{MONDRIAN_DRIVER}://#{CONNECTION_PARAMS[:host]}/?db=#{CONNECTION_PARAMS[:database]}" \
      "&schema=#{DATABASE_SCHEMA}&warehouse=#{WAREHOUSE_NAME}", # &tracing=ALL
    username: CONNECTION_PARAMS[:username],
    password: CONNECTION_PARAMS[:password],
    dialect: 'jdbc'
  }
when 'clickhouse'
  # CREATE USER mondrian_test IDENTIFIED WITH plaintext_password BY 'mondrian_test';
  # CREATE DATABASE mondrian_test;
  # GRANT ALL ON mondrian_test.* TO mondrian_test;

  # For testing different protocols
  # CONNECTION_PARAMS[:protocol] = 'http'
  # CONNECTION_PARAMS[:properties] ={'http_connection_provider' => 'APACHE_HTTP_CLIENT'}

  AR_CONNECTION_PARAMS = {
    adapter: 'jdbc',
    driver: JDBC_DRIVER,
    url: "jdbc:ch:#{CONNECTION_PARAMS[:protocol]&.+(':')}//#{CONNECTION_PARAMS[:host]}/#{CONNECTION_PARAMS[:database]}",
    username: CONNECTION_PARAMS[:username],
    password: CONNECTION_PARAMS[:password],
    dialect: 'jdbc'
  }
when /jdbc/
  AR_CONNECTION_PARAMS = {
    adapter: MONDRIAN_DRIVER =~ /mysql/ ? 'mysql' : 'jdbc',
    driver: JDBC_DRIVER,
    url: CONNECTION_PARAMS[:jdbc_url],
    username: CONNECTION_PARAMS[:username],
    password: CONNECTION_PARAMS[:password],
    dialect: MONDRIAN_DRIVER =~ /mysql/ ? 'mysql' : 'jdbc'
  }
else
  AR_CONNECTION_PARAMS = {
    adapter: 'jdbc',
    driver: JDBC_DRIVER,
    url: "jdbc:#{MONDRIAN_DRIVER}://#{CONNECTION_PARAMS[:host]}" +
      (CONNECTION_PARAMS[:port] ? ":#{CONNECTION_PARAMS[:port]}" : "") + "/#{CONNECTION_PARAMS[:database]}",
    username: CONNECTION_PARAMS[:username],
    password: CONNECTION_PARAMS[:password],
    dialect: 'jdbc'
  }
end

CONNECTION_PARAMS_WITH_CATALOG = CONNECTION_PARAMS.merge(catalog: CATALOG_FILE)

ActiveRecord::Base.establish_connection(AR_CONNECTION_PARAMS)
