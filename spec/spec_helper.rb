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
DATABASE_USER     = ENV["#{env_prefix}_DATABASE_USER"]     || ENV['DATABASE_USER']     || 'mondrian_test'
DATABASE_PASSWORD = ENV["#{env_prefix}_DATABASE_PASSWORD"] || ENV['DATABASE_PASSWORD'] || 'mondrian_test'
DATABASE_NAME     = ENV["#{env_prefix}_DATABASE_NAME"]     || ENV['DATABASE_NAME']     || 'mondrian_test'
DATABASE_INSTANCE = ENV["#{env_prefix}_DATABASE_INSTANCE"] || ENV['DATABASE_INSTANCE']

case MONDRIAN_DRIVER
when 'mysql', 'jdbc_mysql'
  require 'jdbc/mysql'
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
    :database => DATABASE_NAME,
    :username => DATABASE_USER,
    :password => DATABASE_PASSWORD
  }
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
    :url      => "jdbc:#{MONDRIAN_DRIVER}://#{CONNECTION_PARAMS[:host]}/#{CONNECTION_PARAMS[:database]}",
    :username => CONNECTION_PARAMS[:username],
    :password => CONNECTION_PARAMS[:password]
  }
end

CONNECTION_PARAMS_WITH_CATALOG = CONNECTION_PARAMS.merge(:catalog => CATALOG_FILE)

ActiveRecord::Base.establish_connection(AR_CONNECTION_PARAMS)
