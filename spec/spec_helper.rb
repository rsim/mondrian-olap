require "rubygems"
require "bundler"
Bundler.setup(:default, :development)

$:.unshift(File.dirname(__FILE__) + '/../lib')

require 'rspec'
require 'active_record'
require 'pry'

DATABASE_HOST     = ENV['DATABASE_HOST']     || 'localhost'
DATABASE_USER     = ENV['DATABASE_USER']     || 'mondrian_test'
DATABASE_PASSWORD = ENV['DATABASE_PASSWORD'] || 'mondrian_test'
DATABASE_NAME     = ENV['DATABASE_NAME']     || 'mondrian_test'
DATABASE_INSTANCE = ENV['DATABASE_INSTANCE']
MONDRIAN_DRIVER   = ENV['MONDRIAN_DRIVER']   || 'mysql'

case MONDRIAN_DRIVER
when 'mysql'
  require 'jdbc/mysql'
  JDBC_DRIVER = 'com.mysql.jdbc.Driver'
when 'postgresql'
  require 'jdbc/postgres'
  JDBC_DRIVER = 'org.postgresql.Driver'
when 'oracle'
  require 'active_record/connection_adapters/oracle_enhanced_adapter'
  DATABASE_NAME = ENV['DATABASE_NAME'] || 'orcl'
  CATALOG_FILE = File.expand_path('../fixtures/MondrianTestOracle.xml', __FILE__)
when 'mssql'
  require 'jdbc/jtds'
  JDBC_DRIVER = 'net.sourceforge.jtds.jdbc.Driver'
when 'sqlserver'
  JDBC_DRIVER = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
when 'luciddb'
  require 'jdbc/luciddb'
  CATALOG_FILE = File.expand_path('../fixtures/MondrianTestOracle.xml', __FILE__)

  # Hack to disable :text type for LucidDB
  require 'arjdbc/jdbc/type_converter'
  ActiveRecord::ConnectionAdapters::JdbcTypeConverter::AR_TO_JDBC_TYPES.delete(:text)

  # patches for LucidDB minimal AR support
  require 'arjdbc/jdbc/adapter'
  ActiveRecord::ConnectionAdapters::JdbcAdapter.class_eval do
    def modify_types(tp)
      # mapping of ActiveRecord data types to LucidDB data types
      # data will be imported into LucidDB therefore primary key is defined as simple integer field
      tp[:primary_key] = "INT"
      tp[:integer] = "INT"
    end
    # by default LucidDB stores table and column names in uppercase
    def quote_table_name(name)
      "\"#{name.to_s.upcase}\""
    end
    def quote_column_name(name)
      "\"#{name.to_s.upcase}\""
    end
  end
  JDBC_DRIVER = 'org.luciddb.jdbc.LucidDbClientDriver'
  DATABASE_USER.upcase! if DATABASE_USER == 'mondrian_test'
  DATABASE_NAME = nil
  DATABASE_SCHEMA = ENV['DATABASE_SCHEMA'] || 'mondrian_test'
end

puts "==> Using #{MONDRIAN_DRIVER} driver"

require 'mondrian/olap'
require 'support/matchers/be_like'

RSpec.configure do |config|
  config.include Matchers
end

CATALOG_FILE = File.expand_path('../fixtures/MondrianTest.xml', __FILE__) unless defined?(CATALOG_FILE)

CONNECTION_PARAMS = {
  :driver   => MONDRIAN_DRIVER,
  :host     => DATABASE_HOST,
  :database => DATABASE_NAME,
  :username => DATABASE_USER,
  :password => DATABASE_PASSWORD
}

case MONDRIAN_DRIVER
when 'oracle'
  AR_CONNECTION_PARAMS = {
    :adapter  => 'oracle_enhanced',
    :host     => CONNECTION_PARAMS[:host],
    :database => CONNECTION_PARAMS[:database],
    :username => CONNECTION_PARAMS[:username],
    :password => CONNECTION_PARAMS[:password]
  }
when 'luciddb'
  CONNECTION_PARAMS[:database] = nil
  CONNECTION_PARAMS[:database_schema] = DATABASE_SCHEMA
  AR_CONNECTION_PARAMS = {
    :adapter  => 'jdbc',
    :driver   => JDBC_DRIVER,
    :url      => "jdbc:#{MONDRIAN_DRIVER}:http://#{CONNECTION_PARAMS[:host]};schema=#{CONNECTION_PARAMS[:database_schema]}",
    :username => CONNECTION_PARAMS[:username],
    :password => CONNECTION_PARAMS[:password]
  }
when 'mssql'
  url = "jdbc:jtds:sqlserver://#{CONNECTION_PARAMS[:host]}/#{CONNECTION_PARAMS[:database]}"
  url << ";instance=#{DATABASE_INSTANCE}" if DATABASE_INSTANCE
  AR_CONNECTION_PARAMS = {
    :adapter  => 'jdbc',
    :driver   => JDBC_DRIVER,
    :url      => url,
    :username => CONNECTION_PARAMS[:username],
    :password => CONNECTION_PARAMS[:password]
  }
when 'sqlserver'
  url = "jdbc:sqlserver://#{CONNECTION_PARAMS[:host]};databaseName=#{CONNECTION_PARAMS[:database]};"
  url << ";instanceName=#{DATABASE_INSTANCE}" if DATABASE_INSTANCE
  AR_CONNECTION_PARAMS = {
    :adapter  => 'jdbc',
    :driver   => JDBC_DRIVER,
    :url      => url,
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
