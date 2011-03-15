require "rubygems"
require "bundler"
Bundler.setup(:default, :development)

$:.unshift(File.dirname(__FILE__) + '/../lib')

require 'rspec'
require 'active_record'

DATABASE_HOST = ENV['DATABASE_HOST'] || 'localhost'
DATABASE_USER = ENV['DATABASE_USER'] || 'mondrian_test'
DATABASE_PASSWORD = ENV['DATABASE_PASSWORD'] || 'mondrian_test'

case MONDRIAN_DRIVER = ENV['MONDRIAN_DRIVER'] || 'mysql'
when 'mysql'
  require 'jdbc/mysql'
  JDBC_DRIVER = 'com.mysql.jdbc.Driver'
  DATABASE_NAME = ENV['DATABASE_NAME'] || 'mondrian_test'
when 'postgresql'
  require 'jdbc/postgres'
  JDBC_DRIVER = 'org.postgresql.Driver'
  DATABASE_NAME = ENV['DATABASE_NAME'] || 'mondrian_test'
when 'oracle'
  require 'active_record/connection_adapters/oracle_enhanced_adapter'
  DATABASE_NAME = ENV['DATABASE_NAME'] || 'orcl'
end

puts "==> Using #{MONDRIAN_DRIVER} driver"

require 'mondrian/olap'

require 'support/matchers/be_like'

RSpec.configure do |config|
  config.include Matchers
end

CONNECTION_PARAMS = {
  :driver => MONDRIAN_DRIVER,
  :host => DATABASE_HOST,
  :database => DATABASE_NAME,
  :username => DATABASE_USER,
  :password => DATABASE_PASSWORD
}

if MONDRIAN_DRIVER == 'oracle'
  CATALOG_FILE = File.expand_path('../fixtures/MondrianTestOracle.xml', __FILE__)
  AR_CONNECTION_PARAMS = {
    :adapter => 'oracle_enhanced',
    :host => CONNECTION_PARAMS[:host],
    :database => CONNECTION_PARAMS[:database],
    :username => CONNECTION_PARAMS[:username],
    :password => CONNECTION_PARAMS[:password]
  }
else
  CATALOG_FILE = File.expand_path('../fixtures/MondrianTest.xml', __FILE__)
  AR_CONNECTION_PARAMS = {
    :adapter => 'jdbc',
    :driver => JDBC_DRIVER,
    :url => "jdbc:#{MONDRIAN_DRIVER}://#{CONNECTION_PARAMS[:host]}/#{CONNECTION_PARAMS[:database]}",
    :username => CONNECTION_PARAMS[:username],
    :password => CONNECTION_PARAMS[:password]
  }
end
CONNECTION_PARAMS_WITH_CATALOG = CONNECTION_PARAMS.merge(:catalog => CATALOG_FILE)

ActiveRecord::Base.establish_connection(AR_CONNECTION_PARAMS)
