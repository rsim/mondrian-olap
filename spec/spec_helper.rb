require "rubygems"
require "bundler"
Bundler.setup(:default, :development)

$:.unshift(File.dirname(__FILE__) + '/../lib')

require 'rspec'
require 'jdbc/mysql'
require 'mondrian/olap'

require 'active_record'

require 'support/matchers/be_like'

RSpec.configure do |config|
  config.include Matchers
end

CONNECTION_PARAMS = {
  :driver => 'mysql',
  :host => 'localhost',
  :database => 'mondrian_test',
  :username => 'mondrian_test',
  :password => 'mondrian_test'
}
CATALOG_FILE = File.expand_path('../fixtures/MondrianTest.xml', __FILE__)
CONNECTION_PARAMS_WITH_CATALOG = CONNECTION_PARAMS.merge(
  :catalog => CATALOG_FILE
)

AR_CONNECTION_PARAMS = {
  :adapter => 'jdbc',
  :driver => 'com.mysql.jdbc.Driver',
  :url => "jdbc:mysql://#{CONNECTION_PARAMS[:host]}/#{CONNECTION_PARAMS[:database]}",
  :username => CONNECTION_PARAMS[:username],
  :password => CONNECTION_PARAMS[:password]
}

ActiveRecord::Base.establish_connection(AR_CONNECTION_PARAMS)
