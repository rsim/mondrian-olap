require "rubygems"
require "bundler"
Bundler.setup(:default, :test)

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
  :database => 'foodmart',
  :host => 'localhost',
  :username => 'foodmart',
  :password => 'foodmart'
}
CATALOG_FILE = File.expand_path('../fixtures/FoodMart.xml', __FILE__)
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
