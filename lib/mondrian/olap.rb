require 'java'
require 'nokogiri'

directory = File.expand_path("../jars", __FILE__)
Dir["#{directory}/*.jar"].each do |file|
  require file
end

# Register Mondrian olap4j driver
Java::mondrian.olap4j.MondrianOlap4jDriver

%w(error connection query result schema schema_udf cube).each do |file|
  require "mondrian/olap/#{file}"
end
