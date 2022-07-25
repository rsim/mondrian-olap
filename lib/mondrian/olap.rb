require 'java'
require 'nokogiri'

{
  # Do not register MondrianOlap4jDriver
  "mondrian.olap4j.registerDriver" => false,
  # Do not register log3j2 MBean
  "log4j2.disable.jmx" => true
}.each do |key, value|
  if java.lang.System.getProperty(key).nil?
    java.lang.System.setProperty(key, value.to_s)
  end
end

directory = File.expand_path("../jars", __FILE__)
Dir["#{directory}/*.jar"].each do |file|
  require file
end

%w(error connection query result schema schema_udf cube).each do |file|
  require "mondrian/olap/#{file}"
end
