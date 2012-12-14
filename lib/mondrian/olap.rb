require 'java'

directory = File.expand_path("../jars", __FILE__)
Dir["#{directory}/*.jar"].each do |file|
  require file
end

unless java.lang.System.getProperty("log4j.configuration")
  file_uri = java.io.File.new("#{directory}/log4j.properties").toURI.to_s
  java.lang.System.setProperty("log4j.configuration", file_uri)
end
# register Mondrian olap4j driver
Java::mondrian.olap4j.MondrianOlap4jDriver

%w(error connection query result schema schema_udf cube).each do |file|
  require "mondrian/olap/#{file}"
end
