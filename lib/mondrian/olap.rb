require 'java'

directory = File.expand_path("../jars", __FILE__)
Dir["#{directory}/*.jar"].each do |file|
  require file
end

java.lang.System.setProperty("log4j.configuration", "file://#{directory}/log4j.properties")

%w(connection query result schema cube).each do |file|
  require "mondrian/olap/#{file}"
end
